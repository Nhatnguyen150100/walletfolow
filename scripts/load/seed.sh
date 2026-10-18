#!/usr/bin/env bash
# =====================================================================
# Chuẩn bị dữ liệu cho load test (roadmap 1.5b): N người gửi + 1 ví "merchant", mỗi người gửi được
# nạp tiền. Ghi thông tin đăng nhập ra k6/.data/users.json (gitignore) cho k6 đọc.
#
#   ./scripts/load/seed.sh                 # LOAD_USERS=40, LOAD_FUND=10000000 (minor)
#
# Vì sao nhiều người gửi: gateway giữ ngưỡng PRODUCTION 10 rps/user cho /api/v1/transactions — tải
# 80 TPS phải chia cho ≥ 8 user. 40 user => ~2 rps/user ở mức cao nhất, xa ngưỡng, không gặp 429.
# Vì sao có merchant: kịch bản "ví nóng" = nhiều người cùng chuyển VÀO một ví — tranh chấp khoá thật
# trên tài khoản nhận mà không cần nới rate limit (một người không thể gửi từ ví người khác).
#
# Mật khẩu sinh ngẫu nhiên mỗi lần, chỉ nằm trong file gitignore trên máy chạy, cho stack dev/E2E.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/e2e/lib.sh

USERS=${LOAD_USERS:-40}
FUND=${LOAD_FUND:-10000000}
[[ "$USERS" =~ ^[1-9][0-9]*$ && "$FUND" =~ ^[1-9][0-9]*$ ]] || die "LOAD_USERS/LOAD_FUND phải là số nguyên dương"
OUT=k6/.data/users.json
mkdir -p "$(dirname "$OUT")"

log "Đăng ký $USERS người gửi + 1 merchant (gateway giới hạn 5 rps/IP ở /api/auth — mất khoảng $(( (USERS + 1) / 4 ))s)"
declare -a EMAILS PASSWORDS TOKENS WALLETS
for i in $(seq 0 "$USERS"); do
  prefix=load-sender; (( i == USERS )) && prefix=load-merchant
  out=$(register_user "$prefix")
  read -r _ EMAILS[i] PASSWORDS[i] TOKENS[i] <<<"$out"
done

log "Mở ví (gửi hết rồi mới chờ — ledger mở tài khoản bất đồng bộ)"
for i in $(seq 0 "$USERS"); do
  TOKEN=${TOKENS[i]} http POST /api/v1/wallets "{\"currency\":\"$CURRENCY\"}"
  [[ "$HTTP_STATUS" == 201 ]] || die "Mở ví #$i: HTTP $HTTP_STATUS $HTTP_BODY"
  WALLETS[i]=$(jq -r .data.id <<<"$HTTP_BODY")
done
ID_LIST=$(printf "'%s'," "${WALLETS[@]}"); ID_LIST=${ID_LIST%,}
all_active() {
  [[ "$(psql_q walletdb "SELECT count(*) FROM wallets WHERE id IN ($ID_LIST) AND status = 'ACTIVE'")" \
     == "$(( USERS + 1 ))" ]]
}
wait_until "$(( USERS + 1 )) ví ACTIVE" 180 all_active

log "Nạp $FUND vào mỗi ví người gửi (một lần gọi producer cho cả lô)"
topup_many "$FUND" "${WALLETS[@]:0:USERS}"

{
  printf '{"merchant":"%s","users":[' "${WALLETS[USERS]}"
  for i in $(seq 0 $(( USERS - 1 ))); do
    (( i > 0 )) && printf ','
    printf '{"email":"%s","password":"%s","walletId":"%s"}' "${EMAILS[i]}" "${PASSWORDS[i]}" "${WALLETS[i]}"
  done
  printf ']}\n'
} >"$OUT"
chmod 600 "$OUT"
log "Đã ghi $OUT: $USERS người gửi (mỗi ví $FUND), merchant ${WALLETS[USERS]}"
