#!/usr/bin/env bash
# =====================================================================
# Definition of Done Phase 1 — kiểm END-TO-END qua gateway trên stack thật (roadmap 1.5a).
#
#   ./scripts/e2e/up.sh && ./scripts/e2e/phase1-dod.sh
#
# Mỗi khối dưới đây ánh xạ tới một ô DoD; hỏng ở đâu thì dừng ở đó với exit code ≠ 0. Chạy lại được
# nhiều lần trên cùng stack: mọi người dùng/ví/key đều sinh mới mỗi lần.
#
# Số liệu tiền dùng đơn vị minor (ADR-005). Mọi ghi sổ đi qua đường thật (gateway → transaction →
# Kafka → ledger); psql chỉ dùng để ĐỐI CHIẾU với sổ cái và để cố tình làm hỏng read model.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/e2e/lib.sh

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

DLT_BEFORE=$(dlt_total)
[[ "$DLT_BEFORE" =~ ^[0-9]+$ ]] || die "Không đọc được số message trong DLT"

# ---------------------------------------------------------------------
log "Chuẩn bị — Alice và Bob, mỗi người một ví; nạp 1.000.000 vào ví Alice"
# ---------------------------------------------------------------------
out=$(register_user alice); read -r ALICE_ID _ _ ALICE_TOKEN <<<"$out"
out=$(register_user bob);   read -r BOB_ID   _ _ BOB_TOKEN   <<<"$out"

# Service vừa khởi động có thể chưa đăng ký Eureka => gateway trả 503 một lúc
TOKEN=$ALICE_TOKEN wait_until "gateway định tuyến tới wallet-service" 180 \
  status_is 200 GET /api/v1/wallets/me
TOKEN=$ALICE_TOKEN wait_until "gateway định tuyến tới transaction-service" 180 \
  status_is 404 GET "/api/v1/transactions/$(uuid)"

ALICE_W=$(open_wallet "$ALICE_TOKEN")
BOB_W=$(open_wallet "$BOB_TOKEN")
topup "$ALICE_W" 1000000 >/dev/null
wait_until "read model của Alice thấy tiền nạp" 60 wallet_balance_is "$ALICE_TOKEN" 1000000
pass "Ví Alice $ALICE_W = 1000000, ví Bob $BOB_W = 0"

# ---------------------------------------------------------------------
log "DoD 1 — POST /transfer qua gateway → tiền chuyển, số dư hai ví đúng"
# ---------------------------------------------------------------------
K1="e2e-$(uuid)"
BODY1=$(transfer_body "$ALICE_W" "$BOB_W" 250000 "dod-1")
post_transfer "$ALICE_TOKEN" "$K1" "$BODY1"
assert_eq 202 "$HTTP_STATUS" "Lệnh chuyển được nhận (202)"
FIRST_BODY=$HTTP_BODY
TXN1=$(jq -r .data.transactionId <<<"$HTTP_BODY")

TOKEN=$ALICE_TOKEN wait_until "giao dịch $TXN1 COMPLETED" 90 txn_status_is "$TXN1" COMPLETED
pass "Giao dịch $TXN1 COMPLETED"
assert_eq 750000 "$(ledger_balance "$ALICE_W")" "Sổ cái: Alice còn 750000"
assert_eq 250000 "$(ledger_balance "$BOB_W")" "Sổ cái: Bob có 250000"
wait_until "read model khớp sổ cái" 60 wallet_balance_is "$ALICE_TOKEN" 750000
wait_until "read model khớp sổ cái" 60 wallet_balance_is "$BOB_TOKEN" 250000
pass "Read model: Alice 750000, Bob 250000"

# ---------------------------------------------------------------------
log "DoD 2 — gửi lại cùng Idempotency-Key → không trừ tiền lần hai"
# ---------------------------------------------------------------------
post_transfer "$ALICE_TOKEN" "$K1" "$BODY1"
assert_eq 202 "$HTTP_STATUS" "Gửi lại: vẫn 202"
assert_eq true "$(header_of Idempotent-Replayed)" "Gửi lại: Idempotent-Replayed = true"
assert_eq "$FIRST_BODY" "$HTTP_BODY" "Gửi lại: body khớp từng byte với lần đầu"

# 10 lần gửi lại SONG SONG — tất cả phải nhận ĐÚNG câu trả lời đã lưu (từng byte), đánh dấu replay
printf '%s' "$FIRST_BODY" >"$WORK/first-body"
for i in $(seq 10); do
  ( post_transfer "$ALICE_TOKEN" "$K1" "$BODY1"
    printf '%s' "$HTTP_BODY" >"$WORK/replay-body-$i"
    echo "$HTTP_STATUS $(header_of Idempotent-Replayed)" >"$WORK/replay-$i" ) &
done
wait
assert_eq "10 202 true" "$(cat "$WORK"/replay-[0-9]* | sort | uniq -c | awk '{print $1, $2, $3}')" \
  "10 lần gửi lại song song: đều 202 + Idempotent-Replayed"
for i in $(seq 10); do
  cmp -s "$WORK/first-body" "$WORK/replay-body-$i" || die "Lần gửi lại song song #$i: body khác lần đầu"
done
pass "10 lần gửi lại song song: body khớp từng byte với lần đầu"

# Race thật ở bước giành khoá: 10 request ĐẦU TIÊN cùng một key MỚI gửi cùng lúc → đúng MỘT lệnh
K_RACE="e2e-$(uuid)"
BODY_RACE=$(transfer_body "$ALICE_W" "$BOB_W" 1000 "dod-2-race")
for i in $(seq 10); do
  ( post_transfer "$ALICE_TOKEN" "$K_RACE" "$BODY_RACE"
    echo "$HTTP_STATUS $(jq -r .data.transactionId <<<"$HTTP_BODY")" >"$WORK/race-$i" ) &
done
wait
assert_eq 1 "$(cut -d' ' -f2 "$WORK"/race-* | sort -u | wc -l | tr -d ' ')" \
  "10 request đầu tiên song song cùng key: chung MỘT transactionId"
assert_eq "10 202" "$(cut -d' ' -f1 "$WORK"/race-* | sort | uniq -c | awk '{print $1, $2}')" \
  "10 request đầu tiên song song cùng key: đều 202"
TXN_RACE=$(cut -d' ' -f2 "$WORK/race-1")
TOKEN=$ALICE_TOKEN wait_until "giao dịch $TXN_RACE COMPLETED" 90 txn_status_is "$TXN_RACE" COMPLETED

post_transfer "$ALICE_TOKEN" "$K1" "$(transfer_body "$ALICE_W" "$BOB_W" 1 "dod-2-khac")"
assert_eq 422 "$HTTP_STATUS" "Cùng key, body khác → 422"
assert_eq IDEMPOTENCY_KEY_REUSED "$(jq -r .errorCode <<<"$HTTP_BODY")" "errorCode = IDEMPOTENCY_KEY_REUSED"

sleep 5   # nếu có lệnh thừa lọt qua, cho nó đủ thời gian đi tới sổ cái trước khi đếm
assert_eq 1 "$(journal_count "$TXN1")" "Sổ cái: đúng MỘT bút toán cho $TXN1"
# Bằng chứng quyết định là SỐ DƯ, không phải số bút toán theo TXN1: ledger có unique
# (tenant, transaction_id, type) nên không bao giờ ghi hai bút toán cùng TXN1 — idempotency hỏng ở
# transaction-service sẽ sinh lệnh với transactionId MỚI, chỉ lộ ra ở số dư và số giao dịch.
assert_eq 749000 "$(ledger_balance "$ALICE_W")" \
  "Sổ cái: Alice 749000 (−250000 một lần, −1000 một lần) — không bị trừ lần hai"
assert_eq 2 "$(psql_q txndb "SELECT count(*) FROM transactions WHERE from_wallet = '$ALICE_W'")" \
  "transaction-service: đúng 2 giao dịch dù đã nhận 23 request"

# ---------------------------------------------------------------------
log "DoD 3 — giết (SIGKILL) ledger rồi transaction-service giữa saga → giao dịch vẫn kết thúc đúng"
# ---------------------------------------------------------------------
# docker KILL (SIGKILL), không phải stop: dừng êm cho Spring kịp drain consumer, commit offset, xả
# outbox — tức là bỏ qua đúng những ca crash cần chứng minh (đã commit DB nhưng chưa ack Kafka...).
docker kill wf-ledger >/dev/null
log "  ledger đã bị giết"
K2="e2e-$(uuid)"
post_transfer "$ALICE_TOKEN" "$K2" "$(transfer_body "$ALICE_W" "$BOB_W" 100000 "dod-3")"
assert_eq 202 "$HTTP_STATUS" "Ledger chết vẫn NHẬN được lệnh (202)"
TXN2=$(jq -r .data.transactionId <<<"$HTTP_BODY")
TOKEN=$ALICE_TOKEN txn_status_is "$TXN2" PROCESSING || die "Giao dịch $TXN2 phải còn PROCESSING khi ledger chết"
pass "Giao dịch $TXN2 PROCESSING"

# Quá deadline saga (PT10S ở docker-compose.e2e.yml) => job quét gửi lại lệnh với eventId MỚI.
# Lệnh gốc + các lần gửi lại nằm chờ trong Kafka; ledger sống lại phải ghi đúng MỘT bút toán.
saga_resent() {
  [[ "$(psql_q txndb "SELECT attempt_count FROM saga_state WHERE transaction_id = '$TXN2'")" -ge 2 ]]
}
wait_until "job quét saga gửi lại lệnh" 90 saga_resent
pass "Job quét đã gửi lại lệnh (attempt_count ≥ 2)"

docker kill wf-transaction >/dev/null
log "  transaction-service đã bị giết; khởi động lại ledger"
docker start wf-ledger >/dev/null
wait_healthy wf-ledger
# Ledger ghi sổ trong lúc transaction-service CHẾT — câu trả lời nằm chờ trong Kafka
wait_until "ledger ghi sổ $TXN2" 120 journal_count_is "$TXN2" 1
pass "Ledger ghi sổ $TXN2 trong lúc transaction-service chết"

docker start wf-transaction >/dev/null
wait_healthy wf-transaction
TOKEN=$ALICE_TOKEN wait_until "giao dịch $TXN2 COMPLETED sau khi sống lại" 180 txn_status_is "$TXN2" COMPLETED
pass "Giao dịch $TXN2 COMPLETED"
sleep 5
assert_eq 1 "$(journal_count "$TXN2")" "Sổ cái: đúng MỘT bút toán dù lệnh được gửi nhiều lần"
assert_eq 649000 "$(ledger_balance "$ALICE_W")" "Sổ cái: Alice 649000"
assert_eq 351000 "$(ledger_balance "$BOB_W")" "Sổ cái: Bob 351000"
wait_until "read model Alice" 60 wallet_balance_is "$ALICE_TOKEN" 649000
wait_until "read model Bob" 60 wallet_balance_is "$BOB_TOKEN" 351000
pass "Read model: Alice 649000, Bob 351000"

# ---------------------------------------------------------------------
log "DoD 4 — rebuild read model → khớp sổ cái 100%"
# ---------------------------------------------------------------------
# Làm hỏng read model của Bob theo hai kiểu: số dư sai, và mất một dòng lịch sử
psql_q walletdb "UPDATE wallets SET balance_minor = balance_minor + 999999 WHERE id = '$BOB_W'"
psql_q walletdb "DELETE FROM wallet_entries WHERE seq =
                   (SELECT max(seq) FROM wallet_entries WHERE wallet_id = '$BOB_W')"
wallet_balance_is "$BOB_TOKEN" 351000 && die "Read model của Bob phải đang SAI trước khi rebuild"
pass "Read model của Bob đã bị làm hỏng"

TOKEN=$BOB_TOKEN http POST "/api/v1/admin/wallets/$BOB_W/rebuild"
assert_eq 403 "$HTTP_STATUS" "USER gọi rebuild → 403"

out=$(register_user admin); read -r _ ADMIN_EMAIL ADMIN_PASS _ <<<"$out"
promote_admin "$ADMIN_EMAIL"
ADMIN_TOKEN=$(login "$ADMIN_EMAIL" "$ADMIN_PASS")   # token cũ mang role USER — phải đăng nhập lại
TOKEN=$ADMIN_TOKEN http POST "/api/v1/admin/wallets/$BOB_W/rebuild"
assert_eq 200 "$HTTP_STATUS" "ADMIN gọi rebuild → 200"

LEDGER_BOB=$(ledger_balance "$BOB_W")
wallet_balance_is "$BOB_TOKEN" "$LEDGER_BOB" || die "Sau rebuild, read model của Bob ≠ sổ cái ($LEDGER_BOB)"
pass "Sau rebuild: read model Bob = sổ cái = $LEDGER_BOB"
assert_eq "$LEDGER_BOB" \
  "$(psql_q walletdb "SELECT COALESCE(SUM(amount_minor), 0) FROM wallet_entries WHERE wallet_id = '$BOB_W'")" \
  "Sau rebuild: tổng lịch sử ví = số dư"
assert_eq \
  "$(psql_q ledgerdb "SELECT count(*) FROM postings p JOIN accounts a ON a.id = p.account_id
                      WHERE a.tenant_id = '$TENANT_ID' AND a.external_ref = '$BOB_W'")" \
  "$(psql_q walletdb "SELECT count(*) FROM wallet_entries WHERE wallet_id = '$BOB_W'")" \
  "Sau rebuild: số dòng lịch sử = số vế ở sổ cái"

# ---------------------------------------------------------------------
log "DoD 5 — tenant & danh tính chỉ lấy từ token; dữ liệu người khác không lộ"
# ---------------------------------------------------------------------
TOKEN='' http GET /api/v1/wallets/me
assert_eq 401 "$HTTP_STATUS" "Không token → 401"

# Header danh tính/tenant do client tự gửi phải bị gateway xoá rồi gắn lại từ token. Nếu lọt
# xuống, wallet-service sẽ đọc tenant/user giả và trả danh sách khác.
TOKEN=$ALICE_TOKEN http GET /api/v1/wallets/me "" \
  -H "X-Tenant-Id: $(uuid)" -H "X-User-Id: $BOB_ID" -H "X-User-Role: ADMIN"
assert_eq 200 "$HTTP_STATUS" "Header giả: vẫn 200"
assert_eq "$ALICE_W" "$(jq -r '[.data[].id] | join(",")' <<<"$HTTP_BODY")" \
  "Header X-Tenant-Id / X-User-Id giả bị bỏ qua — vẫn chỉ thấy ví của Alice"
TOKEN=$ALICE_TOKEN http POST "/api/v1/admin/wallets/$BOB_W/rebuild" "" -H "X-User-Role: ADMIN"
assert_eq 403 "$HTTP_STATUS" "X-User-Role: ADMIN tự gửi không leo quyền được"

# "Không tồn tại" và "của người khác" phải trả CÙNG một lỗi (chống dò id). Chốt giá trị mong đợi
# TRƯỚC khi so với nhau: hai lần cùng 202/503/429 cũng "bằng nhau" mà không chứng minh được gì.
TOKEN=$BOB_TOKEN http GET "/api/v1/transactions/$TXN1"
assert_eq "404 NOT_FOUND" "$HTTP_STATUS $(jq -r .errorCode <<<"$HTTP_BODY")" \
  "Bob đọc giao dịch của Alice → 404 NOT_FOUND"
TOKEN=$BOB_TOKEN http GET "/api/v1/transactions/$(uuid)"
assert_eq "404 NOT_FOUND" "$HTTP_STATUS $(jq -r .errorCode <<<"$HTTP_BODY")" \
  "Giao dịch không tồn tại → cùng 404 NOT_FOUND"

post_transfer "$BOB_TOKEN" "e2e-$(uuid)" "$(transfer_body "$ALICE_W" "$BOB_W" 1 "dod-5")"
assert_eq "404 NOT_FOUND" "$HTTP_STATUS $(jq -r .errorCode <<<"$HTTP_BODY")" \
  "Bob chuyển tiền từ ví của Alice → 404 NOT_FOUND"
post_transfer "$BOB_TOKEN" "e2e-$(uuid)" "$(transfer_body "$(uuid)" "$BOB_W" 1 "dod-5")"
assert_eq "404 NOT_FOUND" "$HTTP_STATUS $(jq -r .errorCode <<<"$HTTP_BODY")" \
  "Chuyển từ ví không tồn tại → cùng 404 NOT_FOUND"
# Số dư của Alice được kiểm lại ở khối bất biến cuối — lúc đó mọi saga đã kết thúc, một lệnh lọt qua
# kiểm tra sở hữu không còn "chưa kịp" ghi sổ.

# ---------------------------------------------------------------------
log "Đồng thời — 100 lệnh song song từ một ví chỉ đủ tiền cho 10 lệnh (roadmap 1.2 test 3)"
# ---------------------------------------------------------------------
out=$(register_user carol); read -r _ _ _ CAROL_TOKEN <<<"$out"
out=$(register_user dave);  read -r _ _ _ DAVE_TOKEN  <<<"$out"
CAROL_W=$(open_wallet "$CAROL_TOKEN")
DAVE_W=$(open_wallet "$DAVE_TOKEN")
topup "$CAROL_W" 100000 >/dev/null

# Gateway giữ ngưỡng production (10 rps/user): phần lớn request sẽ gặp 429 và được gửi lại với
# CÙNG key — vừa đúng cách client phải làm, vừa là thêm một phép thử idempotency dưới tải.
for i in $(seq 100); do
  ( post_transfer "$CAROL_TOKEN" "e2e-$(uuid)" "$(transfer_body "$CAROL_W" "$DAVE_W" 10000 "song-song-$i")"
    echo "$HTTP_STATUS" >"$WORK/burst-$i" ) &
done
wait
assert_eq "100 202" "$(cat "$WORK"/burst-* | sort | uniq -c | awk '{print $1, $2}')" \
  "100 lệnh đều được nhận (202)"

all_done() {
  [[ "$(psql_q txndb "SELECT count(*) FROM transactions
                      WHERE from_wallet = '$CAROL_W' AND status IN ('COMPLETED', 'FAILED')")" == 100 ]]
}
wait_until "100 giao dịch kết thúc" 300 all_done
assert_eq "COMPLETED 10,FAILED 90" \
  "$(psql_q txndb "SELECT string_agg(status || ' ' || n, ',' ORDER BY status) FROM
                     (SELECT status, count(*) n FROM transactions WHERE from_wallet = '$CAROL_W'
                      GROUP BY status) s")" \
  "Đúng 10 lệnh thành công, 90 lệnh thất bại"
assert_eq INSUFFICIENT_FUNDS \
  "$(psql_q txndb "SELECT string_agg(DISTINCT failure_code, ',') FROM transactions
                   WHERE from_wallet = '$CAROL_W' AND status = 'FAILED'")" \
  "Mọi lệnh thất bại đều vì INSUFFICIENT_FUNDS"
assert_eq 0 "$(ledger_balance "$CAROL_W")" "Sổ cái: ví Carol về đúng 0, không âm"
assert_eq 100000 "$(ledger_balance "$DAVE_W")" "Sổ cái: ví Dave nhận đúng 100000"
wait_until "read model Carol" 60 wallet_balance_is "$CAROL_TOKEN" 0
wait_until "read model Dave" 60 wallet_balance_is "$DAVE_TOKEN" 100000
pass "Read model: Carol 0, Dave 100000"

# ---------------------------------------------------------------------
log "Bất biến toàn hệ thống"
# ---------------------------------------------------------------------
no_open_saga() {
  [[ "$(psql_q txndb "SELECT count(*) FROM transactions
                      WHERE from_wallet IN ('$ALICE_W', '$BOB_W')
                        AND status NOT IN ('COMPLETED', 'FAILED')")" == 0 ]]
}
wait_until "mọi giao dịch của Alice/Bob kết thúc" 120 no_open_saga
assert_eq 649000 "$(ledger_balance "$ALICE_W")" "Sổ cái: Alice vẫn 649000 — không lệnh lạ nào lọt qua"
assert_eq 351000 "$(ledger_balance "$BOB_W")" "Sổ cái: Bob vẫn 351000"
assert_eq 0 "$(psql_q txndb "SELECT count(*) FROM transactions WHERE initiated_by = '$BOB_ID'")" \
  "Không lệnh nào của Bob được nhận (mọi lần thử ở DoD 5 đều bị từ chối trước khi lưu)"
assert_eq 0 "$(psql_q ledgerdb "SELECT count(*) FROM (SELECT tenant_id FROM postings GROUP BY tenant_id
                                 HAVING SUM(amount_minor) <> 0) lech")" \
  "SUM(postings) = 0 theo từng tenant"
DLT_AFTER=$(dlt_total)
assert_eq "$DLT_BEFORE" "$DLT_AFTER" "Không message nào rơi vào DLT trong lần chạy"

log "✅ DoD Phase 1: tất cả kiểm tra đều PASS"
