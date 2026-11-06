#!/usr/bin/env bash
# =====================================================================
# Definition of Done Phase 2, ô 1 — "nạp tiền thật qua sandbox → số dư tăng đúng".
#
#   export BANK_PROVIDER=vnpay   # + VNPAY_* trong .env
#   ./scripts/e2e/up.sh && ./scripts/e2e/topup-vnpay.sh
#
# Khác `phase1-dod.sh` ở một chỗ quyết định: kịch bản này đi qua **API nạp tiền thật**
# (`POST /api/v1/transactions/topup`) chứ không nạp bằng cửa sau Kafka như `lib.sh:topup`. Đó là
# toàn bộ lý do nó tồn tại — đường `transaction → outbox → bank-adapter → provider → querydr →
# ledger → read model` chưa bao giờ được đi hết một lần ngoài Testcontainers.
#
# Hai chế độ, CÙNG một bộ kiểm tra:
#   BANK_PROVIDER=mock   — phiên tự chốt sau ~10s (walletflow.bank.mock.auto-settle-after). Không có
#                          tiền thật nào, KHÔNG tick được ô DoD; dùng để kiểm chính kịch bản này.
#   BANK_PROVIDER=vnpay  — dừng lại, in trang thanh toán, chờ bạn trả tiền trên sandbox thật. Đây là
#                          lần chạy lấy bằng chứng.
#
# Vì sao KHÔNG cần tunnel ra internet: kết quả cuối chỉ được phát sau khi adapter tự hỏi lại provider
# (`PendingChargeReconciler` + `querydr`, ADR-016 §1) — webhook chỉ là gợi ý. Return URL là redirect
# phía trình duyệt nên `localhost:8080` chạy được trên chính máy bạn. Muốn chứng minh luôn cả đường
# IPN thì tự dựng tunnel và đặt VNPAY_RETURN_URL trỏ ra ngoài; kịch bản này không đòi.
#
# Kết quả ghi ra docs/evidence/phase-2-topup-vnpay/ — xem docs/runbooks/e2e-topup-sandbox.md.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/e2e/lib.sh

# VND không có đơn vị phụ: 10000 minor = 10.000 đ, trên mức tối thiểu của sandbox VNPay.
AMOUNT_MINOR="${AMOUNT_MINOR:-10000}"
[[ "$AMOUNT_MINOR" =~ ^[1-9][0-9]*$ ]] || die "AMOUNT_MINOR phải là số nguyên dương: $AMOUNT_MINOR"
# 2400s = session.ttl PT15M + session.expiry-grace PT15M + một nhịp status-check.max-recheck-after
# PT10M. Phải cộng cả nhịp hỏi lại: `PendingChargeReconciler` giãn khoảng hỏi theo cấp số (30s, 1m,
# 2m, 4m, 8m, rồi mỗi 10m), nên trả tiền ở phút 14 vẫn có thể mãi phút 24 mới được xác nhận. Bỏ qua
# số hạng đó là cách sinh ra một file bằng chứng "failed" cho một lần nạp đã thành công.
# Vẫn nằm trong topup.payment-deadline PT45M của saga.
PAY_TIMEOUT="${PAY_TIMEOUT:-2400}"
[[ "$PAY_TIMEOUT" =~ ^[1-9][0-9]*$ ]] || die "PAY_TIMEOUT phải là số giây nguyên dương: $PAY_TIMEOUT"
EVIDENCE_DIR="${EVIDENCE_DIR:-docs/evidence/phase-2-topup-vnpay}"

PROVIDER="${BANK_PROVIDER:-}"
[[ -n "$PROVIDER" ]] || die "Phải đặt BANK_PROVIDER=vnpay (lấy bằng chứng) hoặc =mock (thử kịch bản).
  Biến của shell THẮNG .env khi compose nội suy, nên hãy export nó trước cả scripts/e2e/up.sh."

case "$PROVIDER" in
  vnpay)
    # Thiếu bí mật thì service đã không khởi động (CLAUDE.md §6) — nhưng báo ở đây rõ hơn nhiều so với
    # một container restart-loop mười phút sau.
    for v in VNPAY_TMN_CODE VNPAY_HASH_SECRET VNPAY_PAY_URL VNPAY_API_URL VNPAY_RETURN_URL VNPAY_SERVER_IP; do
      [[ -n "${!v:-}" ]] || die "Thiếu $v — nguồn duy nhất là .env của bạn, không có mặc định nào trong repo"
    done
    ;;
  mock)
    log "⚠️  BANK_PROVIDER=mock — KHÔNG có tiền thật nào được thu. Lần chạy này KHÔNG tick được ô DoD;"
    log "   nó chỉ chứng minh kịch bản và đường ống chạy đúng."
    ;;
  *) die "BANK_PROVIDER phải là 'vnpay' hoặc 'mock', nhận được: $PROVIDER" ;;
esac

STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
DLT_BEFORE=$(dlt_total)
[[ "$DLT_BEFORE" =~ ^[0-9]+$ ]] || die "Không đọc được số message trong DLT"

# Dòng ghi vào bằng chứng. Mọi `pass`/`assert_eq` quan trọng đều gọi kèm hàm này.
EVIDENCE_LINES=()
note() { EVIDENCE_LINES+=("$1"); }

# Ghi cả khi HỎNG. Một lần nạp thất bại trên sandbox thật là thứ đáng lưu ngang lần thành công: nó là
# bằng chứng cho ô DoD đang để trống, và là đầu mối cho runbook.
EVIDENCE_WRITTEN=
write_evidence() {
  local outcome=$1
  EVIDENCE_WRITTEN=1
  mkdir -p "$EVIDENCE_DIR"
  # Giờ-phút-giây, không chỉ ngày: vòng "hỏng → sửa → chạy lại" trong cùng một buổi sẽ ghi đè lên
  # nhau, mà lần hỏng mới là lần đáng giữ.
  local file="$EVIDENCE_DIR/$(date -u +%Y-%m-%dT%H%M%SZ)-$PROVIDER-$outcome.md"
  {
    printf '# E2E nạp tiền — %s, %s (%s)\n\n' "$PROVIDER" "$outcome" "$(date -u +%Y-%m-%d)"
    printf 'Sinh bởi `scripts/e2e/topup-vnpay.sh`. Bắt đầu %s, dừng %s (UTC).\n\n' \
      "$STARTED_AT" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if [[ "$PROVIDER" == mock ]]; then
      printf '> ⚠️ **Chạy với `mock` — không có tiền thật nào được thu.** Lần chạy này **không** đóng\n'
      printf '> được ô DoD "nạp tiền thật qua sandbox", dù mọi kiểm tra có xanh.\n\n'
    else
      printf '> Lần nạp trên VNPay sandbox, tiền được trả tay trên trình duyệt.\n\n'
    fi
    printf '## Đã kiểm\n\n'
    printf '%s\n' "${EVIDENCE_LINES[@]+"${EVIDENCE_LINES[@]}"}"
    printf '\n## Còn cần bạn đính kèm (nếu là lần lấy bằng chứng)\n\n'
    printf -- '- Ảnh chụp trang thanh toán VNPay và trang quay về.\n'
    printf -- '- `docker compose -p wf-e2e logs bank-adapter transaction-service` của khoảng thời gian trên.\n'
    printf '\nMọi URL trong file đã bị cắt query string: nó mang `vnp_TmnCode` và `vnp_SecureHash`.\n'
  } >"$file"
  log "Bằng chứng: $file"
}

# Hỏng ở BẤT KỲ bước nào cũng để lại bằng chứng của những gì đã kiểm được. Ca đáng sợ nhất là hỏng ở
# bước 5-6 trên sandbox thật: tiền đã ra khỏi tài khoản người dùng, và đó đúng là lúc cần hồ sơ nhất.
trap 'rc=$?; [[ $rc -eq 0 || -n "$EVIDENCE_WRITTEN" || ${#EVIDENCE_LINES[@]} -eq 0 ]] || write_evidence failed' EXIT

# Trang thanh toán mang vnp_TmnCode và vnp_SecureHash trong query string — in ra màn hình thì được
# (bạn phải mở nó), ghi vào file commit lên repo thì KHÔNG (bất biến §6).
mask_url() { sed -E 's/\?.*/?<query đã cắt — chứa vnp_TmnCode, vnp_SecureHash>/' <<<"$1"; }

# post_topup <Idempotency-Key> <body> — gửi lại với CÙNG key khi bị rate limit, như `lib.sh:post_transfer`.
# Gateway giữ nguyên ngưỡng production cho /api/v1/transactions/**; gặp 429 mà báo "kỳ vọng 202" thì
# kịch bản đang tố cáo sản phẩm vì lỗi của chính nó.
post_topup() {
  local _
  for _ in $(seq 30); do
    TOKEN=$TOKEN_U http POST /api/v1/transactions/topup "$2" -H "Idempotency-Key: $1"
    [[ "$HTTP_STATUS" == 429 ]] || return 0
    sleep 1
  done
}

# ---------------------------------------------------------------------
log "Chuẩn bị — một người dùng mới, một ví rỗng"
# ---------------------------------------------------------------------
# Giữ cả mật khẩu: access token chỉ sống 900 giây (auth-service `access-ttl-seconds`) trong khi lần
# chạy lấy bằng chứng chờ tới PAY_TIMEOUT = 2400 giây. Không có mật khẩu thì không có đường lấy token
# mới, và mọi lời gọi sau phút 15 trả 401 — kịch bản sẽ kết luận "giao dịch không kết thúc" trong khi
# tiền đã vào ví.
out=$(register_user topup); read -r _ EMAIL PASSWORD TOKEN_U <<<"$out"
TOKEN=$TOKEN_U wait_until "gateway định tuyến tới wallet-service" 180 status_is 200 GET /api/v1/wallets/me
TOKEN=$TOKEN_U wait_until "gateway định tuyến tới transaction-service" 180 \
  status_is 404 GET "/api/v1/transactions/$(uuid)"

WALLET=$(open_wallet "$TOKEN_U")
assert_eq 0 "$(ledger_balance "$WALLET")" "Sổ cái: ví mới mở = 0"
pass "Ví $WALLET của $EMAIL"

# ---------------------------------------------------------------------
log "Bước 1 — POST /api/v1/transactions/topup (API thật, không phải cửa sau Kafka)"
# ---------------------------------------------------------------------
IDEM="e2e-topup-$(uuid)"
BODY=$(printf '{"walletId":"%s","amountMinor":%s,"currency":"%s"}' "$WALLET" "$AMOUNT_MINOR" "$CURRENCY")
post_topup "$IDEM" "$BODY"
assert_eq 202 "$HTTP_STATUS" "Lệnh nạp được nhận (202)"
FIRST_BODY=$HTTP_BODY
TXN=$(jq -r .data.transactionId <<<"$HTTP_BODY")
[[ "$TXN" != null && -n "$TXN" ]] || die "Không đọc được transactionId: $HTTP_BODY"
note "- Giao dịch: \`$TXN\` · ví \`$WALLET\` · $AMOUNT_MINOR $CURRENCY (minor) · Idempotency-Key \`$IDEM\`"

# ---------------------------------------------------------------------
log "Bước 2 — chờ trang thanh toán (PENDING → AWAITING_PAYMENT, ADR-019)"
# ---------------------------------------------------------------------
TOKEN=$TOKEN_U wait_until "giao dịch $TXN tới AWAITING_PAYMENT" 180 txn_status_is "$TXN" AWAITING_PAYMENT
TOKEN=$TOKEN_U http GET "/api/v1/transactions/$TXN"
PAY_URL=$(jq -r .data.paymentUrl <<<"$HTTP_BODY")
[[ "$PAY_URL" != null && -n "$PAY_URL" ]] || die "AWAITING_PAYMENT nhưng không có paymentUrl"
pass "Có trang thanh toán"
note "- Trang thanh toán: \`$(mask_url "$PAY_URL")\`"

# ---------------------------------------------------------------------
log "Bước 3 — gửi lại cùng Idempotency-Key → không tạo phiên thu thứ hai (bất biến §2)"
# ---------------------------------------------------------------------
# Kiểm NGAY ở đây, lúc lệnh còn dang dở: đó mới là lúc một lần gửi lại do timeout thực sự xảy ra.
post_topup "$IDEM" "$BODY"
assert_eq 202 "$HTTP_STATUS" "Gửi lại: vẫn 202"
assert_eq true "$(header_of Idempotent-Replayed)" "Gửi lại: Idempotent-Replayed = true"
# transaction-service trả lại NGUYÊN VĂN body đã lưu, không serialize lại (CLAUDE.md Gotchas)
assert_eq "$FIRST_BODY" "$HTTP_BODY" "Gửi lại: body khớp từng byte với lần đầu"
note "- Gửi lại cùng \`Idempotency-Key\`: 202, \`Idempotent-Replayed: true\`, body khớp từng byte"

# ---------------------------------------------------------------------
log "Bước 4 — CHƯA trả tiền thì chưa ghi gì (bất biến đắt nhất của luồng nạp, ADR-019)"
# ---------------------------------------------------------------------
# Kiểm được ĐÚNG lúc này và không lúc nào khác: lệnh đã nhận, phiên đã mở, tiền chưa trả. Nếu ví đã
# tăng ở đây thì hệ thống đang cộng tiền dựa trên một lời hứa, không phải một lần thu.
assert_eq 0 "$(ledger_balance "$WALLET")" "Chưa trả tiền: sổ cái ví vẫn 0"
assert_eq 0 "$(journal_count "$TXN")" "Chưa trả tiền: chưa có bút toán nào"
wallet_balance_is "$TOKEN_U" 0 || die "Chưa trả tiền mà read model đã có tiền"
pass "Chưa trả tiền: read model vẫn 0"
note "- Lúc \`AWAITING_PAYMENT\`: sổ cái 0, chưa có bút toán, read model 0 — không ghi có trước khi thu"

# ---------------------------------------------------------------------
if [[ "$PROVIDER" == vnpay ]]; then
  cat >&2 <<EOF

  ================= CẦN BẠN LÀM TAY =================
  Mở trang dưới đây và trả tiền bằng thẻ test của VNPay sandbox:

$PAY_URL

  Trả xong, trình duyệt quay về $GATEWAY/payments/return/vnpay — trang đó CHỈ để hiển thị,
  nó không bao giờ là kết quả (PaymentReturnIT). Kịch bản vẫn chờ adapter tự hỏi lại VNPay.
  Chụp màn hình trang VNPay và trang quay về, để cạnh file bằng chứng.
  ===================================================

EOF
  log "Đang chờ tối đa ${PAY_TIMEOUT}s (hạn phiên + grace + một nhịp hỏi lại), poll mỗi 2s..."
else
  log "Mock tự chốt phiên sau ~10s (thay cho người bấm thanh toán)"
fi

# ---------------------------------------------------------------------
log "Bước 5 — tiền chỉ vào ví SAU KHI ledger ghi xong (ADR-019)"
# ---------------------------------------------------------------------
# Chờ một TRẠNG THÁI KẾT THÚC, không chỉ chờ COMPLETED: hỏng mà cứ chờ tiếp thì kịch bản chỉ im lặng
# tới lúc hết giờ, và người chạy mất cả tiếng để biết nó đã FAILED từ phút đầu.
txn_settled() {
  TOKEN=$TOKEN_U http GET "/api/v1/transactions/$TXN"
  # 401 = access token đã hết hạn giữa lúc chờ (900s, ngắn hơn PAY_TIMEOUT). Đăng nhập lại và hỏi
  # tiếp — coi nó là "chưa kết thúc" thì vòng lặp quay vô nghĩa tới hết giờ rồi kết luận sai về tiền.
  if [[ "$HTTP_STATUS" == 401 ]]; then
    log "Access token hết hạn — đăng nhập lại"
    TOKEN_U=$(login "$EMAIL" "$PASSWORD")
    TOKEN=$TOKEN_U http GET "/api/v1/transactions/$TXN"
  fi
  [[ "$HTTP_STATUS" == 200 ]] || return 1
  [[ "$(jq -r .data.status <<<"$HTTP_BODY")" =~ ^(COMPLETED|FAILED|MANUAL_REVIEW)$ ]]
}
wait_until "giao dịch $TXN kết thúc" "$PAY_TIMEOUT" txn_settled
STATUS=$(jq -r .data.status <<<"$HTTP_BODY")
if [[ "$STATUS" != COMPLETED ]]; then
  FAILURE_CODE=$(jq -r .data.failureCode <<<"$HTTP_BODY")
  note "- ❌ Kết cục: **$STATUS**, failureCode \`$FAILURE_CODE\`"
  write_evidence "$(tr '[:upper:]' '[:lower:]' <<<"$STATUS")"
  die "Giao dịch kết thúc ở $STATUS (failureCode $FAILURE_CODE).
  $STATUS không phải lỗi của kịch bản — xem docs/runbooks/topup-stuck.md."
fi
PROVIDER_REF=$(jq -r .data.providerRef <<<"$HTTP_BODY")
# COMPLETED mà không có mã lần thu thì không đối chiếu được với biên lai ngân hàng — chặn ngay, đừng
# để nó đi tiếp rồi ghi chữ `null` vào bằng chứng.
[[ "$PROVIDER_REF" != null && -n "$PROVIDER_REF" ]] || die "COMPLETED nhưng không có providerRef"
pass "Giao dịch $TXN COMPLETED, providerRef $PROVIDER_REF"

# ---------------------------------------------------------------------
log "Bước 6 — số dư tăng ĐÚNG, và sổ cái vẫn cân"
# ---------------------------------------------------------------------
assert_eq "$AMOUNT_MINOR" "$(ledger_balance "$WALLET")" "Sổ cái: ví tăng đúng $AMOUNT_MINOR"
wait_until "read model thấy tiền nạp" 60 wallet_balance_is "$TOKEN_U" "$AMOUNT_MINOR"
pass "Read model: $AMOUNT_MINOR"
assert_eq 1 "$(journal_count "$TXN")" "Đúng MỘT bút toán cho lệnh nạp — không ghi sổ hai lần"
assert_eq 0 "$(psql_q ledgerdb "SELECT count(*) FROM (SELECT tenant_id FROM postings GROUP BY tenant_id
                                 HAVING SUM(amount_minor) <> 0) lech")" \
  "SUM(postings) = 0 theo từng tenant"
note "- Sổ cái ví: $AMOUNT_MINOR · read model: $AMOUNT_MINOR · đúng 1 bút toán · \`SUM(postings) = 0\`"

# ---------------------------------------------------------------------
log "Bước 7 — lần thu phía provider khớp, và không có bí mật nào trong raw_response"
# ---------------------------------------------------------------------
# Đếm trước, đọc sau. `read` chỉ lấy dòng đầu, nên nếu có hàng CHARGE thứ hai cho cùng ví thì mọi
# khẳng định dưới đây vẫn xanh trong khi người dùng đã bị thu tiền hai lần — đúng thứ bất biến §2
# phải chặn. Đây là phép kiểm thật cho nhan đề "không tạo phiên thu thứ hai" ở bước 3.
assert_eq 1 "$(psql_q railsdb "SELECT count(*) FROM provider_transactions
                               WHERE tenant_id = '$TENANT_ID' AND wallet_id = '$WALLET'
                                 AND operation = 'CHARGE'")" \
  "Đúng MỘT lần thu cho ví này — lần gửi lại không mở phiên thứ hai"

PT=$(psql_q railsdb "SELECT status || ' ' || provider || ' ' || amount_minor || ' ' ||
                            (raw_response IS NOT NULL) || ' ' || coalesce(provider_ref, '-')
                     FROM provider_transactions
                     WHERE tenant_id = '$TENANT_ID' AND transaction_id = '$TXN' AND operation = 'CHARGE'")
[[ -n "$PT" ]] || die "Không thấy hàng provider_transactions nào cho $TXN"
read -r PT_STATUS PT_PROVIDER PT_AMOUNT PT_HAS_RESPONSE PT_REF <<<"$PT"
assert_eq SUCCEEDED "$PT_STATUS" "provider_transactions: SUCCEEDED"
assert_eq "$PROVIDER" "$PT_PROVIDER" "provider_transactions: provider = $PROVIDER"
assert_eq "$AMOUNT_MINOR" "$PT_AMOUNT" "provider_transactions: số tiền khớp lệnh nạp"
## `true`, không phải `t`: boolean bị nối vào chuỗi nên Postgres đã ép nó sang text trước khi psql in
assert_eq true "$PT_HAS_RESPONSE" "provider_transactions: có raw_response đã lưu"
assert_eq "$PROVIDER_REF" "$PT_REF" "provider_transactions: providerRef khớp cái API trả cho người dùng"

# Masking là lời hứa của ADR-017, không phải giả định — kiểm trên dữ liệu thật vừa ghi. So khớp bằng
# tay thay vì nhét bí mật vào câu SQL: một dấu nháy trong HashSecret vừa làm hỏng câu lệnh, vừa đẩy
# chính bí mật đó vào log lỗi của psql.
if [[ "$PROVIDER" == vnpay ]]; then
  RAW=$(psql_q railsdb "SELECT coalesce(raw_request::text, '') || coalesce(raw_response::text, '')
                        FROM provider_transactions
                        WHERE tenant_id = '$TENANT_ID' AND transaction_id = '$TXN'
                          AND operation = 'CHARGE'")
  if [[ "$RAW" == *"$VNPAY_HASH_SECRET"* ]]; then
    die "HashSecret nằm trong raw_request/raw_response của $TXN — ADR-017 bị vi phạm, KHÔNG commit bằng chứng"
  fi
  pass "Không có HashSecret nào nằm trong raw_request/raw_response"
  SECRET_NOTE=", không lộ \`VNPAY_HASH_SECRET\`"
else
  # Không có bí mật nào để tìm khi chạy mock — đừng ghi vào bằng chứng một phép kiểm đã không chạy
  SECRET_NOTE=""
fi
note "- \`provider_transactions\`: SUCCEEDED, $PT_AMOUNT $CURRENCY, đúng 1 lần thu, có \`raw_response\`$SECRET_NOTE"
# Ghi kết cục SAU CÙNG: một lần hỏng ở bước 6-7 không được để lại file nói "COMPLETED" ở giữa thân bài
note "- Kết cục: **COMPLETED** · providerRef \`$PROVIDER_REF\`"

DLT_AFTER=$(dlt_total)
assert_eq "$DLT_BEFORE" "$DLT_AFTER" "Không message nào rơi vào DLT trong lần chạy"
note "- DLT: $DLT_BEFORE trước, $DLT_AFTER sau — không message nào bị loại"

# ---------------------------------------------------------------------
# Bằng chứng. Ô DoD đòi "ảnh chụp/log lưu lại" (roadmap §Phase 2) — repo chưa có nơi lưu nào, đây là nó.
# ---------------------------------------------------------------------
write_evidence completed

if [[ "$PROVIDER" == vnpay ]]; then
  log "✅ DoD Phase 2 ô 1: nạp tiền qua sandbox VNPay → số dư tăng đúng. Tất cả kiểm tra PASS."
else
  log "✅ Kịch bản chạy đúng với mock. Chạy lại với BANK_PROVIDER=vnpay để lấy bằng chứng thật."
fi
