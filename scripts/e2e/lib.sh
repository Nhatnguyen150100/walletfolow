#!/usr/bin/env bash
# =====================================================================
# Thư viện dùng chung cho E2E (roadmap 1.5a). `source` từ script khác, không chạy trực tiếp.
#
# Mọi request nghiệp vụ đi qua GATEWAY như client thật. Chỉ có ba đường "cửa sau", đều là công cụ
# của người vận hành chứ không phải của client:
#   - Kafka: nạp tiền (Phase 1 chưa có API top-up — roadmap 2.4)
#   - psql : đọc sổ cái để đối chiếu, nâng role ADMIN, cố tình làm hỏng read model trước khi rebuild
#   - docker stop/start: giết service giữa saga
# =====================================================================

GATEWAY="${GATEWAY:-http://localhost:8080}"
# Tenant mà auth-service phục vụ (auth.tenant-id) — mặc định của giai đoạn một khách hàng
TENANT_ID="${TENANT_ID:-00000000-0000-0000-0000-000000000001}"
CURRENCY="${CURRENCY:-VND}"
# Hai giá trị này đi thẳng vào câu SQL và message — chặn từ đầu thay vì escape từng chỗ
[[ "$TENANT_ID" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] \
  || { echo "TENANT_ID phải là UUID chữ thường: $TENANT_ID" >&2; exit 2; }
[[ "$CURRENCY" =~ ^[A-Z]{3}$ ]] || { echo "CURRENCY phải là mã 3 chữ in hoa: $CURRENCY" >&2; exit 2; }

# Tên container cố định trong docker-compose.yml
PG_CONTAINER=wf-postgres
KAFKA_CONTAINER=wf-kafka

# Windows thường không có jq: chạy bản trong Docker (chậm hơn ~1 giây mỗi lần, nhưng không bắt cài gì)
if ! command -v jq >/dev/null 2>&1; then
  jq() { docker run --rm -i ghcr.io/jqlang/jq:1.7.1 "$@"; }
fi

# ---------------------------------------------------------------------
# Log & assert — hỏng là dừng ngay: các bước sau dựa vào trạng thái của bước trước
# ---------------------------------------------------------------------
log()  { printf '\033[36m[e2e]\033[0m %s\n' "$*" >&2; }
pass() { printf '\033[32m  ✔ %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[31m  ✘ %s\033[0m\n' "$*" >&2; exit 1; }

assert_eq() {  # assert_eq <kỳ vọng> <thực tế> <mô tả>
  [[ "$1" == "$2" ]] || die "$3 — kỳ vọng '$1', thực tế '$2'"
  pass "$3"
}

# UUID v4 chữ thường (schema của message chỉ nhận chữ thường). Git Bash không có uuidgen.
uuid() {
  local h
  h=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
  printf '%s-%s-4%s-%x%s-%s\n' "${h:0:8}" "${h:8:4}" "${h:13:3}" \
    $(( (16#${h:16:1} & 3) | 8 )) "${h:17:3}" "${h:20:12}"
}

# wait_until <mô tả> <giây tối đa> <lệnh...> — chạy lệnh mỗi 2 giây tới khi thành công
wait_until() {
  local desc=$1 timeout=$2; shift 2
  local deadline=$(( SECONDS + timeout ))
  until "$@" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || die "Hết ${timeout}s chờ: $desc"
    sleep 2
  done
}

# ---------------------------------------------------------------------
# HTTP qua gateway. Kết quả ở HTTP_STATUS, HTTP_BODY, HTTP_HEADERS.
#   TOKEN=... http GET /api/v1/wallets/me
#   TOKEN=... http POST /api/v1/transactions/transfer "$json" -H "Idempotency-Key: k1"
#   TOKEN=... http GET /api/v1/wallets/me "" -H "X-Foo: bar"   # có header thêm => body "" bắt buộc
# ---------------------------------------------------------------------
http() {
  local method=$1 path=$2 body=${3:-}
  shift 2; [[ $# -gt 0 ]] && shift
  local out hdr
  out=$(mktemp); hdr=$(mktemp)
  local args=(-sS -o "$out" -D "$hdr" -w '%{http_code}' -X "$method" "$GATEWAY$path")
  [[ -n "${TOKEN:-}" ]] && args+=(-H "Authorization: Bearer $TOKEN")
  [[ -n "$body" ]] && args+=(-H 'Content-Type: application/json' --data "$body")
  # -w luôn in mã (000 khi không kết nối được) kể cả lúc curl trả lỗi — không nối thêm gì vào
  HTTP_STATUS=$(curl "${args[@]}" "$@") || true
  HTTP_BODY=$(cat "$out"); HTTP_HEADERS=$(cat "$hdr")
  rm -f "$out" "$hdr"
}

status_is() {  # status_is <mã mong đợi> <METHOD> <path> — dùng TOKEN của người gọi
  http "$2" "$3"
  [[ "$HTTP_STATUS" == "$1" ]]
}

# post_transfer <token> <Idempotency-Key> <body>. Bị rate limit (429) thì gửi lại với CÙNG key —
# đúng cách client phải làm; gateway giữ nguyên ngưỡng production nên ca song song sẽ gặp 429.
post_transfer() {
  local _
  for _ in $(seq 60); do
    TOKEN=$1 http POST /api/v1/transactions/transfer "$3" -H "Idempotency-Key: $2"
    [[ "$HTTP_STATUS" == 429 ]] || return 0
    sleep 1
  done
}

transfer_body() {  # transfer_body <ví nguồn> <ví đích> <số tiền> [ghi chú]
  printf '{"fromWalletId":"%s","toWalletId":"%s","amountMinor":%s,"currency":"%s","note":"%s"}' \
    "$1" "$2" "$3" "$CURRENCY" "${4:-e2e}"
}

txn_status_is() {  # txn_status_is <transaction id> <trạng thái> — dùng TOKEN của người gọi
  http GET "/api/v1/transactions/$1"
  [[ "$HTTP_STATUS" == 200 && "$(jq -r .data.status <<<"$HTTP_BODY")" == "$2" ]]
}

header_of() {  # header_of <tên> — đọc từ HTTP_HEADERS của lần gọi cuối, không phân biệt hoa thường
  printf '%s\n' "$HTTP_HEADERS" | tr -d '\r' | awk -v k="$(echo "$1" | tr '[:upper:]' '[:lower:]')" \
    -F': ' 'tolower($1) == k { print $2 }' | tail -1
}

# ---------------------------------------------------------------------
# Hạ tầng: psql, Kafka, container
# ---------------------------------------------------------------------
# Git Bash trên Windows tự đổi đối số bắt đầu bằng "/" thành đường dẫn Windows
# (/opt/kafka/... -> C:/Program Files/Git/opt/kafka/...) và lệnh trong container hỏng một cách khó hiểu.
# Chỉ tắt cho docker exec: tắt toàn cục thì curl/openssl (binary Windows) lại nhận /tmp/... sai.
dexec() {
  MSYS_NO_PATHCONV=1 docker exec "$@"
}

psql_q() {  # psql_q <db> <sql> — một giá trị/dòng, không header, không dòng trạng thái ("UPDATE 1")
  dexec "$PG_CONTAINER" psql -U walletflow -d "$1" -v ON_ERROR_STOP=1 -qtAc "$2"
}

container_healthy() {
  [[ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" == "healthy" ]]
}

wait_healthy() {  # wait_healthy <container> [giây]
  wait_until "container $1 healthy" "${2:-240}" container_healthy "$1"
}

# Tổng số message đang nằm trong mọi topic DLT (*.DLT). Một lệnh tiền rơi vào DLT là một lệnh không
# được thực thi — E2E so số này trước/sau để bắt mọi message bị loại trong lần chạy.
dlt_total() {
  local offsets
  # Gán vào biến trước: lỗi của lệnh phải làm hỏng E2E. Nuốt lỗi ở đây thì "0 message" và "không đọc
  # được Kafka" trông y hệt nhau — và phép so trước/sau PASS giả.
  offsets=$(dexec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic '.*\.DLT' --time -1) \
    || die "Không đọc được offset của các topic DLT"
  [[ -n "$offsets" ]] || die "Không thấy topic *.DLT nào — sai broker hoặc topic chưa được tạo"
  awk -F: '{ s += $NF } END { print s + 0 }' <<<"$offsets"
}

# ---------------------------------------------------------------------
# Người dùng & ví
# ---------------------------------------------------------------------
# register_user <tiền tố> -> in "userId email password token"
register_user() {
  local email password
  email="$1-$(uuid)@e2e.walletflow.dev"
  # Mật khẩu sinh mỗi lần chạy — không có mật khẩu cố định nào nằm trong repo
  password="E2e-$(uuid)"
  TOKEN='' http POST /api/auth/register \
    "{\"email\":\"$email\",\"password\":\"$password\",\"fullName\":\"E2E $1\"}"
  [[ "$HTTP_STATUS" == 201 ]] || die "Đăng ký $email: HTTP $HTTP_STATUS $HTTP_BODY"
  printf '%s %s %s %s\n' \
    "$(jq -r .data.userId <<<"$HTTP_BODY")" "$email" "$password" \
    "$(jq -r .data.accessToken <<<"$HTTP_BODY")"
}

login() {  # login <email> <password> -> in access token
  TOKEN='' http POST /api/auth/login "{\"email\":\"$1\",\"password\":\"$2\"}"
  [[ "$HTTP_STATUS" == 200 ]] || die "Đăng nhập $1: HTTP $HTTP_STATUS $HTTP_BODY"
  jq -r .data.accessToken <<<"$HTTP_BODY"
}

# Ví của người dùng (theo CURRENCY) như read model trả về: "id status balanceMinor"
my_wallet() {  # my_wallet <token>
  TOKEN=$1 http GET /api/v1/wallets/me
  [[ "$HTTP_STATUS" == 200 ]] || return 1
  jq -r --arg c "$CURRENCY" '.data[] | select(.currency == $c) | "\(.id) \(.status) \(.balanceMinor)"' \
    <<<"$HTTP_BODY"
}

wallet_status_is() {  # wallet_status_is <token> <status>
  local w; w=$(my_wallet "$1") || return 1
  [[ "$(cut -d' ' -f2 <<<"$w")" == "$2" ]]
}

wallet_balance_is() {  # wallet_balance_is <token> <số dư>
  local w; w=$(my_wallet "$1") || return 1
  [[ "$(cut -d' ' -f3 <<<"$w")" == "$2" ]]
}

# open_wallet <token> -> in wallet id, sau khi ledger đã mở tài khoản (ACTIVE)
open_wallet() {
  TOKEN=$1 http POST /api/v1/wallets "{\"currency\":\"$CURRENCY\"}"
  [[ "$HTTP_STATUS" == 201 ]] || die "Mở ví: HTTP $HTTP_STATUS $HTTP_BODY"
  local id; id=$(jq -r .data.id <<<"$HTTP_BODY")
  wait_until "ví $id ACTIVE" 60 wallet_status_is "$1" ACTIVE
  echo "$id"
}

# ---------------------------------------------------------------------
# Sổ cái (đọc thẳng ledgerdb — chỉ để ĐỐI CHIẾU, mọi ghi sổ vẫn đi qua Kafka)
# ---------------------------------------------------------------------
ledger_balance() {  # ledger_balance <wallet id>
  psql_q ledgerdb "SELECT COALESCE(SUM(p.amount_minor), 0)
                   FROM accounts a LEFT JOIN postings p ON p.account_id = a.id
                   WHERE a.tenant_id = '$TENANT_ID' AND a.external_ref = '$1'
                     AND a.currency = '$CURRENCY'"
}

journal_count() {  # journal_count <transaction id> — số bút toán ghi cho giao dịch đó
  psql_q ledgerdb "SELECT count(*) FROM journal_entries
                   WHERE tenant_id = '$TENANT_ID' AND transaction_id = '$1'"
}

journal_count_is() {  # journal_count_is <transaction id> <số>
  [[ "$(journal_count "$1")" == "$2" ]]
}

# topup <wallet id> <số tiền minor> — nạp tiền bằng lệnh PostJournal TOPUP qua Kafka, in transactionId.
# Đi đúng đường của mọi lệnh ghi sổ: schema, dedup, kiểm tra cân bằng, trạng thái tài khoản, outbox.
topup() {
  local wallet=$1 amount=$2 txn evt
  txn=$(uuid); evt=$(uuid)
  # Key = ví được nạp: cùng partition với các lệnh chuyển đi từ ví đó, nên ledger xử lý lệnh nạp
  # TRƯỚC các lệnh chuyển gửi sau nó (WalletTopics — thứ tự theo ví).
  # Message phải nằm trên MỘT dòng: console producer cắt message theo dấu xuống dòng.
  printf '%s|{"eventId":"%s","eventType":"PostJournal","version":1,"occurredAt":"%s","tenantId":"%s","traceId":null,"payload":{"transactionId":"%s","type":"TOPUP","referenceId":null,"currency":"%s","legs":[{"accountRef":"EXTERNAL_BANK","amountMinor":-%s},{"accountRef":"%s","amountMinor":%s}]}}\n' \
    "$wallet" "$evt" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$TENANT_ID" "$txn" "$CURRENCY" \
    "$amount" "$wallet" "$amount" \
  | dexec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic txn.command.post-journal.v1 \
      --property parse.key=true --property 'key.separator=|' >/dev/null
  wait_until "ledger ghi lệnh nạp $txn" 60 journal_count_is "$txn" 1
  echo "$txn"
}

# promote_admin <email> — nâng role trong authdb. Token cũ vẫn mang role USER: phải đăng nhập lại.
promote_admin() {
  local id
  id=$(psql_q authdb "UPDATE users SET role = 'ADMIN', updated_at = now()
                      WHERE tenant_id = '$TENANT_ID' AND lower(email) = lower('$1')
                      RETURNING id")
  [[ -n "$id" ]] || die "Không tìm thấy user $1 để nâng ADMIN"
}
