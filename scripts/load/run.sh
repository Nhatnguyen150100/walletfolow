#!/usr/bin/env bash
# =====================================================================
# Chạy một lượt load test (roadmap 1.5b) rồi thu số liệu cơ sở.
#
#   ./scripts/load/run.sh cold|hot        # LEVELS=5,10,20,40,80 STAGE_SEC=60 GAP_SEC=10 (env)
#
# 1. k6 (image grafana/k6, trong network của compose) bắn lệnh chuyển qua gateway theo từng mức TPS.
# 2. Chờ mọi lệnh của lượt này kết thúc VÀ outbox rỗng — đo xong mà còn tồn là số đo dở, và lượt sau
#    sẽ đo nhầm độ trễ của lượt này.
# 3. Thu số liệu: độ trễ nhận lệnh (k6), độ trễ END-TO-END theo mức (SQL theo note), tốc độ relay
#    outbox (= trần công suất), lag/Hikari/CPU (Prometheus), tốc độ xả hàng đợi (tạm thời).
# 4. Kiểm tiền SAU tải: SUM(postings)=0, không ví âm, không ghi sổ trùng, tổng tiền của các ví seed
#    không đổi, read model khớp sổ cái, DLT không tăng. Tải cao làm chậm là chuyện của số đo; tải cao
#    làm SAI TIỀN là bug — script dừng với exit ≠ 0.
#
# Kết quả: in ra stdout và ghi k6/.out/report-<RUN_ID>.md (gitignore) để dán vào docs/benchmarks.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/e2e/lib.sh

MODE=${1:-}
[[ "$MODE" == cold || "$MODE" == hot ]] || { echo "Cách dùng: $0 cold|hot" >&2; exit 2; }
DATA=k6/.data/users.json
[[ -f "$DATA" ]] || die "Chưa có $DATA — chạy ./scripts/load/seed.sh trước"
LEVELS=${LEVELS:-5,10,20,40,80}
STAGE_SEC=${STAGE_SEC:-60}
GAP_SEC=${GAP_SEC:-10}
AMOUNT=${AMOUNT:-1000}
[[ "$LEVELS" =~ ^[0-9]+(,[0-9]+)*$ && "$STAGE_SEC" =~ ^[0-9]+$ && "$GAP_SEC" =~ ^[0-9]+$ \
   && "$AMOUNT" =~ ^[1-9][0-9]*$ ]] || die "LEVELS/STAGE_SEC/GAP_SEC/AMOUNT không hợp lệ"

# Token k6 lấy lúc setup sống 15 phút (JWT_ACCESS_TTL_SECONDS=900) — hết hạn giữa lượt thì các mức cuối
# toàn 401 và bị đọc thành "hệ thống sập". Chừa ~2 phút cho phần đăng nhập trong setup.
N_LEVELS=$(tr ',' '\n' <<<"$LEVELS" | wc -l | tr -d ' ')
PLANNED=$(( N_LEVELS * (STAGE_SEC + GAP_SEC) ))
(( PLANNED <= 780 )) || die "Lịch tải ${PLANNED}s vượt TTL token 15 phút — giảm LEVELS hoặc STAGE_SEC"

RUN_ID="$(date -u +%Y%m%dT%H%M%S)-$MODE"
mkdir -p k6/.out
REPORT=k6/.out/report-$RUN_ID.md
PROM=http://localhost:9090
curl -sf "$PROM/-/ready" >/dev/null || die "Prometheus chưa chạy — dùng ./scripts/load/up.sh"
prom_time() {  # thời điểm theo đồng hồ của Prometheus — đồng hồ VM WSL2 có thể lệch với host
  curl -sfG "$PROM/api/v1/query" --data-urlencode 'query=time()' | jq -r '.data.result[1]' | tr -d '\r'
}

# Outbox chưa gửi của cả ba service. "Mọi giao dịch đã COMPLETED" CHƯA phải là xả xong: mỗi giao
# dịch còn để lại event txn.event.completed trong outbox của transaction-service — lượt sau chạy chồng
# lên thì lệnh của nó xếp hàng sau đống đó và đo ra độ trễ của lượt TRƯỚC (đã gặp: hot_005 p50 15s).
outbox_backlog() {
  local db total=0 n
  for db in txndb ledgerdb walletdb; do
    n=$(psql_q "$db" "SELECT count(*) FROM outbox_event WHERE published = false")
    total=$(( total + n ))
  done
  echo "$total"
}
outbox_empty() { [[ "$(outbox_backlog)" == 0 ]]; }
log "Chờ outbox rỗng trước khi đo (đang tồn $(outbox_backlog))"
wait_until "outbox của lượt trước xả hết" 1800 outbox_empty

# Ví của lượt seed: đi thẳng vào SQL — chỉ nhận UUID
WALLETS=$(jq -r '[.merchant] + [.users[].walletId] | .[]' <"$DATA" | tr -d '\r')
N_WALLETS=$(wc -w <<<"$WALLETS" | tr -d ' ')
for w in $WALLETS; do
  [[ "$w" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] || die "Wallet id không hợp lệ trong $DATA: $w"
done
ID_LIST=$(printf "'%s'," $WALLETS); ID_LIST=${ID_LIST%,}

# Tổng số dư sổ cái của các ví seed: mọi lệnh của lượt chỉ chuyển GIỮA các ví này, nên tổng phải giữ
# nguyên. SUM(postings)=0 không bắt được ghi sổ trùng (bút toán trùng vẫn cân); bảo toàn này + đếm bút
# toán bên dưới thì bắt được.
seed_total() {
  psql_q ledgerdb "SELECT COALESCE(SUM(p.amount_minor), 0) FROM accounts a JOIN postings p ON p.account_id = a.id
                   WHERE a.tenant_id = '$TENANT_ID' AND a.currency = '$CURRENCY' AND a.external_ref IN ($ID_LIST)"
}
SEED_TOTAL_BEFORE=$(seed_total)
DLT_BEFORE=$(dlt_total)
T0=$(prom_time)

log "k6: MODE=$MODE LEVELS=$LEVELS STAGE_SEC=$STAGE_SEC RUN_ID=$RUN_ID"
# Gateway không có cổng nào khác ngoài 8080; k6 chạy trong network của compose và gọi thẳng
# api-gateway:8080 — vẫn đi qua JWT + rate limit như client ngoài.
K6_DIR=$(pwd -W 2>/dev/null || pwd)/k6
# Image k6 chạy bằng uid 12345: trên Linux nó không đọc được users.json (600) của uid host và không
# ghi được k6/.out — chạy bằng chính uid/gid của người gọi. Docker Desktop (Windows/macOS) không cần.
USER_FLAG=()
if [[ "$(uname -s)" == Linux ]]; then USER_FLAG=(--user "$(id -u):$(id -g)"); fi
MSYS_NO_PATHCONV=1 docker run --rm --network wf-e2e_default "${USER_FLAG[@]}" \
  -v "$K6_DIR:/scripts" \
  -e GATEWAY=http://api-gateway:8080 -e MODE="$MODE" -e RUN_ID="$RUN_ID" \
  -e LEVELS="$LEVELS" -e STAGE_SEC="$STAGE_SEC" -e GAP_SEC="$GAP_SEC" -e AMOUNT="$AMOUNT" \
  -e CURRENCY="$CURRENCY" \
  grafana/k6:0.54.0 run --quiet /scripts/transfer.js | tee "k6/.out/k6-$RUN_ID.txt"
DB_T_LOAD_END=$(psql_q ledgerdb "SELECT now()")
T_LOAD_END=$(prom_time)
# Bảng độ trễ NHẬN LỆNH (202) theo mức do handleSummary của k6 in ra
K6_TABLE=$(grep '^|' "k6/.out/k6-$RUN_ID.txt" || true)

NOTE_PREFIX="load:$RUN_ID:"
open_count() {
  psql_q txndb "SELECT count(*) FROM transactions
                WHERE note LIKE '${NOTE_PREFIX}%' AND status NOT IN ('COMPLETED', 'FAILED')"
}
drained() { [[ "$(open_count)" == 0 ]]; }
BACKLOG_AT_LOAD_END=$(open_count)
log "Chờ hàng đợi xả hết (còn $BACKLOG_AT_LOAD_END lệnh chưa kết thúc)"
wait_until "mọi lệnh của $RUN_ID kết thúc" 1800 drained
T_DRAINED=$(prom_time)
log "Chờ outbox xả hết event còn lại (đang tồn $(outbox_backlog))"
wait_until "outbox xả hết" 1800 outbox_empty
T_END=$(prom_time)
WINDOW=$(awk -v a="$T0" -v b="$T_END" 'BEGIN { printf "%d", b - a + 1 }')

# ---------------------------------------------------------------------
# Độ trễ end-to-end theo mức — chỉ lệnh COMPLETED (FAILED đếm riêng). updated_at được đặt đúng lúc
# chuyển sang trạng thái cuối (TransactionRepository.transition) và không bị đổi lại sau đó.
# ---------------------------------------------------------------------
E2E_TABLE=$(psql_q txndb "
  SELECT '| ' || split_part(note, ':', 3)
      || ' | ' || count(*)
      || ' | ' || count(*) FILTER (WHERE status = 'COMPLETED')
      || ' | ' || count(*) FILTER (WHERE status = 'FAILED')
      || ' | ' || COALESCE(round(percentile_cont(0.50) WITHIN GROUP (ORDER BY ms) FILTER (WHERE status = 'COMPLETED')::numeric)::text, '-')
      || ' | ' || COALESCE(round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) FILTER (WHERE status = 'COMPLETED')::numeric)::text, '-')
      || ' | ' || COALESCE(round(percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) FILTER (WHERE status = 'COMPLETED')::numeric)::text, '-')
      || ' | ' || COALESCE(round((max(ms) FILTER (WHERE status = 'COMPLETED'))::numeric)::text, '-') || ' |'
  FROM (SELECT note, status, extract(epoch FROM updated_at - created_at) * 1000 AS ms
        FROM transactions WHERE note LIKE '${NOTE_PREFIX}%') t
  GROUP BY split_part(note, ':', 3) ORDER BY 1")

# ---------------------------------------------------------------------
# Tốc độ XẢ hàng đợi sau khi k6 dừng — số TẠM THỜI, KHÔNG phải công suất bền vững. Outbox gửi theo
# FIFO: trong pha xả relay dành gần hết sức cho các PostJournal còn tồn, còn event txn.event.completed
# sinh ra trong pha này xếp sau và được trả dần ("outbox rỗng sau thêm Ns"). Công suất BỀN VỮNG đọc
# từ relay: relay publish (bản ghi/s) ÷ 2 bản ghi outbox mỗi giao dịch ở transaction-service.
# ---------------------------------------------------------------------
if (( BACKLOG_AT_LOAD_END >= 200 )); then
  DRAIN_RATE=$(psql_q txndb "
    SELECT COALESCE(count(*) || ' lệnh trong ' || round(extract(epoch FROM max(updated_at)
             - '$DB_T_LOAD_END'::timestamptz)::numeric) || ' s = '
             || round((count(*) / NULLIF(extract(epoch FROM max(updated_at) - '$DB_T_LOAD_END'::timestamptz), 0))::numeric, 1)
             || ' TPS', '(không có lệnh hoàn tất sau khi k6 dừng)')
    FROM transactions
    WHERE note LIKE '${NOTE_PREFIX}%' AND status = 'COMPLETED' AND updated_at >= '$DB_T_LOAD_END'")
else
  DRAIN_RATE="không đo — hệ thống KHÔNG bão hoà (chỉ $BACKLOG_AT_LOAD_END lệnh tồn khi k6 dừng)"
fi

# ---------------------------------------------------------------------
# Prometheus: giá trị lớn nhất trong cửa sổ chạy, theo service. Gauge được lấy mẫu mỗi 10s — đỉnh
# ngắn hơn thế có thể bị sót: "0" không chứng minh là không xảy ra.
# ---------------------------------------------------------------------
prom_max() {  # prom_max <biểu thức> — in "job=giá trị" mỗi dòng, hoặc "(không có series)"
  local res
  res=$(curl -sfG "$PROM/api/v1/query" \
          --data-urlencode "query=max by (job) (max_over_time(($1)[${WINDOW}s:10s]))" \
          --data-urlencode "time=$T_END") || { echo "(lỗi truy vấn)"; return; }
  jq -r 'if (.data.result | length) == 0 then "(không có series)"
         else .data.result[] | "\(.metric.job)=\(.value[1] | tonumber | . * 100 | round / 100)" end' <<<"$res" \
    | tr -d '\r' | paste -sd ' ' -
}
RELAY_RATE=$(prom_max 'rate(walletflow_outbox_published_total[30s])')
OUTBOX_AGE=$(prom_max 'walletflow_outbox_oldest_age_seconds')
OUTBOX_PENDING=$(prom_max 'walletflow_outbox_pending')
CONSUMER_LAG=$(prom_max 'kafka_consumer_fetch_manager_records_lag_max')
HIKARI_PENDING=$(prom_max 'hikaricp_connections_pending')
HIKARI_ACTIVE=$(prom_max 'hikaricp_connections_active')
# process_cpu_usage đã CHIA cho số nhân của VM: 1 thread chạy kín chỉ hiện ~1/NCPU
CPU=$(prom_max 'process_cpu_usage')
NCPU=$(docker info --format '{{.NCPU}}' 2>/dev/null || echo '?')

# Saga bị job quét gửi lại trong lượt = số đo bị nhiễm lệnh lặp (deadline production PT2M)
RESENT=$(psql_q txndb "SELECT count(*) FROM saga_state s JOIN transactions t ON t.id = s.transaction_id
                       WHERE t.note LIKE '${NOTE_PREFIX}%' AND s.attempt_count > 1")

# ---------------------------------------------------------------------
# Tiền sau tải
# ---------------------------------------------------------------------
log "Kiểm tiền sau tải"
assert_eq 0 "$(psql_q ledgerdb "SELECT count(*) FROM (SELECT tenant_id FROM postings GROUP BY tenant_id
                                 HAVING SUM(amount_minor) <> 0) lech")" "SUM(postings) = 0 theo từng tenant"
assert_eq 0 "$(psql_q ledgerdb "SELECT count(*) FROM (SELECT a.id FROM accounts a JOIN postings p ON p.account_id = a.id
                                 WHERE a.type = 'USER' GROUP BY a.id HAVING SUM(p.amount_minor) < 0) am")" \
  "Không tài khoản USER nào âm"
COMPLETED=$(psql_q txndb "SELECT count(*) FROM transactions WHERE note LIKE '${NOTE_PREFIX}%' AND status = 'COMPLETED'")
# Mỗi lệnh COMPLETED của lượt có đúng MỘT bút toán TRANSFER, và lệnh không COMPLETED thì không có bút
# toán nào. Lọc theo đúng transaction_id của lượt (không theo cửa sổ thời gian — thứ gì khác ghi sổ
# cùng lúc sẽ bị đếm nhầm). Danh sách id đi qua stdin vào bảng tạm: vài nghìn UUID trong dòng lệnh vượt
# giới hạn độ dài dòng lệnh của Windows. Unique (tenant, transaction_id, type) ở ledger đã chặn bút
# toán trùng từ DB — kiểm tra này xác nhận lại trên hệ thống thật.
RUN_TXN_IDS=$(psql_q txndb "SELECT id || ',' || status FROM transactions WHERE note LIKE '${NOTE_PREFIX}%'")
JOURNALS=$( { printf 'CREATE TEMP TABLE run_txn (id uuid PRIMARY KEY, status text);\n'
              printf 'COPY run_txn FROM STDIN WITH (FORMAT csv);\n%s\n\\.\n' "$RUN_TXN_IDS"
              printf "SELECT count(*) FILTER (WHERE r.status = 'COMPLETED') || ' ' || count(*) FILTER (WHERE r.status <> 'COMPLETED')
                      FROM journal_entries j JOIN run_txn r ON r.id = j.transaction_id
                      WHERE j.tenant_id = '%s' AND j.type = 'TRANSFER';\n" "$TENANT_ID"
            } | dexec -i "$PG_CONTAINER" psql -U walletflow -d ledgerdb -v ON_ERROR_STOP=1 -qtA )
assert_eq "$COMPLETED 0" "$JOURNALS" \
  "Bút toán TRANSFER của lượt: đúng $COMPLETED (= số COMPLETED), 0 cho lệnh chưa/không hoàn tất"
assert_eq "$SEED_TOTAL_BEFORE" "$(seed_total)" "Tổng số dư của $N_WALLETS ví seed không đổi ($SEED_TOTAL_BEFORE)"

ledger_side() {
  psql_q ledgerdb "SELECT a.external_ref || ' ' || COALESCE(SUM(p.amount_minor), 0)
                   FROM accounts a LEFT JOIN postings p ON p.account_id = a.id
                   WHERE a.tenant_id = '$TENANT_ID' AND a.currency = '$CURRENCY' AND a.external_ref IN ($ID_LIST)
                   GROUP BY a.external_ref ORDER BY 1"
}
LEDGER_SIDE=$(ledger_side)
assert_eq "$N_WALLETS" "$(grep -c . <<<"$LEDGER_SIDE")" "Sổ cái có đủ $N_WALLETS tài khoản seed"
read_model_matches() {
  local side
  side=$(psql_q walletdb "SELECT id || ' ' || balance_minor FROM wallets WHERE id IN ($ID_LIST) ORDER BY 1") || return 1
  [[ "$(grep -c . <<<"$side")" == "$N_WALLETS" && "$side" == "$LEDGER_SIDE" ]]
}
wait_until "read model của $N_WALLETS ví khớp sổ cái" 300 read_model_matches
pass "Read model của $N_WALLETS ví khớp sổ cái"
DLT_AFTER=$(dlt_total)
assert_eq "$DLT_BEFORE" "$DLT_AFTER" "Không message nào rơi vào DLT"

secs() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%d", b - a }'; }
{
  echo "## Lượt $RUN_ID"
  echo
  echo "- Mức: \`$LEVELS\` TPS × ${STAGE_SEC}s (nghỉ ${GAP_SEC}s) · tải ${MODE} · tải chạy $(secs "$T0" "$T_LOAD_END")s (gồm đăng nhập trong setup), giao dịch kết thúc hết sau thêm $(secs "$T_LOAD_END" "$T_DRAINED")s, outbox rỗng sau thêm $(secs "$T_DRAINED" "$T_END")s"
  echo "- Lệnh COMPLETED: $COMPLETED · còn tồn khi k6 dừng: $BACKLOG_AT_LOAD_END · saga bị gửi lại: $RESENT"
  echo "- Relay transaction-service lớn nhất: xem bảng Prometheus — **công suất bền vững ≈ relay ÷ 2** (2 bản ghi outbox mỗi giao dịch)"
  echo "- Tốc độ xả hàng đợi sau khi k6 dừng (TẠM THỜI, không phải công suất bền vững — xem comment trong run.sh): $DRAIN_RATE"
  echo
  echo "Độ trễ NHẬN LỆNH (POST → 202, phía client k6), theo mức:"
  echo
  echo "$K6_TABLE"
  echo
  echo "Độ trễ END-TO-END (nhận lệnh → COMPLETED), theo mức:"
  echo
  echo "| scenario | lệnh | COMPLETED | FAILED | p50 ms | p95 ms | p99 ms | max ms |"
  echo "|---|---|---|---|---|---|---|---|"
  echo "$E2E_TABLE"
  echo
  echo "Prometheus — giá trị lớn nhất trong cửa sổ ${WINDOW}s (gauge lấy mẫu mỗi 10s):"
  echo
  echo "| chỉ số | theo service |"
  echo "|---|---|"
  echo "| relay publish (bản ghi/s, rate 30s) | $RELAY_RATE |"
  echo "| tuổi outbox cũ nhất (s) | $OUTBOX_AGE |"
  echo "| outbox chờ gửi | $OUTBOX_PENDING |"
  echo "| consumer lag (records) | $CONSUMER_LAG |"
  echo "| Hikari chờ kết nối | $HIKARI_PENDING |"
  echo "| Hikari đang dùng | $HIKARI_ACTIVE |"
  echo "| CPU tiến trình (0–1, đã chia cho $NCPU nhân) | $CPU |"
  echo
  echo "Tiền sau tải: SUM(postings)=0 ✔ · không ví âm ✔ · không ghi sổ trùng ✔ · tổng ví seed không đổi ✔ · read model khớp sổ cái ✔ · DLT không tăng ✔"
} | tee "$REPORT"
if (( RESENT > 0 )); then
  log "⚠ $RESENT saga bị job quét gửi lại trong lượt — độ trễ ở các mức cao có lẫn lệnh lặp"
fi
log "Báo cáo: $REPORT"
