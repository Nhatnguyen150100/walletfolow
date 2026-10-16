#!/usr/bin/env bash
# =====================================================================
# Khôi phục database từ một bản backup.
#
#   ./scripts/restore.sh ./backups/20261016-120000
#
# ⚠️ GHI ĐÈ dữ liệu hiện có. Script sẽ hỏi xác nhận.
#
# Chạy script này ÍT NHẤT MỘT LẦN khi chưa có sự cố, và ghi lại thời gian thực tế
# vào docs/runbooks/restore.md. Thời gian khôi phục chỉ là con số thật nếu đã đo;
# còn lại nó chỉ là một ước lượng lạc quan.
# =====================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

SRC="${1:-}"
CONTAINER="${PG_CONTAINER:-wf-postgres}"
PG_USER="${DB_USER:-walletflow}"

if [[ -z "$SRC" || ! -d "$SRC" ]]; then
  echo "Dùng: ./scripts/restore.sh <thư-mục-backup>" >&2
  echo "Có sẵn:" >&2
  ls -1d ./backups/*/ 2>/dev/null >&2 || echo "  (chưa có bản backup nào)" >&2
  exit 1
fi

echo "📂 Bản backup: $SRC"
[[ -f "$SRC/MANIFEST.txt" ]] && cat "$SRC/MANIFEST.txt"
echo
read -rp "⚠️  GHI ĐÈ toàn bộ database hiện có trong $CONTAINER. Gõ 'yes' để tiếp tục: " confirm
[[ "$confirm" == "yes" ]] || { echo "Đã huỷ."; exit 1; }

START=$(date +%s)

if [[ -f "$SRC/00-globals.sql" ]]; then
  echo "   • role & quyền"
  docker exec -i "$CONTAINER" psql -U "$PG_USER" -d postgres < "$SRC/00-globals.sql" >/dev/null
fi

for DUMP in "$SRC"/*.dump; do
  [[ -e "$DUMP" ]] || continue
  DB=$(basename "$DUMP" .dump)
  echo "   • $DB"
  # --clean --if-exists: xoá object cũ trước khi dựng lại, nếu không thì restore
  # chồng lên schema cũ sẽ sinh ra trạng thái lai giữa hai phiên bản.
  docker exec -i "$CONTAINER" psql -U "$PG_USER" -d postgres \
    -c "DROP DATABASE IF EXISTS $DB" -c "CREATE DATABASE $DB" >/dev/null
  docker exec -i "$CONTAINER" pg_restore -U "$PG_USER" -d "$DB" --no-owner < "$DUMP"
done

ELAPSED=$(( $(date +%s) - START ))
echo "✅ Khôi phục xong trong ${ELAPSED}s"
echo
echo "Kiểm chứng NGAY — restore 'thành công' mà sổ cái lệch thì còn tệ hơn không restore:"
echo "  docker exec $CONTAINER psql -U $PG_USER -d ledgerdb -c 'SELECT SUM(amount_minor) AS phai_bang_0 FROM postings;'"
echo
echo "👉 Ghi ${ELAPSED}s vào docs/runbooks/restore.md làm RTO đo được."
