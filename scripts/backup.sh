#!/usr/bin/env bash
# =====================================================================
# Sao lưu toàn bộ database của WalletFlow.
#
#   ./scripts/backup.sh [thư-mục-đích]     (mặc định: ./backups)
#
# Hiện tại cả 5 database nằm trong MỘT container Postgres với MỘT volume.
# Mất volume đó là mất toàn bộ sổ cái — tức là mất nguồn sự thật về tiền, thứ
# không thể dựng lại từ bất kỳ đâu khác. Script này là biện pháp phòng vệ duy
# nhất cho tới khi Phase 4 đưa Postgres lên bản có HA + PITR.
# =====================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

CONTAINER="${PG_CONTAINER:-wf-postgres}"
PG_USER="${DB_USER:-walletflow}"
DEST="${1:-./backups}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$DEST/$STAMP"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-30}"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "❌ Container $CONTAINER không chạy. Bật hạ tầng trước:" >&2
  echo "   docker compose -f docker-compose.infra.yml up -d" >&2
  exit 1
fi

mkdir -p "$OUT"
echo "📦 Sao lưu từ container $CONTAINER -> $OUT"

# pg_dumpall để lấy cả role và quyền, KHÔNG chỉ dữ liệu: khôi phục mà thiếu role
# thì database restore xong vẫn không ai kết nối được — một bản backup "thành công"
# nhưng vô dụng, và chỉ phát hiện ra đúng vào lúc đang có sự cố.
docker exec "$CONTAINER" pg_dumpall -U "$PG_USER" --globals-only \
  > "$OUT/00-globals.sql"

for DB in authdb walletdb ledgerdb txndb railsdb; do
  if docker exec "$CONTAINER" psql -U "$PG_USER" -lqt | cut -d'|' -f1 | grep -qw "$DB"; then
    echo "   • $DB"
    # Format custom (-Fc) cho phép restore chọn lọc từng bảng và nén sẵn
    docker exec "$CONTAINER" pg_dump -U "$PG_USER" -Fc "$DB" > "$OUT/$DB.dump"
  else
    echo "   • $DB (chưa tồn tại, bỏ qua)"
  fi
done

# Ghi lại ngữ cảnh: bản backup không có thông tin về thời điểm và phiên bản schema
# thì lúc khôi phục sẽ phải đoán.
{
  echo "created_at=$(date -Iseconds)"
  echo "git_commit=$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "postgres_version=$(docker exec "$CONTAINER" postgres --version)"
} > "$OUT/MANIFEST.txt"

SIZE=$(du -sh "$OUT" | cut -f1)
echo "✅ Xong: $OUT ($SIZE)"

# Dọn bản cũ
find "$DEST" -maxdepth 1 -type d -name '20*' -mtime +"$RETENTION_DAYS" -print -exec rm -rf {} + 2>/dev/null || true

echo
echo "⚠️  Một bản backup CHƯA TỪNG restore thử thì chưa phải là backup."
echo "    Diễn tập khôi phục: ./scripts/restore.sh $OUT"
