#!/usr/bin/env bash
# =====================================================================
# Nạp tiền vào một ví trên stack đang chạy — CÔNG CỤ DEV/E2E, không phải tính năng.
#
#   ./scripts/e2e/seed-topup.sh <wallet-id> <số tiền minor>      # ví dụ: ... 3f2b...-... 1000000
#
# Phase 1 chưa có API nạp tiền (top-up qua cổng thanh toán là roadmap 2.4). Script gửi lệnh
# PostJournal TOPUP (EXTERNAL_BANK → ví) lên txn.command.post-journal.v1, tức đi ĐÚNG đường của mọi
# lệnh ghi sổ: schema, dedup, kiểm tra cân bằng + trạng thái tài khoản, outbox. Không có cửa sau
# nào trong code service để "tạo tiền".
#
# Biến môi trường: TENANT_ID (mặc định tenant 0001), CURRENCY (mặc định VND).
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/e2e/lib.sh

[[ $# -eq 2 && "$2" =~ ^[1-9][0-9]*$ ]] || { echo "Cách dùng: $0 <wallet-id> <số tiền minor > 0>" >&2; exit 2; }
# Wallet id đi thẳng vào message và câu SQL đối chiếu — chỉ nhận UUID chữ thường như schema yêu cầu
[[ "$1" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] \
  || { echo "Wallet id phải là UUID chữ thường: $1" >&2; exit 2; }

TXN=$(topup "$1" "$2")
log "Đã ghi sổ lệnh nạp $TXN: +$2 $CURRENCY vào ví $1 (số dư sổ cái: $(ledger_balance "$1"))"
