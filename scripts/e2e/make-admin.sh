#!/usr/bin/env bash
# =====================================================================
# Nâng một user lên ADMIN trên stack DEV/E2E đang chạy (sửa thẳng authdb).
#
#   ./scripts/e2e/make-admin.sh <email>
#
# Chưa có cách nào tạo ADMIN qua API — quản trị người dùng thuộc roadmap 2.5. Token đang có vẫn
# mang role cũ: phải đăng nhập lại để nhận token có role ADMIN.
#
# ⚠️ Chỉ dùng cho stack dev/E2E. Ở môi trường thật, cấp quyền admin phải qua quy trình có audit.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/e2e/lib.sh

[[ $# -eq 1 ]] || { echo "Cách dùng: $0 <email>" >&2; exit 2; }
# Email đi thẳng vào câu SQL — chặn ký tự lạ thay vì cố escape
[[ "$1" =~ ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+$ ]] || { echo "Email không hợp lệ: $1" >&2; exit 2; }

promote_admin "$1"
log "Đã nâng $1 lên ADMIN (tenant $TENANT_ID) — đăng nhập lại để nhận token mới"
