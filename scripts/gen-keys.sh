#!/usr/bin/env bash
# =====================================================================
# Sinh cặp khoá RSA để auth-service ký JWT (RS256) và ghi vào .env
#
#   ./scripts/gen-keys.sh
#
# auth-service giữ private key và KÝ. api-gateway chỉ lấy public key qua JWKS
# và chỉ VERIFY được — nên gateway bị chiếm cũng không phát hành được token.
#
# Khoá sinh ra KHÔNG BAO GIỜ được commit: .env đã nằm trong .gitignore.
# =====================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE=".env"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

if ! command -v openssl >/dev/null 2>&1; then
  echo "❌ Cần openssl. Cài: sudo apt install openssl" >&2
  exit 1
fi

echo "🔑 Sinh cặp khoá RSA 2048-bit..."
# PKCS#8 là định dạng mà Java KeyFactory đọc được trực tiếp (xem JwtKeyProvider).
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out "$TMP_DIR/private.pem" 2>/dev/null
openssl rsa -in "$TMP_DIR/private.pem" -pubout -out "$TMP_DIR/public.pem" 2>/dev/null

# Bỏ dòng header/footer PEM và xuống dòng -> một chuỗi base64 duy nhất, nhét vừa một dòng .env
PRIVATE_KEY=$(grep -v -- '-----' "$TMP_DIR/private.pem" | tr -d '\n')
PUBLIC_KEY=$(grep -v -- '-----' "$TMP_DIR/public.pem" | tr -d '\n')

if [[ ! -f "$ENV_FILE" ]]; then
  echo "📄 Chưa có .env — tạo từ .env.example"
  cp .env.example "$ENV_FILE"
fi

# Ghi đè đúng hai dòng khoá, giữ nguyên phần còn lại của .env
write_var() {
  local key="$1" value="$2"
  if grep -q "^${key}=" "$ENV_FILE"; then
    # Dùng | làm phân cách vì base64 có chứa /
    sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
  else
    echo "${key}=${value}" >> "$ENV_FILE"
  fi
}

write_var "JWT_PRIVATE_KEY" "$PRIVATE_KEY"
write_var "JWT_PUBLIC_KEY" "$PUBLIC_KEY"

chmod 600 "$ENV_FILE"

echo "✅ Đã ghi JWT_PRIVATE_KEY và JWT_PUBLIC_KEY vào $ENV_FILE (chmod 600)"
echo
echo "Chạy auth-service với khoá vừa sinh:"
echo "   set -a && source .env && set +a"
echo "   ./mvnw -pl services/auth-service -am spring-boot:run -Dspring-boot.run.profiles=local"
echo
echo "⚠️  Đây là khoá DEV. Môi trường thật phải sinh khoá riêng và giữ trong"
echo "    secret manager (Vault / AWS Secrets Manager), không nằm trong file."
