package com.walletflow.auth.config;

import io.jsonwebtoken.security.Jwks;
import io.jsonwebtoken.security.RsaPublicJwk;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

/**
 * Nạp cặp khoá RSA dùng để ký JWT, và dựng sẵn bản JWK công khai để phục vụ endpoint JWKS.
 *
 * <p><b>Vì sao bất đối xứng thay cho secret chia sẻ (HS256):</b> với HS256, khoá ký và khoá verify
 * là MỘT. Gateway cần verify nên cũng phải cầm khoá ký — nghĩa là gateway (thành phần duy nhất lộ
 * ra Internet) có khả năng <b>phát hành</b> token với bất kỳ {@code role} nào. Một lỗ hổng đọc file
 * hay đọc biến môi trường ở gateway là chiếm trọn hệ thống. Với RS256, gateway chỉ có public key:
 * chiếm được gateway cũng không giả mạo được token.
 *
 * <p>Lợi ích thứ hai, quan trọng cho vận hành: xoay khoá không cần deploy đồng thời. Gateway lấy
 * public key qua JWKS theo {@code kid}, nên auth-service có thể phát hành khoá mới và gateway tự
 * nhận ra — không có cửa sổ thời gian nào mà hai bên lệch nhau làm mọi request 401.
 *
 * <p><b>Không có giá trị mặc định.</b> Thiếu biến môi trường thì service chết ngay lúc khởi động.
 * Một khoá mặc định "chỉ dùng cho dev" là khoá sẽ theo lên production — đó chính là lỗi mà
 * phiên bản trước của dự án này mắc phải (secret HS256 nằm sẵn trong {@code application.yml} và
 * {@code docker-compose.yml}, ai clone repo cũng ký được token admin).
 */
@Slf4j
@Getter
@Component
public class JwtKeyProvider {

  private final RSAPrivateKey privateKey;
  private final RSAPublicKey publicKey;

  /** Bản công khai của khoá, dạng JWK — chính là thứ trả về ở {@code /.well-known/jwks.json}. */
  private final RsaPublicJwk publicJwk;

  /** {@code kid} đưa vào header mỗi token để gateway biết dùng khoá nào mà verify. */
  private final String keyId;

  public JwtKeyProvider(
      @Value("${jwt.private-key}") String privateKeyBase64,
      @Value("${jwt.public-key}") String publicKeyBase64) {

    this.privateKey = parsePrivateKey(require(privateKeyBase64, "jwt.private-key / JWT_PRIVATE_KEY"));
    this.publicKey = parsePublicKey(require(publicKeyBase64, "jwt.public-key / JWT_PUBLIC_KEY"));

    this.publicJwk = Jwks.builder()
        .key(publicKey)
        .algorithm("RS256")
        .publicKeyUse("sig")
        // kid sinh từ thumbprint (RFC 7638): tất định theo nội dung khoá, nên hai instance
        // auth-service cùng cặp khoá luôn cho ra cùng kid, và kid đổi khi và chỉ khi khoá đổi.
        .idFromThumbprint()
        .build();

    this.keyId = publicJwk.getId();
    log.info("JWT: đã nạp khoá RS256, kid={}", keyId);
  }

  private static String require(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          "Thiếu " + name + ". Sinh khoá bằng ./scripts/gen-keys.sh rồi nạp vào .env. "
              + "Cố tình KHÔNG có giá trị mặc định: khoá mặc định sẽ theo lên production.");
    }
    return value;
  }

  /** Chấp nhận cả chuỗi base64 thuần lẫn PEM đầy đủ — dán nhầm dạng nào cũng chạy. */
  private static byte[] decode(String key) {
    String cleaned = key
        .replaceAll("-----BEGIN [A-Z ]+-----", "")
        .replaceAll("-----END [A-Z ]+-----", "")
        .replaceAll("\\s", "");
    return Base64.getDecoder().decode(cleaned);
  }

  private static RSAPrivateKey parsePrivateKey(String base64) {
    try {
      return (RSAPrivateKey) KeyFactory.getInstance("RSA")
          .generatePrivate(new PKCS8EncodedKeySpec(decode(base64)));
    } catch (Exception e) {
      throw new IllegalStateException(
          "JWT_PRIVATE_KEY không đọc được. Phải là RSA private key dạng PKCS#8 (base64 hoặc PEM).", e);
    }
  }

  private static RSAPublicKey parsePublicKey(String base64) {
    try {
      return (RSAPublicKey) KeyFactory.getInstance("RSA")
          .generatePublic(new X509EncodedKeySpec(decode(base64)));
    } catch (Exception e) {
      throw new IllegalStateException(
          "JWT_PUBLIC_KEY không đọc được. Phải là RSA public key dạng X.509/SPKI (base64 hoặc PEM).", e);
    }
  }
}
