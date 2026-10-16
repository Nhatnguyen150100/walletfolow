package com.walletflow.auth.controller;

import com.walletflow.auth.config.JwtKeyProvider;
import io.jsonwebtoken.security.Jwks;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Công bố PUBLIC key dưới dạng JWKS (RFC 7517) để gateway tự lấy mà verify token.
 *
 * <p>Endpoint này cố ý <b>công khai</b>: nó chỉ chứa public key, thứ mà theo thiết kế ai cũng được
 * biết. Giá trị của nó là ở vận hành — gateway không cần được cấu hình khoá thủ công, và khi xoay
 * khoá thì gateway tự nhận ra khoá mới qua {@code kid} mà không cần deploy đồng thời hai service.
 * Với secret HS256 chia sẻ thì mỗi lần đổi khoá là một cửa sổ mà mọi request đều 401.
 */
@RestController
@RequiredArgsConstructor
@Tag(name = "JWKS", description = "Public key để verify JWT")
public class JwksController {

  private final JwtKeyProvider keyProvider;

  @GetMapping(value = "/.well-known/jwks.json", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Tập khoá công khai (JWKS) dùng để verify access token")
  public String jwks() {
    // Dựng JSON trực tiếp từ Jwks.json(): nó serialize đúng chuẩn JWK và đảm bảo không bao giờ
    // lọt thành phần private ra ngoài (Jwks.json chỉ nhận PublicJwk — đây là lý do dùng nó thay
    // vì để Jackson tự serialize một object khoá).
    return "{\"keys\":[" + Jwks.json(keyProvider.getPublicJwk()) + "]}";
  }
}
