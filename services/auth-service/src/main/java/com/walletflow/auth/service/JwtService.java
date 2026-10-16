package com.walletflow.auth.service;

import com.walletflow.auth.config.JwtKeyProvider;
import com.walletflow.auth.entity.User;
import io.jsonwebtoken.Jwts;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * Sinh ACCESS TOKEN ký bằng RS256.
 *
 * <p>Chỉ auth-service có private key nên chỉ nó phát hành được token; gateway lấy public key qua
 * JWKS và chỉ verify được — xem {@link JwtKeyProvider}.
 *
 * <p><b>Access token cố tình sống rất ngắn (mặc định 15 phút).</b> Nó không thu hồi được: một khi
 * đã ký thì mọi bên verify đều chấp nhận cho tới lúc hết hạn, vì verify là thao tác offline không
 * hỏi lại auth-service. Thời hạn ngắn chính là cơ chế thu hồi duy nhất. Khả năng duy trì phiên dài
 * được chuyển sang refresh token — thứ CÓ thể thu hồi vì nó được tra trong DB mỗi lần dùng.
 * (Bản trước của dự án dùng token 24 giờ không thu hồi được — mất token là mất ví trọn một ngày.)
 */
@Service
@RequiredArgsConstructor
public class JwtService {

  /** Phải KHỚP với {@code JwtAuthenticationFilter.CLAIM_TENANT} ở gateway. */
  public static final String TENANT_CLAIM = "tid";

  private final JwtKeyProvider keyProvider;

  @Value("${jwt.access-ttl-seconds}")
  private long accessTtlSeconds;

  @Value("${jwt.issuer}")
  private String issuer;

  public String generateAccessToken(User user) {
    Instant now = Instant.now();
    Instant expiry = now.plusSeconds(accessTtlSeconds);

    return Jwts.builder()
        // kid để gateway chọn đúng khoá trong JWKS -> xoay khoá không gây downtime
        .header().keyId(keyProvider.getKeyId()).and()
        // sub = userId chứ không phải email: email đổi được, danh tính thì không.
        .subject(user.getId().toString())
        .claim("email", user.getEmail())
        .claim("role", user.getRole().name())
        // tid = tenant. Gateway TỪ CHỐI token thiếu claim này thay vì đoán tenant mặc định.
        .claim(TENANT_CLAIM, user.getTenantId().toString())
        .issuer(issuer)
        .issuedAt(Date.from(now))
        .expiration(Date.from(expiry))
        .signWith(keyProvider.getPrivateKey(), Jwts.SIG.RS256)
        .compact();
  }

  public Duration accessTokenTtl() {
    return Duration.ofSeconds(accessTtlSeconds);
  }
}
