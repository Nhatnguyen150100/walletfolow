package com.walletflow.auth.service;

import com.walletflow.auth.entity.User;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.util.Date;
import java.util.Map;

/**
 * Sinh JWT. Khoá ký (jwt.secret) PHẢI trùng với khoá verify bên api-gateway, vì gateway mới là nơi
 * kiểm tra chữ ký token cho toàn hệ thống.
 */
@Service
public class JwtService {

  @Value("${jwt.secret}")
  private String secret;

  @Value("${jwt.expiration}")
  private long expiration;

  public String generateToken(User user) {
    Date now = new Date();
    return Jwts.builder()
        .claims(Map.of(
            "id", user.getId().toString(),
            "role", user.getRole().name(),
            "email", user.getEmail()))
        .subject(user.getEmail())
        .issuedAt(now)
        .expiration(new Date(now.getTime() + expiration))
        .signWith(signingKey(), Jwts.SIG.HS256)
        .compact();
  }

  private SecretKey signingKey() {
    return Keys.hmacShaKeyFor(Decoders.BASE64.decode(secret));
  }
}
