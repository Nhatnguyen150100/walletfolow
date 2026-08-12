package com.walletflow.gateway.filter;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import javax.crypto.SecretKey;
import java.util.List;

/**
 * Bộ lọc XÁC THỰC TẬP TRUNG (mô hình "Gateway-centric security").
 * <ol>
 *   <li>Route công khai (đăng nhập, swagger, actuator) → cho qua.</li>
 *   <li>Route cần bảo vệ → verify chữ ký JWT NGAY TẠI ĐÂY.</li>
 *   <li>Hợp lệ → bóc thông tin user, gắn vào header X-User-* rồi chuyển xuống service nội bộ
 *       (service nội bộ KHÔNG cần tự verify JWT nữa).</li>
 *   <li>Sai → trả 401 ngay, request không bao giờ chạm tới service.</li>
 * </ol>
 */
@Slf4j
@Component
public class JwtAuthenticationFilter implements GlobalFilter, Ordered {

  @Value("${jwt.secret}")
  private String secret;

  private final AntPathMatcher antPathMatcher = new AntPathMatcher();

  /** Tên header phải KHỚP với AuthHeaders trong common-lib (inline để gateway reactive không kéo JPA). */
  private static final String HDR_USER_ID = "X-User-Id";
  private static final String HDR_USER_EMAIL = "X-User-Email";
  private static final String HDR_USER_ROLE = "X-User-Role";

  /** Các tiền tố đường dẫn KHÔNG cần đăng nhập. */
  private static final List<String> PUBLIC_PREFIXES = List.of(
      "/api/auth/**",
      "/v3/api-docs/**",
      "/swagger-ui/**",
      "/actuator/**");

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
    ServerHttpRequest request = exchange.getRequest();
    String path = request.getURI().getPath();

    if (isPublic(path)) {
      return chain.filter(exchange);
    }

    String authHeader = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
    if (authHeader == null || !authHeader.startsWith("Bearer ")) {
      return unauthorized(exchange, "Thiếu Authorization header");
    }

    String token = authHeader.substring(7);
    try {
      Claims claims = Jwts.parser()
          .verifyWith(signingKey())
          .build()
          .parseSignedClaims(token)
          .getPayload();

      ServerHttpRequest mutated = request.mutate()
          .header(HDR_USER_ID, String.valueOf(claims.get("id")))
          .header(HDR_USER_EMAIL, claims.getSubject())
          .header(HDR_USER_ROLE, String.valueOf(claims.get("role")))
          .build();

      return chain.filter(exchange.mutate().request(mutated).build());
    } catch (Exception e) {
      log.warn("JWT không hợp lệ: {}", e.getMessage());
      return unauthorized(exchange, "Token không hợp lệ hoặc đã hết hạn");
    }
  }

  private boolean isPublic(String path) {
    return PUBLIC_PREFIXES.stream().anyMatch(pattern -> antPathMatcher.match(pattern, path));
  }

  private SecretKey signingKey() {
    return Keys.hmacShaKeyFor(Decoders.BASE64.decode(secret));
  }

  private Mono<Void> unauthorized(ServerWebExchange exchange, String message) {
    exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
    exchange.getResponse().getHeaders().add("Content-Type", "application/json; charset=UTF-8");
    String body = "{\"success\":false,\"status\":401,\"errorCode\":\"UNAUTHORIZED\",\"message\":\"" + message + "\"}";
    var buffer = exchange.getResponse().bufferFactory().wrap(body.getBytes());
    return exchange.getResponse().writeWith(Mono.just(buffer));
  }

  @Override
  public int getOrder() {
    return -1; // chạy sớm, trước các filter định tuyến
  }
}
