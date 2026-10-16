package com.walletflow.gateway.filter;

import com.walletflow.gateway.security.JwksKeyLocator;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwts;
import lombok.RequiredArgsConstructor;
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

import java.security.Key;
import java.util.List;
import java.util.UUID;

/**
 * Bộ lọc XÁC THỰC TẬP TRUNG (mô hình "Gateway-centric security").
 *
 * <ol>
 *   <li><b>Gỡ bỏ mọi header định danh do client gửi vào — trên MỌI đường dẫn.</b></li>
 *   <li>Route công khai (đăng nhập, swagger, actuator) → cho qua.</li>
 *   <li>Route cần bảo vệ → verify chữ ký RS256 bằng public key lấy từ JWKS của auth-service.</li>
 *   <li>Hợp lệ → gắn {@code X-User-*} rồi chuyển xuống service nội bộ.</li>
 *   <li>Sai → 401 ngay, request không bao giờ chạm tới service.</li>
 * </ol>
 *
 * <p><b>Vì sao bước 1 phải đứng TRƯỚC bước 2 và không có ngoại lệ:</b> service nội bộ tin tuyệt đối
 * header {@code X-User-Id}. Bản trước của filter này thoát sớm bằng {@code return chain.filter()}
 * cho các path công khai — nghĩa là trên những path đó, một header {@code X-User-Id} do client tự
 * đặt đi thẳng xuống downstream mà không ai đụng vào. Hiện chưa khai thác được vì chưa có endpoint
 * tiền nào nằm trong danh sách công khai, nhưng đó là một quả mìn hẹn giờ: nó sẽ phát nổ vào ngày
 * có người thêm một route công khai mới mà không nhớ tới chi tiết này. Quy tắc đúng là
 * <b>header định danh chỉ được SINH RA tại gateway, không bao giờ được đi vào từ ngoài</b>.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter implements GlobalFilter, Ordered {

  private final JwksKeyLocator jwksKeyLocator;

  @Value("${jwt.issuer:walletflow-auth}")
  private String issuer;

  private final AntPathMatcher antPathMatcher = new AntPathMatcher();

  /** Phải KHỚP với AuthHeaders trong common-lib (inline để gateway reactive không kéo JPA). */
  private static final String HDR_USER_ID = "X-User-Id";
  private static final String HDR_USER_EMAIL = "X-User-Email";
  private static final String HDR_USER_ROLE = "X-User-Role";
  private static final String HDR_REQUEST_ID = "X-Request-Id";

  private static final List<String> IDENTITY_HEADERS =
      List.of(HDR_USER_ID, HDR_USER_EMAIL, HDR_USER_ROLE);

  /** Các tiền tố đường dẫn KHÔNG cần đăng nhập. */
  private static final List<String> PUBLIC_PREFIXES = List.of(
      "/api/auth/**",
      "/.well-known/**",
      "/v3/api-docs/**",
      "/swagger-ui/**",
      "/actuator/**");

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
    ServerHttpRequest request = exchange.getRequest();
    String path = request.getURI().getPath();

    // --- Bước 1: luôn luôn, không ngoại lệ ---
    ServerHttpRequest sanitized = request.mutate()
        .headers(headers -> IDENTITY_HEADERS.forEach(headers::remove))
        // Correlation id: dùng lại nếu client gửi, không thì sinh mới. Mọi log của mọi service
        // trong request này sẽ mang cùng một giá trị.
        .header(HDR_REQUEST_ID, requestId(request))
        .build();

    if (isPublic(path)) {
      return chain.filter(exchange.mutate().request(sanitized).build());
    }

    String authHeader = sanitized.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
    if (authHeader == null || !authHeader.startsWith("Bearer ")) {
      return unauthorized(exchange, "Thiếu Authorization header");
    }

    String token = authHeader.substring(7);
    try {
      Claims claims = Jwts.parser()
          // Chọn khoá theo `kid` trong header token -> xoay khoá được mà không cần deploy lại
          .keyLocator(this::resolveKey)
          .requireIssuer(issuer)
          // Lệch đồng hồ giữa các máy là chuyện thường; 30 giây đủ để không từ chối oan token vừa
          // được phát ra, và đủ ngắn để không kéo dài đáng kể vòng đời token đã hết hạn.
          .clockSkewSeconds(30)
          .build()
          .parseSignedClaims(token)
          .getPayload();

      ServerHttpRequest authenticated = sanitized.mutate()
          .header(HDR_USER_ID, claims.getSubject())
          .header(HDR_USER_EMAIL, String.valueOf(claims.get("email")))
          .header(HDR_USER_ROLE, String.valueOf(claims.get("role")))
          .build();

      return chain.filter(exchange.mutate().request(authenticated).build());
    } catch (Exception e) {
      log.warn("JWT không hợp lệ cho {}: {}", path, e.getMessage());
      return unauthorized(exchange, "Token không hợp lệ hoặc đã hết hạn");
    }
  }

  /**
   * Trả về khoá ứng với {@code kid}. Trả {@code null} khiến jjwt ném lỗi và request thành 401 —
   * đúng hành vi mong muốn khi gặp một kid chưa biết (xem {@link JwksKeyLocator} về xoay khoá).
   */
  private Key resolveKey(Header header) {
    if (header instanceof JwsHeader jwsHeader) {
      return jwksKeyLocator.find(jwsHeader.getKeyId());
    }
    return null;
  }

  private static String requestId(ServerHttpRequest request) {
    String existing = request.getHeaders().getFirst(HDR_REQUEST_ID);
    return (existing == null || existing.isBlank()) ? UUID.randomUUID().toString() : existing;
  }

  private boolean isPublic(String path) {
    return PUBLIC_PREFIXES.stream().anyMatch(pattern -> antPathMatcher.match(pattern, path));
  }

  private Mono<Void> unauthorized(ServerWebExchange exchange, String message) {
    exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
    exchange.getResponse().getHeaders().add("Content-Type", "application/json; charset=UTF-8");
    String body = "{\"success\":false,\"status\":401,\"errorCode\":\"UNAUTHORIZED\",\"message\":\""
        + message + "\"}";
    var buffer = exchange.getResponse().bufferFactory()
        .wrap(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return exchange.getResponse().writeWith(Mono.just(buffer));
  }

  @Override
  public int getOrder() {
    return -1; // chạy sớm, trước các filter định tuyến
  }
}
