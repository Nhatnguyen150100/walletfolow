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
 *   <li>Hợp lệ → gắn {@code X-User-*} và {@code X-Tenant-Id} (từ claim {@code tid}) rồi chuyển
 *       xuống service nội bộ. Token thiếu {@code tid} → 401.</li>
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
  static final String HDR_USER_ID = "X-User-Id";
  static final String HDR_USER_EMAIL = "X-User-Email";
  static final String HDR_USER_ROLE = "X-User-Role";
  static final String HDR_TENANT_ID = "X-Tenant-Id";
  private static final String HDR_REQUEST_ID = "X-Request-Id";

  /** Phải KHỚP với {@code JwtService.TENANT_CLAIM} ở auth-service. */
  static final String CLAIM_TENANT = "tid";

  /**
   * Tenant nằm cùng danh sách với userId, và vì cùng một lý do: service nội bộ tin tuyệt đối
   * header này để chọn sổ của khách hàng nào. Một {@code X-Tenant-Id} do client tự gửi lọt xuống
   * downstream là đọc/ghi được sổ của tenant khác chỉ bằng một dòng curl.
   */
  private static final List<String> IDENTITY_HEADERS =
      List.of(HDR_USER_ID, HDR_USER_EMAIL, HDR_USER_ROLE, HDR_TENANT_ID);

  /** Các tiền tố đường dẫn KHÔNG cần đăng nhập. */
  private static final List<String> PUBLIC_PREFIXES = List.of(
      "/api/auth/**",
      "/.well-known/**",
      "/v3/api-docs/**",
      "/swagger-ui/**",
      "/actuator/**",
      // Webhook/IPN của cổng thanh toán (roadmap 2.3): provider không có JWT của ta. Ranh giới tin cậy
      // là CHỮ KÝ, kiểm ở bank-adapter — không phải đường dẫn này. Rate limit theo IP ở route.
      "/webhooks/**",
      // Trang người dùng quay về sau khi trả tiền (roadmap 2.3b): trình duyệt tới thẳng từ provider nên
      // không mang JWT. Cũng chỉ chữ ký mới được tin, và endpoint chỉ ĐỌC trạng thái.
      "/payments/return/**");

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
    ServerHttpRequest request = exchange.getRequest();
    String path = request.getURI().getPath();

    // --- Bước 0: path đi vòng bị chặn trước MỌI so khớp ---
    if (isTraversal(request.getURI().getRawPath())) {
      log.warn("Từ chối path có dấu hiệu đi vòng: {}", request.getURI().getRawPath());
      return reject(exchange, HttpStatus.BAD_REQUEST, "INVALID_PATH", "Đường dẫn không hợp lệ");
    }

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

      // Token hợp lệ nhưng thiếu tenant -> TỪ CHỐI, không gắn tenant mặc định. Gặp ở hai trường
      // hợp: token phát trước khi có claim (hết hạn sau tối đa 15 phút, client chỉ cần refresh), và
      // một bên phát hành token sai — cả hai đều không được phép đoán ra một tenant.
      UUID tenantId = tenantOf(claims);
      if (tenantId == null) {
        log.warn("JWT hợp lệ nhưng thiếu/sai claim '{}' cho {} — từ chối", CLAIM_TENANT, path);
        return unauthorized(exchange, "Token không mang tenant hợp lệ, vui lòng đăng nhập lại");
      }

      ServerHttpRequest authenticated = sanitized.mutate()
          .header(HDR_USER_ID, claims.getSubject())
          .header(HDR_USER_EMAIL, String.valueOf(claims.get("email")))
          .header(HDR_USER_ROLE, String.valueOf(claims.get("role")))
          .header(HDR_TENANT_ID, tenantId.toString())
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

  private static UUID tenantOf(Claims claims) {
    Object raw = claims.get(CLAIM_TENANT);
    if (raw == null) {
      return null;
    }
    try {
      return UUID.fromString(raw.toString());
    } catch (IllegalArgumentException malformed) {
      return null;
    }
  }

  private static String requestId(ServerHttpRequest request) {
    String existing = request.getHeaders().getFirst(HDR_REQUEST_ID);
    return (existing == null || existing.isBlank()) ? UUID.randomUUID().toString() : existing;
  }

  private boolean isPublic(String path) {
    return PUBLIC_PREFIXES.stream().anyMatch(pattern -> antPathMatcher.match(pattern, path));
  }

  /**
   * Path có thể bị chuẩn hoá thành một path KHÁC ở phía sau gateway.
   *
   * <p>Gateway so khớp route và danh sách path công khai trên chuỗi path như nó nhận được; Tomcat ở
   * service phía sau lại chuẩn hoá {@code ..}, bỏ tham số {@code ;...}, giải mã {@code %2e}. Hai bên
   * hiểu khác nhau là đủ để đi vòng:
   * <ul>
   *   <li>{@code /api/v1/wallets/../../internal/wallets/{id}} khớp route wallet-service nhưng tới
   *       {@code /internal/**} — API nội bộ không có route nào ở gateway;</li>
   *   <li>{@code /api/auth/../v1/...} khớp path CÔNG KHAI {@code /api/auth/**} nên bỏ qua kiểm JWT.</li>
   * </ul>
   * Client hợp lệ không bao giờ cần những ký tự này, nên chặn thẳng thay vì cố chuẩn hoá cho đúng.
   */
  static boolean isTraversal(String rawPath) {
    if (rawPath == null) {
      return false;
    }
    String path = rawPath.toLowerCase(java.util.Locale.ROOT);
    return path.contains("/../") || path.endsWith("/..")
        || path.contains("/./") || path.endsWith("/.")
        || path.contains(";")
        || path.contains("\\")
        || path.contains("%2e") || path.contains("%2f") || path.contains("%5c") || path.contains("%3b");
  }

  private Mono<Void> unauthorized(ServerWebExchange exchange, String message) {
    return reject(exchange, HttpStatus.UNAUTHORIZED, "UNAUTHORIZED", message);
  }

  private Mono<Void> reject(ServerWebExchange exchange, HttpStatus status, String errorCode, String message) {
    exchange.getResponse().setStatusCode(status);
    // RFC 7807, cùng định dạng với GlobalExceptionHandler ở các service — client chỉ cần một bộ
    // xử lý lỗi cho cả gateway lẫn downstream.
    exchange.getResponse().getHeaders().add("Content-Type", "application/problem+json; charset=UTF-8");
    String body = "{\"type\":\"https://walletflow.dev/problems/" + errorCode.toLowerCase().replace('_', '-')
        + "\",\"title\":\"" + status.getReasonPhrase() + "\",\"status\":" + status.value()
        + ",\"errorCode\":\"" + errorCode + "\",\"detail\":\"" + message + "\"}";
    var buffer = exchange.getResponse().bufferFactory()
        .wrap(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return exchange.getResponse().writeWith(Mono.just(buffer));
  }

  @Override
  public int getOrder() {
    return -1; // chạy sớm, trước các filter định tuyến
  }
}
