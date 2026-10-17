package com.walletflow.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import reactor.core.publisher.Mono;

/**
 * Cấu hình rate limit tại Gateway. KeyResolver quyết định "đếm theo cái gì".
 * - ipKeyResolver: đếm theo IP (chống brute-force đăng nhập).
 * - userKeyResolver: đếm theo user đã xác thực (chống spam giao dịch từ 1 tài khoản).
 */
@Configuration
public class RateLimitConfig {

  /**
   * {@code @Primary}: {@code RequestRateLimiterGatewayFilterFactory} cần MỘT resolver mặc định —
   * thiếu nó, hai bean cùng kiểu làm gateway chết ngay lúc khởi động (lỗi có từ commit đầu, lộ ra ở
   * E2E roadmap 1.5a). Chọn theo IP vì nó luôn có giá trị: route nào có filter
   * {@code RequestRateLimiter} mà quên khai {@code key-resolver} vẫn bị đếm theo từng IP, thay vì gom
   * mọi request chưa đăng nhập vào chung một khoá "anonymous".
   */
  @Bean
  @Primary
  public KeyResolver ipKeyResolver() {
    return exchange -> {
      String ip = exchange.getRequest().getRemoteAddress() != null
          ? exchange.getRequest().getRemoteAddress().getAddress().getHostAddress()
          : "unknown";
      return Mono.just(ip);
    };
  }

  @Bean
  public KeyResolver userKeyResolver() {
    return exchange -> {
      String userId = exchange.getRequest().getHeaders().getFirst("X-User-Id");
      return Mono.just(userId != null ? userId : "anonymous");
    };
  }
}
