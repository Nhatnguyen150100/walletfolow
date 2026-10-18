package com.walletflow.gateway;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Gateway phải KHỞI ĐỘNG được với đúng cấu hình route thật.
 *
 * <p>Test này ra đời vì một lỗi đã nằm từ commit đầu tiên tới roadmap 1.5a: hai bean
 * {@link KeyResolver} không có bean mặc định => {@code RequestRateLimiterGatewayFilterFactory} không
 * tạo được và gateway chết ngay lúc khởi động. Không ai thấy vì gateway chỉ có unit test, còn CI chỉ
 * build image chứ không chạy — E2E là lần đầu có request đi qua gateway thật.
 *
 * <p>Không cần Redis/Eureka/auth thật: rate limiter nối Redis lười (lúc có request đầu tiên), JWKS
 * nạp bất đồng bộ sau khi sẵn sàng, Eureka và config-server tắt hẳn.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "spring.cloud.discovery.enabled=false",
        "jwt.jwks-uri=http://localhost:1/.well-known/jwks.json",
        "management.tracing.enabled=false"
    })
class ApiGatewayApplicationTest {

  @Autowired
  private ApplicationContext context;

  @Autowired
  private RouteLocator routeLocator;

  @Test
  @DisplayName("Gateway khởi động được và nạp đủ route khai trong application.yml")
  void khoi_dong_duoc_va_nap_du_route() {
    assertThat(routeLocator.getRoutes().map(Route::getId).collectList().block())
        .contains("auth-service", "auth-jwks", "wallet-admin", "transaction-service",
            "wallet-service");
  }

  @Test
  @DisplayName("KeyResolver mặc định là theo IP — route quên khai key-resolver vẫn bị giới hạn")
  void key_resolver_mac_dinh_la_theo_ip() {
    assertThat(context.getBean(KeyResolver.class))
        .isSameAs(context.getBean("ipKeyResolver", KeyResolver.class));
  }
}
