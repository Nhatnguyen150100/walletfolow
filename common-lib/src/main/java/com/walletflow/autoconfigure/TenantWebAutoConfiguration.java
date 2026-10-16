package com.walletflow.autoconfigure;

import com.walletflow.common.tenant.TenantFilter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;

/**
 * Gắn {@link TenantFilter} vào MỌI service servlet dùng common-lib, không cần khai báo ở từng
 * service — giống cách {@code KafkaErrorHandlingAutoConfiguration} bảo vệ mọi listener. Một service
 * mới quên đăng ký filter này sẽ nổ ở {@code TenantContext.require()} trong mọi request, nên cách rẻ
 * nhất để không quên là không phải nhớ.
 *
 * <p>Đặt ngoài package được component-scan (như các auto-configuration khác của common-lib) để
 * không bị đăng ký hai lần ở service có {@code scanBasePackages = "com.walletflow.common"}.
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class TenantWebAutoConfiguration {

  @Bean
  public FilterRegistrationBean<TenantFilter> tenantFilter() {
    FilterRegistrationBean<TenantFilter> registration = new FilterRegistrationBean<>(new TenantFilter());
    // Chạy rất sớm: mọi filter phía sau (log, tracing, security) đều thấy tenant trong MDC.
    registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
    registration.addUrlPatterns("/*");
    return registration;
  }
}
