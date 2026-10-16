package com.walletflow.common.tenant;

import com.walletflow.common.security.AuthHeaders;
import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Biên vào HTTP của tenant. Ba tính chất, mỗi cái chặn một cách rò rỉ:
 * <ol>
 *   <li>Tenant từ header có hiệu lực TRONG request và BIẾN MẤT sau request — thread của container
 *       được tái sử dụng, request kế tiếp không được thừa hưởng.</li>
 *   <li>Thiếu header thì KHÔNG có tenant — không có default ngầm.</li>
 *   <li>Header sai định dạng bị từ chối, request không chạm tới controller.</li>
 * </ol>
 */
class TenantFilterTest {

  private final TenantFilter filter = new TenantFilter();

  @AfterEach
  void clean() {
    TenantContext.clear();
  }

  @Test
  @DisplayName("Tenant từ header có hiệu lực trong request và được dọn sạch sau request")
  void tenant_co_hieu_luc_trong_request_va_bi_don_sau_do() throws Exception {
    UUID tenant = UUID.randomUUID();
    MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/v1/wallets/me");
    request.addHeader(AuthHeaders.TENANT_ID, tenant.toString());

    AtomicReference<UUID> seenInside = new AtomicReference<>();
    AtomicReference<String> mdcInside = new AtomicReference<>();
    FilterChain chain = (req, res) -> {
      seenInside.set(TenantContext.require());
      mdcInside.set(MDC.get(TenantContext.MDC_KEY));
    };

    filter.doFilter(request, new MockHttpServletResponse(), chain);

    assertThat(seenInside.get()).isEqualTo(tenant);
    assertThat(mdcInside.get()).as("mọi dòng log trong request mang tenant").isEqualTo(tenant.toString());
    assertThat(TenantContext.current())
        .as("thread được trả về pool: request kế tiếp không được thừa hưởng tenant này")
        .isEmpty();
    assertThat(MDC.get(TenantContext.MDC_KEY)).isNull();
  }

  @Test
  @DisplayName("Thiếu header thì không có tenant — không đoán tenant mặc định")
  void thieu_header_thi_khong_co_tenant() throws Exception {
    AtomicReference<Optional<UUID>> seenInside = new AtomicReference<>();
    FilterChain chain = (req, res) -> seenInside.set(TenantContext.current());

    filter.doFilter(new MockHttpServletRequest("GET", "/actuator/health"),
        new MockHttpServletResponse(), chain);

    assertThat(seenInside.get())
        .as("path công khai vẫn đi qua được, nhưng chạm vào dữ liệu thì require() sẽ nổ")
        .isEmpty();
  }

  @Test
  @DisplayName("Header không phải UUID: 400, request không tới controller")
  void header_sai_dinh_dang_bi_tu_choi() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/v1/wallets/me");
    request.addHeader(AuthHeaders.TENANT_ID, "tenant-cua-nguoi-khac");
    MockHttpServletResponse response = new MockHttpServletResponse();

    AtomicBoolean reachedController = new AtomicBoolean(false);
    filter.doFilter(request, response, (req, res) -> reachedController.set(true));

    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.getContentAsString()).contains("INVALID_TENANT");
    assertThat(reachedController).isFalse();
  }
}
