package com.walletflow.gateway.filter;

import com.walletflow.gateway.security.JwksKeyLocator;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.walletflow.gateway.filter.JwtAuthenticationFilter.HDR_TENANT_ID;
import static com.walletflow.gateway.filter.JwtAuthenticationFilter.HDR_USER_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Biên bảo mật của gateway: header danh tính — kể cả tenant — chỉ được SINH RA ở đây, không bao
 * giờ được đi vào từ ngoài (roadmap 0.3, 1.0b).
 *
 * <p>Đây là test của một tính chất ÂM: không có cách nào để client chọn tenant hay userId mà
 * downstream nhìn thấy. Lỗi kiểu này không bao giờ lộ ra khi dùng app bình thường — app thật không
 * gửi {@code X-Tenant-Id} — mà chỉ lộ ra khi có người cố tình gửi.
 */
class JwtAuthenticationFilterTest {

  private static final String ISSUER = "walletflow-auth";
  private static final String KID = "test-kid";
  private static final UUID REAL_TENANT = UUID.fromString("0000000a-0000-0000-0000-00000000000a");
  private static final String FORGED_TENANT = "0000000b-0000-0000-0000-00000000000b";

  private static final KeyPair KEY_PAIR = generateKeyPair();

  private JwtAuthenticationFilter filter;

  /** Request mà downstream nhận được — {@code null} nghĩa là gateway đã chặn. */
  private final AtomicReference<ServerHttpRequest> forwarded = new AtomicReference<>();
  private final GatewayFilterChain chain = exchange -> {
    forwarded.set(exchange.getRequest());
    return Mono.empty();
  };

  @BeforeEach
  void setUp() {
    JwksKeyLocator keyLocator = mock(JwksKeyLocator.class);
    when(keyLocator.find(any())).thenReturn(KEY_PAIR.getPublic());
    filter = new JwtAuthenticationFilter(keyLocator);
    ReflectionTestUtils.setField(filter, "issuer", ISSUER);
    forwarded.set(null);
  }

  @Test
  @DisplayName("Path công khai: X-Tenant-Id và X-User-Id do client gửi bị xoá trước khi xuống downstream")
  void path_cong_khai_xoa_header_danh_tinh_cua_client() {
    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.post("/api/auth/login")
            .header(HDR_TENANT_ID, FORGED_TENANT)
            .header(HDR_USER_ID, UUID.randomUUID().toString()));

    filter.filter(exchange, chain).block();

    assertThat(forwarded.get()).as("path công khai vẫn được cho qua").isNotNull();
    assertThat(forwarded.get().getHeaders().get(HDR_TENANT_ID))
        .as("auth-service có TenantFilter: một tenant tự khai lọt xuống là chọn được sổ của người khác")
        .isNull();
    assertThat(forwarded.get().getHeaders().get(HDR_USER_ID)).isNull();
  }

  @Test
  @DisplayName("⭐ /webhooks/** công khai (provider không có JWT của ta), nhưng header danh tính client tự gửi vẫn bị xoá")
  void webhook_cong_khai_nhung_van_xoa_header_danh_tinh() {
    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.get("/webhooks/vnpay?vnp_TxnRef=abc")
            .header(HDR_TENANT_ID, FORGED_TENANT)
            .header(HDR_USER_ID, UUID.randomUUID().toString()));

    filter.filter(exchange, chain).block();

    assertThat(forwarded.get()).as("webhook phải đi qua được, provider không có token").isNotNull();
    assertThat(forwarded.get().getHeaders().get(HDR_TENANT_ID))
        .as("bank-adapter suy tenant từ provider_ref, KHÔNG bao giờ từ header của người gửi webhook")
        .isNull();
    assertThat(forwarded.get().getHeaders().get(HDR_USER_ID)).isNull();
  }

  @Test
  @DisplayName("⭐ Đi vòng qua /webhooks/ để tới path cần đăng nhập: chặn — tiền tố công khai mới không được mở đường vào ví")
  void khong_di_vong_qua_webhooks_de_vao_path_bao_ve() {
    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.get("/webhooks/../api/v1/wallets/me")
            .header(HDR_USER_ID, UUID.randomUUID().toString()));

    filter.filter(exchange, chain).block();

    assertThat(forwarded.get()).as("không được xuống downstream").isNull();
    assertThat(exchange.getResponse().getStatusCode())
        .as("chặn ở bước 0 (path đi vòng) trước mọi so khớp tiền tố, nên là 400 chứ không phải 401")
        .isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  @DisplayName("Path bảo vệ: tenant lấy từ claim tid, giá trị client tự gửi bị ghi đè")
  void tenant_lay_tu_token_khong_lay_tu_client() {
    String userId = UUID.randomUUID().toString();
    String token = token(userId).claim(JwtAuthenticationFilter.CLAIM_TENANT, REAL_TENANT.toString())
        .compact();

    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.get("/api/v1/wallets/me")
            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
            .header(HDR_TENANT_ID, FORGED_TENANT));

    filter.filter(exchange, chain).block();

    assertThat(forwarded.get()).isNotNull();
    assertThat(forwarded.get().getHeaders().get(HDR_TENANT_ID))
        .as("đúng MỘT giá trị, và là giá trị từ token — không phải [forged, real]")
        .containsExactly(REAL_TENANT.toString());
    assertThat(forwarded.get().getHeaders().getFirst(HDR_USER_ID)).isEqualTo(userId);
  }

  @Test
  @DisplayName("Token hợp lệ nhưng thiếu claim tid: 401, không gắn tenant mặc định")
  void token_thieu_tenant_bi_tu_choi() {
    String token = token(UUID.randomUUID().toString()).compact();

    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.get("/api/v1/wallets/me")
            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token));

    filter.filter(exchange, chain).block();

    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(forwarded.get()).as("request không được chạm tới service nội bộ").isNull();
  }

  @Test
  @DisplayName("Claim tid không phải UUID: 401")
  void tenant_sai_dinh_dang_bi_tu_choi() {
    String token = token(UUID.randomUUID().toString())
        .claim(JwtAuthenticationFilter.CLAIM_TENANT, "khong-phai-uuid").compact();

    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.get("/api/v1/wallets/me")
            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token));

    filter.filter(exchange, chain).block();

    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(forwarded.get()).isNull();
  }

  @Test
  @DisplayName("Không có token ở path bảo vệ: 401")
  void khong_co_token_bi_tu_choi() {
    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.get("/api/v1/wallets/me").header(HDR_TENANT_ID, FORGED_TENANT));

    filter.filter(exchange, chain).block();

    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(forwarded.get()).isNull();
  }

  @org.junit.jupiter.params.ParameterizedTest(name = "{0}")
  @org.junit.jupiter.params.provider.ValueSource(strings = {
      "/api/v1/wallets/../../internal/wallets/abc",   // khớp route wallet nhưng tới API nội bộ
      "/api/auth/../v1/transactions/transfer",         // khớp path CÔNG KHAI -> bỏ qua JWT
      "/api/v1/wallets/..;/internal/wallets/abc",      // Tomcat bỏ ;... rồi mới chuẩn hoá
      "/api/v1/wallets/%2e%2e/internal/wallets/abc",   // .. mã hoá
      "/api/v1/wallets/%2E%2E%2Finternal",             // chữ hoa + / mã hoá
      "/api/v1/wallets/./me"
  })
  @DisplayName("Path đi vòng bị chặn 400 TRƯỚC mọi so khớp — kể cả khi có token hợp lệ")
  void chan_path_di_vong(String rawPath) {
    String token = token(UUID.randomUUID().toString())
        .claim(JwtAuthenticationFilter.CLAIM_TENANT, REAL_TENANT.toString()).compact();
    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.method(org.springframework.http.HttpMethod.GET, java.net.URI.create(rawPath))
            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token));

    filter.filter(exchange, chain).block();

    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(forwarded.get()).as("không được chạm tới service phía sau").isNull();
  }

  @Test
  @DisplayName("Path bình thường không bị nhận nhầm là đi vòng")
  void path_binh_thuong_khong_bi_chan() {
    assertThat(JwtAuthenticationFilter.isTraversal("/api/v1/wallets/me/transactions")).isFalse();
    assertThat(JwtAuthenticationFilter.isTraversal("/api/v1/transactions/3f1c2a3b-4d5e-4f60-8a71-b2c3d4e5f601"))
        .isFalse();
    assertThat(JwtAuthenticationFilter.isTraversal("/api/v1/wallets/file.v2")).isFalse();
  }

  private static JwtBuilder token(String userId) {
    Instant now = Instant.now();
    return Jwts.builder()
        .header().keyId(KID).and()
        .subject(userId)
        .claim("email", "a@b.com")
        .claim("role", "USER")
        .issuer(ISSUER)
        .issuedAt(Date.from(now))
        .expiration(Date.from(now.plusSeconds(900)))
        .signWith(KEY_PAIR.getPrivate(), Jwts.SIG.RS256);
  }

  private static KeyPair generateKeyPair() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      return generator.generateKeyPair();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
