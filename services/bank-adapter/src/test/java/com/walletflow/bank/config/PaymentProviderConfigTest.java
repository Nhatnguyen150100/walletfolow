package com.walletflow.bank.config;

import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ChargeRequest;
import com.walletflow.bank.provider.ChargeResult;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.ProviderStatus;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.provider.vnpay.VnPayProvider;
import com.walletflow.common.money.ECurrency;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Các cách cấu hình provider sai đều phải nổ LÚC KHỞI ĐỘNG (ADR-016) — không được để lộ ra khi người dùng
 * đầu tiên nạp tiền: thiếu biến, giá trị lạ, {@code mock} thiếu cờ {@code walletflow.bank.mock.allowed}, và
 * {@code mock} dưới profile {@code prod} dù đã bật cờ (mock "thu" tiền giả nhưng saga vẫn cộng tiền thật vào ví).
 *
 * <p>Chạy bằng {@link ApplicationContextRunner}, không cần Docker. Runner được dựng giống Spring Boot thật ở
 * hai điểm mà kết quả test phụ thuộc: placeholder bị coi là LỖI khi không giải được (mặc định của context
 * trần là để nguyên chuỗi {@code ${...}}), và {@code @Value} đổi được chuỗi sang {@code Duration}.
 */
class PaymentProviderConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(context -> {
                // Giống SpringApplication: bean factory dùng ConversionService của Boot (Duration, enum...)
                context.getBeanFactory().setConversionService(ApplicationConversionService.getSharedInstance());
                ConfigurationPropertySources.attach(context.getEnvironment());
            })
            .withConfiguration(AutoConfigurations.of(PropertyPlaceholderAutoConfiguration.class))
            .withUserConfiguration(PaymentProviderConfig.class)
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withPropertyValues(
                    "walletflow.bank.mock.payment-base-url=http://localhost:8085/mock-pay",
                    "walletflow.bank.mock.latency=0ms",
                    "walletflow.bank.mock.failure-rate=0.0",
                    "walletflow.bank.mock.decline-above-minor=0",
                    "walletflow.bank.mock.settle-outcome=SUCCEEDED",
                    "walletflow.bank.resilience.timeout=5s",
                    "walletflow.bank.resilience.max-threads=2",
                    "walletflow.bank.resilience.queue-capacity=2",
                    "walletflow.bank.resilience.failure-rate-threshold=50",
                    "walletflow.bank.resilience.sliding-window-size=10",
                    "walletflow.bank.resilience.minimum-calls=5",
                    "walletflow.bank.resilience.open-wait=30s");

    @Test
    @DisplayName("mock đã bật cờ allowed=true nhưng dưới profile prod: vẫn KHÔNG khởi động, và nói rõ vì sao")
    void mock_duoi_profile_prod_khong_khoi_dong_ke_ca_khi_bat_co() {
        mockAllowedRunner().withPropertyValues("walletflow.bank.provider=mock", "spring.profiles.active=prod")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(allMessages(context.getStartupFailure()))
                            .contains("mock")
                            .contains("prod");
                });
    }

    @Test
    @DisplayName("provider=mock mà THIẾU cờ walletflow.bank.mock.allowed: KHÔNG khởi động (một biến gõ nhầm chưa đủ để chạy mock)")
    void mock_thieu_co_allowed_khong_khoi_dong() {
        runner.withPropertyValues("walletflow.bank.provider=mock", "spring.profiles.active=local")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(allMessages(context.getStartupFailure()))
                            .contains("mock.allowed=true");
                });
    }

    @Test
    @DisplayName("provider=mock với allowed=false tường minh: KHÔNG khởi động")
    void mock_voi_co_allowed_false_khong_khoi_dong() {
        runner.withPropertyValues("walletflow.bank.provider=mock", "walletflow.bank.mock.allowed=false")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(allMessages(context.getStartupFailure()))
                            .contains("mock.allowed=true");
                });
    }

    @Test
    @DisplayName("Giá trị provider lạ: KHÔNG khởi động, không âm thầm rơi về mock")
    void gia_tri_provider_la_khong_khoi_dong() {
        runner.withPropertyValues("walletflow.bank.provider=vnpay-nhung-chua-co")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(allMessages(context.getStartupFailure()))
                            .contains("vnpay-nhung-chua-co")
                            .contains("không được hỗ trợ");
                });
    }

    @Test
    @DisplayName("Thiếu walletflow.bank.provider: KHÔNG khởi động (không có mặc định)")
    void thieu_provider_khong_khoi_dong() {
        runner.run(context -> {
            assertThat(context).hasFailed();
            assertThat(allMessages(context.getStartupFailure()))
                    .contains("walletflow.bank.provider");
        });
    }

    @Test
    @DisplayName("mock ngoài prod: khởi động, và PaymentProvider được tiêm là bản đã bọc resilience tên 'mock'")
    void mock_ngoai_prod_khoi_dong_va_duoc_boc_resilient() {
        mockAllowedRunner().withPropertyValues("walletflow.bank.provider=mock", "spring.profiles.active=local")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // Có hai PaymentProvider (mock gốc + bản bọc): bản bọc phải là @Primary, không ai được
                    // gọi mock gốc mà né breaker/timeout/bulkhead.
                    PaymentProvider injected = context.getBean(PaymentProvider.class);
                    assertThat(injected).isInstanceOf(ResilientPaymentProvider.class);
                    assertThat(injected.name()).isEqualTo("mock");
                    assertThat(context).hasSingleBean(ResilientPaymentProvider.class);
                    assertThat(context).hasSingleBean(MockProvider.class);
                });
    }

    @Test
    @DisplayName("mock.auto-settle-after để rỗng nghĩa là null: phiên chỉ đổi trạng thái khi test gọi settle")
    void auto_settle_after_rong_nghia_la_khong_tu_settle() {
        mockAllowedRunner().withPropertyValues("walletflow.bank.provider=mock", "walletflow.bank.mock.auto-settle-after=")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    MockProvider mock = context.getBean(MockProvider.class);
                    ChargeResult.Created created = (ChargeResult.Created) mock.charge(
                            new ChargeRequest(UUID.randomUUID().toString(), 5_000, ECurrency.VND, null, Duration.ofMinutes(15)));

                    assertThat(mock.getStatus(created.providerRef(), created.sessionCreatedAt()).state())
                            .as("không có auto-settle thì phiên mới tạo phải còn PENDING")
                            .isEqualTo(EProviderState.PENDING);

                    mock.settle(created.providerRef(), EProviderState.SUCCEEDED);
                    ProviderStatus settled = mock.getStatus(created.providerRef(), created.sessionCreatedAt());
                    assertThat(settled.state()).isEqualTo(EProviderState.SUCCEEDED);
                });
    }

    @Test
    @DisplayName("Đối chứng: có auto-settle-after=PT0S thì phiên tự SUCCEEDED — chứng tỏ test trên phân biệt được hai ca")
    void auto_settle_after_co_gia_tri_thi_phien_tu_settle() {
        mockAllowedRunner().withPropertyValues("walletflow.bank.provider=mock", "walletflow.bank.mock.auto-settle-after=PT0S")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    MockProvider mock = context.getBean(MockProvider.class);
                    ChargeResult.Created created = (ChargeResult.Created) mock.charge(
                            new ChargeRequest(UUID.randomUUID().toString(), 5_000, ECurrency.VND, null, Duration.ofMinutes(15)));

                    assertThat(mock.getStatus(created.providerRef(), created.sessionCreatedAt()).state())
                            .isEqualTo(EProviderState.SUCCEEDED);
                });
    }

    // ------------------------------------------------------------------
    // VNPay (ADR-017): không có mặc định cho mã merchant/secret/URL, không sandbox ở prod, timeout client < resilience
    // ------------------------------------------------------------------
    @Test
    @DisplayName("provider=vnpay mà THIẾU mọi cấu hình VNPay: KHÔNG khởi động, thông báo nêu walletflow.bank.vnpay (không có mặc định)")
    void vnpay_thieu_cau_hinh_khong_khoi_dong() {
        runner.withPropertyValues("walletflow.bank.provider=vnpay").run(context -> {
            assertThat(context).hasFailed();
            assertThat(allMessages(context.getStartupFailure())).contains("walletflow.bank.vnpay");
        });
    }

    @ParameterizedTest(name = "thiếu {0}")
    @ValueSource(strings = {"tmn-code", "hash-secret", "pay-url", "api-url", "return-url", "server-ip"})
    @DisplayName("Mỗi biến VNPay bắt buộc: thiếu hẳn thì KHÔNG khởi động")
    void vnpay_thieu_tung_bien_khong_khoi_dong(String property) {
        ApplicationContextRunner partial = runner.withPropertyValues("walletflow.bank.provider=vnpay");
        for (String other : new String[]{"tmn-code", "hash-secret", "pay-url", "api-url", "return-url", "server-ip"}) {
            if (!other.equals(property)) {
                partial = partial.withPropertyValues("walletflow.bank.vnpay." + other + "=" + validValue(other));
            }
        }
        partial.withPropertyValues(vnpayCommonProperties()).run(context -> {
            assertThat(context).hasFailed();
            assertThat(allMessages(context.getStartupFailure()))
                    .contains("walletflow.bank.vnpay." + property)
                    .doesNotContain(SECRET);
        });
    }

    @ParameterizedTest(name = "{0} rỗng -> {1}")
    @CsvSource({
            "tmn-code, VNPAY_TMN_CODE",
            "hash-secret, VNPAY_HASH_SECRET",
            "pay-url, VNPAY_PAY_URL",
            "api-url, VNPAY_API_URL",
            "return-url, VNPAY_RETURN_URL",
            "server-ip, VNPAY_SERVER_IP"
    })
    @DisplayName("Biến VNPay có mặt nhưng RỖNG: KHÔNG khởi động, nêu TÊN biến môi trường và không bao giờ lộ giá trị của biến khác")
    void vnpay_bien_rong_khong_khoi_dong_va_khong_lo_gia_tri(String property, String envName) {
        vnpayRunner().withPropertyValues("walletflow.bank.vnpay." + property + "=").run(context -> {
            assertThat(context).hasFailed();
            String messages = allMessages(context.getStartupFailure());
            assertThat(messages).contains(envName).doesNotContain(SECRET);
        });
    }

    @Test
    @DisplayName("Biến VNPay chỉ toàn khoảng trắng cũng bị coi là rỗng")
    void vnpay_bien_toan_khoang_trang_bi_tu_choi() {
        vnpayRunner().withPropertyValues("walletflow.bank.vnpay.hash-secret=   ").run(context -> {
            assertThat(context).hasFailed();
            assertThat(allMessages(context.getStartupFailure())).contains("VNPAY_HASH_SECRET");
        });
    }

    @ParameterizedTest(name = "{0} = sandbox")
    @ValueSource(strings = {"pay-url", "api-url"})
    @DisplayName("⭐ URL sandbox dưới profile prod: KHÔNG khởi động (tiền sandbox là tiền giả nhưng saga cộng tiền thật vào ví)")
    void vnpay_sandbox_duoi_prod_khong_khoi_dong(String sandboxProperty) {
        vnpayRunner()
                .withPropertyValues("spring.profiles.active=prod",
                        "walletflow.bank.vnpay.pay-url=https://pay.vnpayment.vn/paymentv2/vpcpay.html",
                        "walletflow.bank.vnpay.api-url=https://pay.vnpayment.vn/merchant_webapi/api/transaction",
                        "walletflow.bank.vnpay." + sandboxProperty + "=https://sandbox.vnpayment.vn/x")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(allMessages(context.getStartupFailure()))
                            .contains("SANDBOX")
                            .contains("prod")
                            .doesNotContain(SECRET);
                });
    }

    @Test
    @DisplayName("Đối chứng: URL thật dưới prod thì khởi động; URL sandbox ngoài prod (dev/staging) cũng khởi động")
    void vnpay_url_that_duoi_prod_va_sandbox_ngoai_prod_deu_khoi_dong() {
        vnpayRunner()
                .withPropertyValues("spring.profiles.active=prod",
                        "walletflow.bank.vnpay.pay-url=https://pay.vnpayment.vn/paymentv2/vpcpay.html",
                        "walletflow.bank.vnpay.api-url=https://pay.vnpayment.vn/merchant_webapi/api/transaction")
                .run(context -> assertThat(context).hasNotFailed());
        vnpayRunner().withPropertyValues("spring.profiles.active=local")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @ParameterizedTest(name = "{0} = {1} (resilience.timeout = 5s)")
    @CsvSource({
            "read-timeout, 5s",
            "read-timeout, 6s",
            "connect-timeout, 5s",
            "connect-timeout, 10s"
    })
    @DisplayName("⭐ connect/read-timeout của HTTP client >= resilience.timeout: KHÔNG khởi động (lời gọi treo sẽ giữ chết thread của bulkhead)")
    void vnpay_timeout_cua_client_khong_ngan_hon_resilience_khong_khoi_dong(String property, String value) {
        vnpayRunner().withPropertyValues("walletflow.bank.vnpay." + property + "=" + value).run(context -> {
            assertThat(context).hasFailed();
            assertThat(allMessages(context.getStartupFailure()))
                    .contains("connect-timeout")
                    .contains("read-timeout")
                    .contains("resilience.timeout");
        });
    }

    @Test
    @DisplayName("Timeout của client ngắn hơn resilience.timeout dù chỉ 1ms: khởi động")
    void vnpay_timeout_ngan_hon_resilience_dung_1ms_van_khoi_dong() {
        vnpayRunner().withPropertyValues("walletflow.bank.vnpay.read-timeout=4999ms",
                        "walletflow.bank.vnpay.connect-timeout=1ms")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    @DisplayName("vnpay đủ cấu hình: khởi động, PaymentProvider được tiêm là bản bọc resilience tên 'vnpay', không có MockProvider")
    void vnpay_day_du_khoi_dong_va_duoc_boc_resilient() {
        vnpayRunner().run(context -> {
            assertThat(context).hasNotFailed();
            PaymentProvider injected = context.getBean(PaymentProvider.class);
            assertThat(injected).isInstanceOf(ResilientPaymentProvider.class);
            assertThat(injected.name()).isEqualTo("vnpay");
            assertThat(context).hasSingleBean(ResilientPaymentProvider.class);
            assertThat(context).hasSingleBean(VnPayProvider.class);
            assertThat(context).doesNotHaveBean(MockProvider.class);
        });
    }

    @Test
    @DisplayName("provider=vnpay không đòi cờ mock.allowed, và provider=mock không đòi cấu hình VNPay (hai nhánh độc lập)")
    void hai_nhanh_provider_khong_dong_vao_nhau() {
        vnpayRunner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(MockProvider.class);
        });
        mockAllowedRunner().withPropertyValues("walletflow.bank.provider=mock").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(VnPayProvider.class);
        });
    }

    @ParameterizedTest(name = "{0} = {1}")
    @CsvSource({
            "pay-url, https://SANDBOX.vnpayment.vn/paymentv2/vpcpay.html",
            "api-url, https://Sandbox.vnpayment.vn/merchant_webapi/api/transaction",
            "pay-url, https://pay.vnpayment.vn/paymentv2/vpcpay.html?env=SANDBOX"
    })
    @DisplayName("⭐ 'sandbox' viết hoa/thường lẫn lộn trong URL https dưới prod: vẫn bị coi là SANDBOX, KHÔNG khởi động")
    void vnpay_sandbox_viet_hoa_duoi_prod_khong_khoi_dong(String property, String url) {
        vnpayRunner()
                .withPropertyValues("spring.profiles.active=prod",
                        "walletflow.bank.vnpay.pay-url=https://pay.vnpayment.vn/paymentv2/vpcpay.html",
                        "walletflow.bank.vnpay.api-url=https://pay.vnpayment.vn/merchant_webapi/api/transaction",
                        "walletflow.bank.vnpay." + property + "=" + url)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(allMessages(context.getStartupFailure())).contains("SANDBOX").doesNotContain(SECRET);
                });
    }

    @ParameterizedTest(name = "{0} = {1}")
    @CsvSource({
            "pay-url, http://pay.vnpayment.vn/paymentv2/vpcpay.html",
            "api-url, http://pay.vnpayment.vn/merchant_webapi/api/transaction"
    })
    @DisplayName("⭐ URL http:// (không sandbox) dưới prod: KHÔNG khởi động, nêu https — chỉ TLS ngăn kẻ đứng giữa phát lại câu trả lời 'chưa thu' cũ")
    void vnpay_http_duoi_prod_khong_khoi_dong(String property, String url) {
        vnpayRunner()
                .withPropertyValues("spring.profiles.active=prod",
                        "walletflow.bank.vnpay.pay-url=https://pay.vnpayment.vn/paymentv2/vpcpay.html",
                        "walletflow.bank.vnpay.api-url=https://pay.vnpayment.vn/merchant_webapi/api/transaction",
                        "walletflow.bank.vnpay." + property + "=" + url)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(allMessages(context.getStartupFailure()))
                            .contains("https")
                            .doesNotContain("SANDBOX")
                            .doesNotContain(SECRET);
                });
    }

    @Test
    @DisplayName("Đối chứng: https không sandbox dưới prod khởi động; http:// ngoài prod (dev) cũng khởi động")
    void vnpay_https_that_duoi_prod_khoi_dong_va_http_ngoai_prod_khoi_dong() {
        vnpayRunner()
                .withPropertyValues("spring.profiles.active=prod",
                        "walletflow.bank.vnpay.pay-url=https://pay.vnpayment.vn/paymentv2/vpcpay.html",
                        "walletflow.bank.vnpay.api-url=https://pay.vnpayment.vn/merchant_webapi/api/transaction")
                .run(context -> assertThat(context).hasNotFailed());
        vnpayRunner()
                .withPropertyValues("spring.profiles.active=local",
                        "walletflow.bank.vnpay.pay-url=http://127.0.0.1:9/pay",
                        "walletflow.bank.vnpay.api-url=http://127.0.0.1:9/api")
                .run(context -> assertThat(context).hasNotFailed());
    }

    private static final String SECRET = "unit-test-fake-secret-do-not-leak-9f3a";

    /** Cấu hình VNPay đầy đủ, hợp lệ (URL sandbox: hợp lệ ngoài prod); ca nào cần thì ghi đè một giá trị. */
    private ApplicationContextRunner vnpayRunner() {
        return runner.withPropertyValues("walletflow.bank.provider=vnpay",
                        "walletflow.bank.vnpay.tmn-code=" + validValue("tmn-code"),
                        "walletflow.bank.vnpay.hash-secret=" + validValue("hash-secret"),
                        "walletflow.bank.vnpay.pay-url=" + validValue("pay-url"),
                        "walletflow.bank.vnpay.api-url=" + validValue("api-url"),
                        "walletflow.bank.vnpay.return-url=" + validValue("return-url"),
                        "walletflow.bank.vnpay.server-ip=" + validValue("server-ip"))
                .withPropertyValues(vnpayCommonProperties());
    }

    private static String[] vnpayCommonProperties() {
        return new String[]{
                "walletflow.bank.vnpay.locale=vn",
                "walletflow.bank.vnpay.order-type=other",
                "walletflow.bank.vnpay.connect-timeout=2s",
                "walletflow.bank.vnpay.read-timeout=3s"};
    }

    private static String validValue(String property) {
        return switch (property) {
            case "tmn-code" -> "WFTEST01";
            case "hash-secret" -> SECRET;
            case "pay-url" -> "https://sandbox.vnpayment.vn/paymentv2/vpcpay.html";
            case "api-url" -> "https://sandbox.vnpayment.vn/merchant_webapi/api/transaction";
            case "return-url" -> "http://localhost/return";
            case "server-ip" -> "10.0.0.1";
            default -> throw new IllegalArgumentException(property);
        };
    }

    /** Runner nền KHÔNG có cờ allowed (như môi trường thật); ca cần mock chạy được thì bật cờ ở đây. */
    private ApplicationContextRunner mockAllowedRunner() {
        return runner.withPropertyValues("walletflow.bank.mock.allowed=true");
    }

    private static String allMessages(Throwable failure) {
        Throwable root = failure;
        StringBuilder all = new StringBuilder();
        while (root != null) {
            all.append(root.getMessage()).append(" | ");
            root = root.getCause();
        }
        // Gộp cả chuỗi nguyên nhân: Spring bọc lỗi nhiều tầng, thông điệp có ích nằm ở tầng nào cũng được
        return all.toString();
    }
}
