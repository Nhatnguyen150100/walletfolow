package com.walletflow.bank.config;

import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ChargeRequest;
import com.walletflow.bank.provider.ChargeResult;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.ProviderStatus;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.common.money.ECurrency;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

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
                            new ChargeRequest(UUID.randomUUID().toString(), 5_000, ECurrency.VND));

                    assertThat(mock.getStatus(created.providerRef()).state())
                            .as("không có auto-settle thì phiên mới tạo phải còn PENDING")
                            .isEqualTo(EProviderState.PENDING);

                    mock.settle(created.providerRef(), EProviderState.SUCCEEDED);
                    ProviderStatus settled = mock.getStatus(created.providerRef());
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
                            new ChargeRequest(UUID.randomUUID().toString(), 5_000, ECurrency.VND));

                    assertThat(mock.getStatus(created.providerRef()).state())
                            .isEqualTo(EProviderState.SUCCEEDED);
                });
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
