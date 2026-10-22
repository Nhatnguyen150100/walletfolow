package com.walletflow.bank.provider;

import com.walletflow.bank.config.PaymentProviderConfig;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.support.Await;
import com.walletflow.common.money.ECurrency;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Ngữ nghĩa của lớp bọc mà {@code ChargeIdempotencyIT} dựa vào, kiểm riêng ở mức nhỏ nhất (không DB,
 * không Docker): hết giờ chờ KHÔNG huỷ được việc provider đang làm.
 *
 * <p>Nếu một ngày timeout làm provider (hoặc thread của nó) bị ngắt giữa chừng, ca "timeout rồi thử lại"
 * trong IT vẫn xanh nhưng không còn mô phỏng đúng thế giới thật — nơi provider tạo phiên xong rồi mới trả lời
 * chậm. Test này giữ cho giả định đó tường minh.
 */
class ResilientPaymentProviderTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(context -> context.getBeanFactory()
                    .setConversionService(ApplicationConversionService.getSharedInstance()))
            .withConfiguration(AutoConfigurations.of(PropertyPlaceholderAutoConfiguration.class))
            .withUserConfiguration(PaymentProviderConfig.class)
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withPropertyValues(
                    "walletflow.bank.provider=mock",
                    "walletflow.bank.mock.allowed=true",
                    "walletflow.bank.mock.payment-base-url=http://localhost:8085/mock-pay",
                    "walletflow.bank.mock.latency=0ms",
                    "walletflow.bank.mock.failure-rate=0.0",
                    "walletflow.bank.mock.decline-above-minor=0",
                    "walletflow.bank.mock.settle-outcome=SUCCEEDED",
                    "walletflow.bank.resilience.timeout=300ms",
                    "walletflow.bank.resilience.max-threads=4",
                    "walletflow.bank.resilience.queue-capacity=4",
                    "walletflow.bank.resilience.failure-rate-threshold=50",
                    "walletflow.bank.resilience.sliding-window-size=20",
                    "walletflow.bank.resilience.minimum-calls=10",
                    "walletflow.bank.resilience.open-wait=30s");

    @Test
    @DisplayName("Hết giờ chờ là 'không biết' (ProviderUnavailableException), và provider VẪN tạo phiên sau đó")
    void het_gio_cho_khong_huy_viec_cua_provider() {
        runner.run(context -> {
            MockProvider mock = context.getBean(MockProvider.class);
            ResilientPaymentProvider provider = context.getBean(ResilientPaymentProvider.class);
            mock.setLatency(Duration.ofMillis(900));

            assertThatThrownBy(() -> provider.charge(
                    new ChargeRequest(UUID.randomUUID().toString(), 5_000, ECurrency.VND, null, Duration.ofMinutes(15))))
                    .isInstanceOf(ProviderUnavailableException.class)
                    .hasMessageContaining("quá thời gian chờ");

            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (mock.sessionCount() == 0 && System.nanoTime() < deadline) {
                java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
            }
            assertThat(mock.sessionCount())
                    .as("provider đã nhận lệnh và tạo phiên dù phía ta đã bỏ cuộc — timeout ≠ thất bại")
                    .isEqualTo(1);
        });
    }

    @Test
    @DisplayName("Mạch MỞ tự sang HALF_OPEN khi hết open-wait dù KHÔNG có lời gọi nào (không kẹt ở OPEN sau khi provider đã hồi)")
    void mach_mo_tu_sang_nua_mo_khi_het_open_wait_khong_can_loi_goi() {
        runner.withPropertyValues(
                        "walletflow.bank.resilience.sliding-window-size=4",
                        "walletflow.bank.resilience.minimum-calls=4",
                        "walletflow.bank.resilience.open-wait=1s")
                .run(context -> {
                    MockProvider mock = context.getBean(MockProvider.class);
                    ResilientPaymentProvider provider = context.getBean(ResilientPaymentProvider.class);
                    CircuitBreaker breaker = provider.circuitBreaker();
                    // Ghi lại mọi lần chuyển trạng thái để không phụ thuộc thời điểm đọc getState()
                    List<CircuitBreaker.StateTransition> transitions = new CopyOnWriteArrayList<>();
                    breaker.getEventPublisher().onStateTransition(event -> transitions.add(event.getStateTransition()));

                    mock.failNextCalls(4);
                    for (int i = 0; i < 4; i++) {
                        assertThatThrownBy(() -> provider.charge(
                                new ChargeRequest(UUID.randomUUID().toString(), 5_000, ECurrency.VND, null, Duration.ofMinutes(15))))
                                .isInstanceOf(ProviderUnavailableException.class);
                    }
                    assertThat(transitions).as("đủ lỗi kỹ thuật thì mạch phải mở")
                            .contains(CircuitBreaker.StateTransition.CLOSED_TO_OPEN);

                    // Từ đây KHÔNG gọi provider nữa: chỉ thời gian trôi
                    Await.until(() -> transitions.contains(CircuitBreaker.StateTransition.OPEN_TO_HALF_OPEN),
                            "mạch tự sang HALF_OPEN sau open-wait mà không cần lời gọi nào", Duration.ofSeconds(10));
                    assertThat(breaker.getState()).isIn(CircuitBreaker.State.HALF_OPEN, CircuitBreaker.State.CLOSED);
                    assertThat(mock.chargeCalls()).as("không lời gọi nào được thêm trong lúc chờ").isEqualTo(4);
                });
    }
}
