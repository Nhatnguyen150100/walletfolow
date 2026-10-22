package com.walletflow.bank;

import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ChargeRequest;
import com.walletflow.bank.provider.ChargeResult;
import com.walletflow.bank.provider.ProviderUnavailableException;
import com.walletflow.bank.provider.RefundRequest;
import com.walletflow.bank.provider.RefundResult;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.support.Await;
import com.walletflow.common.money.ECurrency;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Circuit breaker của {@code ResilientPaymentProvider} với ngưỡng nhỏ (4 lời gọi, ≥ 50% lỗi thì mở) để test
 * chạy trong vài giây. Hai điều cần chứng minh, theo hai chiều ngược nhau:
 * <ul>
 *   <li>Lỗi KỸ THUẬT (lỗi, timeout) đủ nhiều thì mở mạch, và khi mở thì provider KHÔNG bị gọi nữa — người gọi
 *       nhận {@link ProviderUnavailableException} ngay thay vì từng cái chờ hết timeout;</li>
 *   <li>Câu trả lời NGHIỆP VỤ (từ chối, phiên FAILED) không bao giờ mở mạch — một người nhập sai số tiền
 *       không được cắt đường của mọi người khác.</li>
 * </ul>
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "outbox.relay.delay-ms=300",
        "walletflow.bank.provider=mock",
        // Mock cần xác nhận thứ hai (PaymentProviderConfig) — chỉ stack dev/test bật
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        "walletflow.bank.status-check.interval-ms=3600000",
        "walletflow.bank.status-check.recheck-after=PT30S",
        "walletflow.bank.resilience.timeout=500ms",
        "walletflow.bank.resilience.sliding-window-size=4",
        "walletflow.bank.resilience.minimum-calls=4",
        "walletflow.bank.resilience.failure-rate-threshold=50",
        "walletflow.bank.resilience.open-wait=PT30S"
})
@Testcontainers
class ProviderResilienceIT {

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("railsdb");

    @SuppressWarnings("resource")
    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    ResilientPaymentProvider provider;
    @Autowired
    MockProvider mock;
    @Autowired
    ApplicationContext context;

    CircuitBreaker breaker;

    @BeforeEach
    void reset() {
        mock.reset();
        breaker = provider.circuitBreaker();
        breaker.reset();
        // Hai mạch riêng từ 2.4c — đặt lại cả hai, không thì một test làm mở mạch hoàn sẽ kéo test sau đỏ
        provider.refundCircuitBreaker().reset();
    }

    @Test
    @DisplayName("Job @Scheduled chạy trên scheduler 4 thread của Spring Boot — không dùng chung thread hẹn giờ timeout")
    void job_dinh_ky_chay_tren_scheduler_bon_thread() {
        // Bean tên `taskScheduler` là thứ @Scheduled dùng. Một bean ScheduledExecutorService khác trong context
        // làm Spring Boot bỏ qua scheduler của nó (spring.task.scheduling.pool.size mất tác dụng) và @Scheduled
        // rơi vào executor đó — outbox relay và job đối chiếu tranh nhau một thread.
        assertThat(context.containsBean("taskScheduler"))
                .as("Spring Boot phải tạo taskScheduler; không có nghĩa là một bean ScheduledExecutorService đã chặn nó")
                .isTrue();
        assertThat(context.getBean("taskScheduler")).isInstanceOfSatisfying(ThreadPoolTaskScheduler.class,
                scheduler -> assertThat(scheduler.getPoolSize()).isEqualTo(4));
    }

    @Test
    @DisplayName("⭐ Lời gọi provider từ thread của scheduler vẫn timeout đúng hạn (job đối chiếu không treo theo provider)")
    void goi_provider_tu_thread_scheduler_van_timeout_dung_han() throws Exception {
        String providerRef = createSession(1_000);
        mock.setLatency(Duration.ofSeconds(3));   // > resilience.timeout = 500ms

        TaskScheduler scheduler = context.getBean("taskScheduler", TaskScheduler.class);
        CompletableFuture<Throwable> outcome = new CompletableFuture<>();
        long started = System.nanoTime();
        scheduler.schedule(() -> {
            try {
                provider.getStatus(providerRef, null);
                outcome.complete(null);
            } catch (Throwable failure) {
                outcome.complete(failure);
            }
        }, Instant.now());

        Throwable failure = outcome.get(10, TimeUnit.SECONDS);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
        assertThat(failure).isInstanceOf(ProviderUnavailableException.class)
                .hasMessageContaining("quá thời gian chờ");
        assertThat(elapsed)
                .as("timeout 500ms phải thả người gọi ra trước khi provider (3s) trả lời")
                .isLessThan(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("⭐ Lỗi kỹ thuật vượt ngưỡng: mạch MỞ, và khi mở thì mock KHÔNG bị gọi nữa (charge/getStatus bị chặn)")
    void loi_ky_thuat_vuot_nguong_mo_mach_va_khong_goi_provider() {
        String providerRef = createSession(1_000);   // 1 lời gọi thành công
        mock.failNextCalls(3);                        // + 3 lỗi kỹ thuật = 4 lời gọi, 75% lỗi

        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> provider.charge(chargeRequest(1_000)))
                    .isInstanceOf(ProviderUnavailableException.class);
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        int callsWhenOpened = mock.chargeCalls();
        assertThat(callsWhenOpened).isEqualTo(4);

        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> provider.charge(chargeRequest(1_000)))
                    .isInstanceOf(ProviderUnavailableException.class)
                    .hasMessageContaining("circuit breaker đang mở")
                    .hasCauseInstanceOf(CallNotPermittedException.class);
        }
        assertThatThrownBy(() -> provider.getStatus(providerRef, null))
                .isInstanceOf(ProviderUnavailableException.class)
                .hasCauseInstanceOf(CallNotPermittedException.class);

        assertThat(mock.chargeCalls())
                .as("mạch mở: các lời gọi bị chặn ở trước provider, bộ đếm của mock đứng yên")
                .isEqualTo(callsWhenOpened);

        // Đối chứng: mock vẫn trả lời được — lời gọi trên bị từ chối vì MẠCH, không phải vì mock hỏng
        breaker.reset();
        assertThat(provider.getStatus(providerRef, null).state()).isEqualTo(EProviderState.PENDING);
    }

    @Test
    @DisplayName("⭐ Mạch của đường THU mở KHÔNG chặn lệnh hoàn — ở đầu kia của lệnh hoàn là tiền đang ngoài ví người dùng")
    void mach_thu_mo_van_hoan_duoc() {
        String providerRef = createSession(1_000);
        mock.settle(providerRef, EProviderState.SUCCEEDED);   // có tiền thật để mà hoàn
        mock.failNextCalls(3);
        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> provider.charge(chargeRequest(1_000)))
                    .isInstanceOf(ProviderUnavailableException.class);
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(provider.refundCircuitBreaker().getState())
                .as("ngân sách lỗi của hai đường là riêng — lỗi của đường thu không được tiêu vào đường hoàn")
                .isEqualTo(CircuitBreaker.State.CLOSED);

        RefundResult result = provider.refund(
                new RefundRequest(UUID.randomUUID().toString(), providerRef, 1_000, ECurrency.VND));

        assertThat(result)
                .as("lệnh hoàn phải TỚI được provider: dùng chung mạch thì mọi lần hoàn rơi thẳng vào DLT "
                        + "chỉ vì đường thu đang hỏng")
                .isInstanceOf(RefundResult.Refunded.class);
    }

    @Test
    @DisplayName("⭐ Mạch của đường HOÀN mở KHÔNG làm mù getStatus — những lần thu đang chờ vẫn được hỏi lại")
    void mach_hoan_mo_van_hoi_duoc_trang_thai() {
        String providerRef = createSession(1_000);
        mock.failNextCalls(1_000);
        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> provider.refund(
                    new RefundRequest(UUID.randomUUID().toString(), providerRef, 1_000, ECurrency.VND)))
                    .isInstanceOf(ProviderUnavailableException.class);
        }
        assertThat(provider.refundCircuitBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(breaker.getState())
                .as("mạch của đường thu phải còn đóng")
                .isEqualTo(CircuitBreaker.State.CLOSED);

        mock.failNextCalls(0);

        assertThat(provider.getStatus(providerRef, null).state())
                .as("người dùng ĐÃ trả tiền cho lần thu này — không biết nó ra sao là ca mất tiền, không "
                        + "phải một tính năng tạm hỏng")
                .isEqualTo(EProviderState.PENDING);
        assertThatThrownBy(() -> provider.refund(
                new RefundRequest(UUID.randomUUID().toString(), providerRef, 1_000, ECurrency.VND)))
                .as("đối chứng: mạch hoàn vẫn đang mở, nên lệnh hoàn vẫn bị chặn")
                .isInstanceOf(ProviderUnavailableException.class)
                .hasCauseInstanceOf(CallNotPermittedException.class);
    }

    @Test
    @DisplayName("Từ chối nghiệp vụ của lệnh HOÀN không mở mạch hoàn — VNPay từ chối 100% cho tới 2.4c")
    void tu_choi_hoan_khong_mo_mach_hoan() {
        String providerRef = createSession(5_000);   // phiên chưa settle ⇒ mock trả NOT_REFUNDABLE

        for (int i = 0; i < 20; i++) {
            assertThat(provider.refund(
                    new RefundRequest(UUID.randomUUID().toString(), providerRef, 1_000, ECurrency.VND)))
                    .isInstanceOf(RefundResult.Declined.class);
        }

        assertThat(provider.refundCircuitBreaker().getState())
                .as("VNPay trả REFUND_UNSUPPORTED cho MỌI lệnh hoàn tới trước 2.4c (ADR-020 §4). Coi đó là "
                        + "lỗi kỹ thuật thì mạch hoàn mở vĩnh viễn trên production và alert "
                        + "PaymentRefundProviderDown page mãi cho một thứ đúng theo thiết kế.")
                .isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(provider.refundCircuitBreaker().getMetrics().getNumberOfFailedCalls()).isZero();
    }

    @Test
    @DisplayName("Timeout cũng là lỗi kỹ thuật: đủ nhiều lần hết giờ thì mở mạch, rồi chặn không gọi provider")
    void timeout_du_nhieu_cung_mo_mach() {
        mock.setLatency(Duration.ofMillis(1_500));   // > resilience.timeout = 500ms

        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> provider.charge(chargeRequest(1_000)))
                    .isInstanceOf(ProviderUnavailableException.class)
                    .hasMessageContaining("quá thời gian chờ");
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        // Provider vẫn làm nốt 4 lời gọi chậm ở phía nó; chờ xong để không rò sang test sau
        Await.until(() -> mock.sessionCount() == 4, "bốn lời gọi chậm hoàn tất ở phía provider");
        int calls = mock.chargeCalls();

        assertThatThrownBy(() -> provider.charge(chargeRequest(1_000)))
                .isInstanceOf(ProviderUnavailableException.class)
                .hasCauseInstanceOf(CallNotPermittedException.class);
        assertThat(mock.chargeCalls()).isEqualTo(calls);
    }

    @Test
    @DisplayName("Provider hồi lại: mạch nửa mở cho thử, thử thành công thì ĐÓNG lại và gọi được bình thường")
    void provider_hoi_lai_thi_mach_dong_lai() {
        mock.failNextCalls(4);
        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> provider.charge(chargeRequest(1_000)))
                    .isInstanceOf(ProviderUnavailableException.class);
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Không chờ 30s thật: đưa mạch sang nửa mở như khi hết open-wait
        breaker.transitionToHalfOpenState();
        provider.charge(chargeRequest(1_000));
        provider.charge(chargeRequest(1_000));

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(provider.charge(chargeRequest(1_000))).isInstanceOf(ChargeResult.Created.class);
    }

    @Test
    @DisplayName("Từ chối nghiệp vụ (Declined) không bao giờ mở mạch, dù 20 lần liên tiếp")
    void tu_choi_nghiep_vu_khong_mo_mach() {
        mock.setDeclineAboveMinor(1_000);

        for (int i = 0; i < 20; i++) {
            ChargeResult result = provider.charge(chargeRequest(5_000));
            assertThat(result).isInstanceOfSatisfying(ChargeResult.Declined.class,
                    declined -> assertThat(declined.failureCode()).isEqualTo("AMOUNT_LIMIT_EXCEEDED"));
        }

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(breaker.getMetrics().getNumberOfFailedCalls()).isZero();
        mock.setDeclineAboveMinor(0);
        assertThat(provider.charge(chargeRequest(5_000)))
                .as("người khác, số tiền hợp lệ, vẫn đi qua bình thường")
                .isInstanceOf(ChargeResult.Created.class);
    }

    @Test
    @DisplayName("Phiên FAILED (câu trả lời getStatus) không phải lỗi kỹ thuật: không mở mạch")
    void phien_failed_khong_mo_mach() {
        String providerRef = createSession(5_000);
        mock.settle(providerRef, EProviderState.FAILED);

        for (int i = 0; i < 12; i++) {
            assertThat(provider.getStatus(providerRef, null).state()).isEqualTo(EProviderState.FAILED);
        }

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(breaker.getMetrics().getNumberOfFailedCalls()).isZero();
    }

    private String createSession(long amountMinor) {
        ChargeResult result = provider.charge(chargeRequest(amountMinor));
        return ((ChargeResult.Created) result).providerRef();
    }

    private static ChargeRequest chargeRequest(long amountMinor) {
        return new ChargeRequest(UUID.randomUUID().toString(), amountMinor, ECurrency.VND, null,
                Duration.ofMinutes(15));
    }
}
