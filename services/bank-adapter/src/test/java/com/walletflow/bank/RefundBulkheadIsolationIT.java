package com.walletflow.bank;

import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ChargeRequest;
import com.walletflow.bank.provider.ChargeResult;
import com.walletflow.bank.provider.RefundRequest;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.support.Await;
import com.walletflow.common.money.ECurrency;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Nửa thứ hai của việc cách ly đường hoàn (2.4c, [ADR-020] *Amendments* B2): không chỉ MẠCH riêng mà cả
 * THREAD POOL riêng. {@code ProviderResilienceIT} chứng minh phần mạch; phần pool phải có lớp riêng vì nó
 * cần một khoang đủ nhỏ để làm đầy một cách tất định.
 *
 * <p>Tính chất phải giữ: một đợt lệnh hoàn chậm làm đầy khoang hoàn <b>không</b> được chặn
 * {@code getStatus}. Sau mỗi lần {@code getStatus} không trả lời được là một giao dịch mà người dùng ĐÃ
 * trả tiền còn ta thì không biết nó ra sao — ca mất tiền, không phải một tính năng tạm hỏng.
 *
 * <p>{@code max-threads=1, queue-capacity=1}: hai lệnh hoàn song song là khoang hoàn đầy. Timeout để rộng
 * (10 giây) và độ trễ của mock là 2 giây, nên lời gọi chậm vẫn đi tới đích — thứ đang đo là
 * <b>bulkhead</b>, không phải timeout.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.bank.provider=mock",
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        "walletflow.bank.status-check.interval-ms=3600000",
        "walletflow.bank.resilience.timeout=10s",
        "walletflow.bank.resilience.max-threads=1",
        "walletflow.bank.resilience.queue-capacity=1",
        "walletflow.bank.resilience.sliding-window-size=100",
        "walletflow.bank.resilience.minimum-calls=100",
        "walletflow.bank.resilience.failure-rate-threshold=50",
        "walletflow.bank.resilience.open-wait=PT30S"
})
@Testcontainers
class RefundBulkheadIsolationIT {

    private static final Duration SLOW = Duration.ofSeconds(2);

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
    MeterRegistry meterRegistry;

    @BeforeEach
    void reset() {
        mock.reset();
        provider.circuitBreaker().reset();
        provider.refundCircuitBreaker().reset();
    }

    @Test
    @DisplayName("⭐ Khoang hoàn ĐẦY không chặn getStatus — lần thu người dùng đã trả tiền vẫn được chốt")
    void khoang_hoan_day_van_hoi_duoc_trang_thai() {
        String providerRef = createSession(300_000);
        mock.settle(providerRef, EProviderState.SUCCEEDED);
        mock.setLatency(SLOW);

        // Pool RIÊNG hai thread, không phải common ForkJoinPool: parallelism của FJ pool là
        // availableProcessors() - 1, nên trên runner 2 vCPU hai lệnh hoàn chạy NỐI TIẾP — khoang hoàn
        // không bao giờ đầy và test xanh kể cả khi hai khoang dùng chung pool. Xanh giả còn tệ hơn
        // không có test.
        ExecutorService caller = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<?> busy = CompletableFuture.allOf(
                    CompletableFuture.supplyAsync(() -> provider.refund(refundOf(providerRef, 1)), caller),
                    CompletableFuture.supplyAsync(() -> provider.refund(refundOf(providerRef, 2)), caller));
            // Chờ khoang hoàn THẬT SỰ đầy (1 thread bận + 1 chỗ xếp hàng đã dùng) thay vì ngủ một quãng
            // đoán chừng — đây cũng chính là định nghĩa "đầy" mà phần assert bên dưới dựa vào.
            Await.until(() -> gauge("resilience4j.bulkhead.available.thread.count") == 0
                            && gauge("resilience4j.bulkhead.queue.depth") == 1,
                    "khoang hoàn provider-mock-refund đầy (0 thread rảnh, 1 chờ)");

            EProviderState state = provider.getStatus(providerRef, null).state();

            assertThat(state)
                    .as("dùng chung thread pool thì lời gọi này ăn BulkheadFullException -> "
                            + "ProviderUnavailableException, và job đối chiếu mù trong suốt đợt hoàn tiền")
                    .isEqualTo(EProviderState.SUCCEEDED);
            busy.join();
        } finally {
            caller.shutdown();
        }
    }

    private double gauge(String name) {
        return meterRegistry.get(name).tag("name", "provider-mock-refund").gauge().value();
    }

    private String createSession(long amountMinor) {
        ChargeResult result = provider.charge(new ChargeRequest(UUID.randomUUID().toString(), amountMinor,
                ECurrency.VND, null, Duration.ofMinutes(15)));
        return ((ChargeResult.Created) result).providerRef();
    }

    /** Mỗi lệnh một khoá riêng: mock nhớ câu trả lời theo khoá, trùng khoá thì nó trả lời ngay, không chiếm thread. */
    private static RefundRequest refundOf(String providerRef, int n) {
        return new RefundRequest("refund-" + n + "-" + UUID.randomUUID(), providerRef, 1_000, ECurrency.VND);
    }
}
