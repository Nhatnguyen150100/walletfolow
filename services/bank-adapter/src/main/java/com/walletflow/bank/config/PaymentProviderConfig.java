package com.walletflow.bank.config;

import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadConfig;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedThreadPoolBulkheadMetrics;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Chọn provider theo {@code walletflow.bank.provider} và bọc nó bằng Resilience4j (ADR-016).
 *
 * <p><b>Không có provider mặc định.</b> Thiếu biến thì không khởi động (placeholder không giải được), giá
 * trị lạ thì không khởi động, và {@code mock} khi chưa được cho phép tường minh thì không khởi động. Các
 * cách hỏng này đều phải nổ lúc khởi động — không được để lộ ra lúc người dùng đầu tiên nạp tiền.
 *
 * <p>Mọi nơi trong service tiêm {@link PaymentProvider} nhận bản đã bọc ({@link Primary}); provider gốc
 * không bao giờ được gọi trực tiếp.
 */
@Configuration
public class PaymentProviderConfig {

    /**
     * Mock cần HAI xác nhận: {@code provider=mock} và {@code mock.allowed=true}, và không bao giờ dưới
     * profile {@code prod}. Một biến gõ nhầm ở môi trường staging/production chưa đủ để chạy mock — chỉ
     * stack dev/E2E/test bật cờ thứ hai. Cờ mặc định là {@code false}: thiếu cấu hình thì phía an toàn thắng.
     */
    @Bean
    @ConditionalOnProperty(prefix = "walletflow.bank", name = "provider", havingValue = MockProvider.NAME)
    public MockProvider mockProvider(
            Environment environment,
            @Value("${walletflow.bank.mock.allowed:false}") boolean allowed,
            @Value("${walletflow.bank.mock.payment-base-url}") String paymentBaseUrl,
            @Value("${walletflow.bank.mock.latency}") Duration latency,
            @Value("${walletflow.bank.mock.failure-rate}") double failureRate,
            @Value("${walletflow.bank.mock.decline-above-minor}") long declineAboveMinor,
            @Value("${walletflow.bank.mock.auto-settle-after:#{null}}") Duration autoSettleAfter,
            @Value("${walletflow.bank.mock.settle-outcome}") EProviderState settleOutcome) {
        if (!allowed || environment.acceptsProfiles(Profiles.of("prod"))) {
            throw new IllegalStateException("walletflow.bank.provider=mock nhưng mock không được cho phép ở "
                    + "đây (cần walletflow.bank.mock.allowed=true và không ở profile 'prod'): MockProvider "
                    + "không thu tiền của ai nhưng saga vẫn sẽ cộng tiền thật vào ví. Từ chối khởi động.");
        }
        return new MockProvider(paymentBaseUrl, Clock.systemUTC(), latency, failureRate, declineAboveMinor,
                autoSettleAfter, settleOutcome);
    }

    @Bean
    @Primary
    public ResilientPaymentProvider paymentProvider(
            @Value("${walletflow.bank.provider}") String selected,
            ObjectProvider<MockProvider> mockProvider,
            MeterRegistry meterRegistry,
            @Value("${walletflow.bank.resilience.timeout}") Duration timeout,
            @Value("${walletflow.bank.resilience.max-threads}") int maxThreads,
            @Value("${walletflow.bank.resilience.queue-capacity}") int queueCapacity,
            @Value("${walletflow.bank.resilience.failure-rate-threshold}") float failureRateThreshold,
            @Value("${walletflow.bank.resilience.sliding-window-size}") int slidingWindowSize,
            @Value("${walletflow.bank.resilience.minimum-calls}") int minimumCalls,
            @Value("${walletflow.bank.resilience.open-wait}") Duration openWait) {

        PaymentProvider raw = switch (selected) {
            case MockProvider.NAME -> mockProvider.getObject();
            default -> throw new IllegalStateException("walletflow.bank.provider='" + selected
                    + "' không được hỗ trợ. Giá trị hợp lệ: " + MockProvider.NAME);
        };

        CircuitBreakerRegistry breakers = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(slidingWindowSize)
                .minimumNumberOfCalls(minimumCalls)
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(openWait)
                // Tự sang HALF_OPEN khi hết open-wait, kể cả không có lời gọi nào: không thì mạch kẹt ở OPEN
                // sau khi provider đã hồi, và alert PaymentProviderDown kêu giả cho tới lời gọi kế tiếp
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .permittedNumberOfCallsInHalfOpenState(2)
                .build());
        CircuitBreaker breaker = breakers.circuitBreaker("provider-" + raw.name());
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(breakers).bindTo(meterRegistry);

        ThreadPoolBulkheadRegistry bulkheads = ThreadPoolBulkheadRegistry.of(ThreadPoolBulkheadConfig.custom()
                .coreThreadPoolSize(maxThreads)
                .maxThreadPoolSize(maxThreads)
                .queueCapacity(queueCapacity)
                .build());
        ThreadPoolBulkhead bulkhead = bulkheads.bulkhead("provider-" + raw.name());
        TaggedThreadPoolBulkheadMetrics.ofThreadPoolBulkheadRegistry(bulkheads).bindTo(meterRegistry);

        // Không bật cancelRunningFuture: trên đường executeCompletionStage nó không ngắt được gì, chỉ gây
        // hiểu nhầm. Lời gọi treo do timeout của HTTP client chặn (xem PaymentProvider).
        TimeLimiter timeLimiter = TimeLimiter.of(TimeLimiterConfig.custom()
                .timeoutDuration(timeout)
                .cancelRunningFuture(false)
                .build());

        // Lịch hẹn giờ cho timeout — một thread là đủ, nó chỉ đánh dấu future quá hạn. CỐ Ý không khai làm
        // @Bean: một bean ScheduledExecutorService làm Spring Boot bỏ qua taskScheduler của nó, và mọi
        // @Scheduled (outbox relay, job đối chiếu) rơi vào đúng thread này — job đang chờ provider thì
        // timeout của chính lời gọi đó xếp hàng sau nó và không bao giờ nổ. ResilientPaymentProvider đóng nó.
        ScheduledExecutorService timeoutScheduler =
                Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("provider-timeout").factory());

        return new ResilientPaymentProvider(raw, breaker, bulkhead, timeLimiter, timeoutScheduler);
    }
}
