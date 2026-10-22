package com.walletflow.bank.config;

import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.provider.vnpay.VnPayProvider;
import com.walletflow.bank.provider.vnpay.VnPaySettings;
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
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
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
            @Value("${walletflow.bank.mock.settle-outcome}") EProviderState settleOutcome,
            // Không có mặc định (CLAUDE.md §6): thiếu khoá thì webhook của mock TẮT, không phải mở bằng
            // một khoá ai cũng đoán được. Chỉ test và môi trường dev tự đặt.
            @Value("${walletflow.bank.mock.webhook-secret:#{null}}") String webhookSecret) {
        if (!allowed || environment.acceptsProfiles(Profiles.of("prod"))) {
            throw new IllegalStateException("walletflow.bank.provider=mock nhưng mock không được cho phép ở "
                    + "đây (cần walletflow.bank.mock.allowed=true và không ở profile 'prod'): MockProvider "
                    + "không thu tiền của ai nhưng saga vẫn sẽ cộng tiền thật vào ví. Từ chối khởi động.");
        }
        return new MockProvider(paymentBaseUrl, Clock.systemUTC(), latency, failureRate, declineAboveMinor,
                autoSettleAfter, settleOutcome, webhookSecret);
    }

    /**
     * VNPay (ADR-017). Mã merchant, hash secret, URL đều BẮT BUỘC từ biến môi trường, không có mặc định:
     * thiếu thì placeholder không giải được, rỗng thì từ chối ở đây — nêu TÊN biến, không bao giờ giá trị.
     *
     * <p>Dưới profile {@code prod} từ chối URL sandbox: tiền "trả" ở sandbox là tiền giả, nhưng saga vẫn cộng
     * tiền thật vào ví — cùng một kiểu tạo tiền từ không khí như mock (ADR-016 §6).
     */
    @Bean
    @ConditionalOnProperty(prefix = "walletflow.bank", name = "provider", havingValue = VnPayProvider.NAME)
    public VnPayProvider vnPayProvider(
            Environment environment,
            @Value("${walletflow.bank.vnpay.tmn-code}") String tmnCode,
            @Value("${walletflow.bank.vnpay.hash-secret}") String hashSecret,
            @Value("${walletflow.bank.vnpay.pay-url}") String payUrl,
            @Value("${walletflow.bank.vnpay.api-url}") String apiUrl,
            @Value("${walletflow.bank.vnpay.return-url}") String returnUrl,
            @Value("${walletflow.bank.vnpay.server-ip}") String serverIp,
            @Value("${walletflow.bank.vnpay.locale}") String locale,
            @Value("${walletflow.bank.vnpay.order-type}") String orderType,
            @Value("${walletflow.bank.vnpay.connect-timeout}") Duration connectTimeout,
            @Value("${walletflow.bank.vnpay.read-timeout}") Duration readTimeout,
            @Value("${walletflow.bank.resilience.timeout}") Duration resilienceTimeout) {
        requireNonBlank("VNPAY_TMN_CODE", tmnCode);
        requireNonBlank("VNPAY_HASH_SECRET", hashSecret);
        requireNonBlank("VNPAY_PAY_URL", payUrl);
        requireNonBlank("VNPAY_API_URL", apiUrl);
        requireNonBlank("VNPAY_RETURN_URL", returnUrl);
        requireNonBlank("VNPAY_SERVER_IP", serverIp);
        if (environment.acceptsProfiles(Profiles.of("prod"))) {
            if (containsIgnoreCase(payUrl, "sandbox") || containsIgnoreCase(apiUrl, "sandbox")) {
                throw new IllegalStateException("VNPay đang trỏ tới SANDBOX dưới profile 'prod': tiền trả ở sandbox "
                        + "là tiền giả nhưng saga vẫn cộng tiền thật vào ví. Từ chối khởi động.");
            }
            // Chữ ký không chống được phát lại một câu trả lời "chưa thu" CŨ nhưng thật của cùng giao dịch — chỉ
            // TLS ngăn kẻ đứng giữa ép một lần thu đã trả thành EXPIRED
            if (!startsWithIgnoreCase(payUrl, "https://") || !startsWithIgnoreCase(apiUrl, "https://")) {
                throw new IllegalStateException("VNPAY_PAY_URL và VNPAY_API_URL phải là https:// dưới profile 'prod'. "
                        + "Từ chối khởi động.");
            }
        }
        // Timeout của chính HTTP client phải ngắn hơn timeout của Resilience4j — không thì lời gọi treo giữ chết
        // thread của bulkhead sau khi người gọi đã được thả ra (xem PaymentProvider)
        if (connectTimeout.compareTo(resilienceTimeout) >= 0 || readTimeout.compareTo(resilienceTimeout) >= 0) {
            throw new IllegalStateException("walletflow.bank.vnpay.connect-timeout (" + connectTimeout + ") và "
                    + "read-timeout (" + readTimeout + ") phải NGẮN hơn walletflow.bank.resilience.timeout ("
                    + resilienceTimeout + ")");
        }

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                // VNPay là HTTP/1.1; không thử nâng cấp h2c trên kết nối thường
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(client);
        requestFactory.setReadTimeout(readTimeout);
        RestClient http = RestClient.builder().requestFactory(requestFactory).build();

        return new VnPayProvider(new VnPaySettings(tmnCode, hashSecret, payUrl, apiUrl, returnUrl, serverIp,
                locale, orderType), http, Clock.systemUTC());
    }

    @Bean
    @Primary
    public ResilientPaymentProvider paymentProvider(
            @Value("${walletflow.bank.provider}") String selected,
            ObjectProvider<MockProvider> mockProvider,
            ObjectProvider<VnPayProvider> vnPayProvider,
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
            case VnPayProvider.NAME -> vnPayProvider.getObject();
            default -> throw new IllegalStateException("walletflow.bank.provider='" + selected
                    + "' không được hỗ trợ. Giá trị hợp lệ: " + MockProvider.NAME + ", " + VnPayProvider.NAME);
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

    private static boolean containsIgnoreCase(String value, String part) {
        return value.toLowerCase(java.util.Locale.ROOT).contains(part);
    }

    private static boolean startsWithIgnoreCase(String value, String prefix) {
        return value.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    private static void requireNonBlank(String envName, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Thiếu " + envName + " cho walletflow.bank.provider=vnpay — "
                    + "không có giá trị mặc định (bất biến §5.6). Từ chối khởi động.");
        }
    }
}
