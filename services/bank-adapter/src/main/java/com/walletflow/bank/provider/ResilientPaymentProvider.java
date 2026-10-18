package com.walletflow.bank.provider;

import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.timelimiter.TimeLimiter;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Bọc MỌI lời gọi ra provider: bulkhead → timeout → circuit breaker (roadmap 2.2).
 *
 * <ul>
 *   <li><b>Bulkhead</b> (thread pool riêng): provider treo chỉ giữ được chừng ấy thread, không kéo thread
 *       của consumer Kafka và job đối chiếu chết theo.</li>
 *   <li><b>Timeout</b>: không chờ vô hạn. Hết giờ là {@link ProviderUnavailableException} — "không biết",
 *       không phải "thất bại": provider có thể vẫn đang tạo phiên ở phía bên kia. Timeout này chỉ thả NGƯỜI
 *       GỌI ra, không ngắt lời gọi đang chạy — xem yêu cầu timeout của HTTP client ở
 *       {@link PaymentProvider}.</li>
 *   <li><b>Circuit breaker</b>: provider lỗi liên tục thì ngừng gọi một lúc, để nó hồi lại và để lệnh của
 *       mình rơi nhanh vào nhánh thử lại thay vì từng cái chờ hết timeout.</li>
 * </ul>
 *
 * <p>Câu trả lời dứt khoát của provider (từ chối, phiên hết hạn) là giá trị trả về bình thường nên
 * KHÔNG tính là lỗi cho breaker — người dùng nhập sai số tiền không được làm mở mạch cho mọi người khác.
 */
public class ResilientPaymentProvider implements PaymentProvider, AutoCloseable {

    private final PaymentProvider delegate;
    private final CircuitBreaker circuitBreaker;
    private final ThreadPoolBulkhead bulkhead;
    private final TimeLimiter timeLimiter;
    private final ScheduledExecutorService timeoutScheduler;

    public ResilientPaymentProvider(PaymentProvider delegate, CircuitBreaker circuitBreaker,
                                    ThreadPoolBulkhead bulkhead, TimeLimiter timeLimiter,
                                    ScheduledExecutorService timeoutScheduler) {
        this.delegate = delegate;
        this.circuitBreaker = circuitBreaker;
        this.bulkhead = bulkhead;
        this.timeLimiter = timeLimiter;
        this.timeoutScheduler = timeoutScheduler;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public ChargeResult charge(ChargeRequest request) {
        return call("charge", () -> delegate.charge(request));
    }

    @Override
    public ProviderStatus getStatus(String providerRef) {
        return call("getStatus", () -> delegate.getStatus(providerRef));
    }

    @Override
    public RefundResult refund(RefundRequest request) {
        return call("refund", () -> delegate.refund(request));
    }

    /** Để quan sát và để test đưa mạch về trạng thái ban đầu. */
    public CircuitBreaker circuitBreaker() {
        return circuitBreaker;
    }

    /** Sở hữu cả thread pool của bulkhead lẫn lịch hẹn giờ timeout — Spring gọi khi đóng context. */
    @Override
    public void close() throws Exception {
        timeoutScheduler.shutdownNow();
        bulkhead.close();
    }

    private <T> T call(String operation, Supplier<T> action) {
        Supplier<CompletionStage<T>> isolated = () -> bulkhead.executeSupplier(action);
        Supplier<CompletionStage<T>> limited = () -> timeLimiter.executeCompletionStage(timeoutScheduler, isolated);
        try {
            return circuitBreaker.executeCompletionStage(limited).toCompletableFuture().join();
        } catch (RuntimeException failure) {
            // Lỗi đi qua future thì bị bọc (có thể nhiều lớp); bulkhead đầy / mạch mở thì ném ngay tại chỗ
            throw translate(operation, unwrap(failure));
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * Mọi thứ không phải câu trả lời của provider đều thành {@link ProviderUnavailableException}: listener
     * Kafka để nó nổ ra (thử lại → DLT), không bao giờ đổi thành "thất bại".
     */
    private ProviderUnavailableException translate(String operation, Throwable cause) {
        if (cause instanceof ProviderUnavailableException unavailable) {
            return unavailable;
        }
        String reason = switch (cause) {
            case CallNotPermittedException ignored -> "circuit breaker đang mở";
            case BulkheadFullException ignored -> "bulkhead đầy";
            case TimeoutException ignored -> "quá thời gian chờ";
            default -> cause.getClass().getSimpleName();
        };
        return new ProviderUnavailableException(
                "Provider " + delegate.name() + " không trả lời được " + operation + ": " + reason, cause);
    }
}
