package com.walletflow.bank.services;

import com.walletflow.bank.entity.WebhookEvent;
import com.walletflow.bank.enums.EWebhookOutcome;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.repositories.WebhookEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Đọc các webhook đã nhận và xử lý chúng (roadmap 2.3b, ADR-018 §4).
 *
 * <p>2.3a ghi webhook vào {@code webhook_events} rồi trả {@code 200} ngay — cố ý, vì chốt kết quả cần một
 * lời gọi mạng ra provider và làm việc đó trong request của webhook là nguyên nhân kinh điển của "provider
 * retry 5 lần vì bạn trả lời chậm". Lớp này là phần "xử lý sau" của câu đó; thiếu nó thì ta đang nói "đã ghi
 * nhận, đừng gửi lại" với provider cho những sự kiện không ai đọc.
 *
 * <p>Khuôn chạy giống {@link PendingChargeReconciler} và vì cùng lý do: lấy lô bằng {@code SKIP LOCKED} rồi
 * lùi lịch thử NGAY trong cùng transaction, trước khi gọi provider. Nhiều instance không xử lý trùng nhau,
 * và một webhook mà provider không trả lời được không bị thử lại mỗi lượt quét, cũng không chặn những
 * webhook tới sau nó.
 *
 * <p><b>Quan hệ với job đối chiếu:</b> hai luồng cùng hỏi provider về một lần thu là chuyện bình thường và
 * an toàn — mọi chuyển trạng thái đều là UPDATE có điều kiện, chỉ luồng thắng phát event
 * ({@code ChargeStatusService}). Job đối chiếu vẫn là lưới an toàn; worker này chỉ làm kết quả tới nhanh hơn.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "walletflow.bank.webhook", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class WebhookWorker {

    private final WebhookEventRepository repository;
    private final WebhookProcessor processor;
    private final PaymentProvider provider;
    private final TransactionTemplate transactionTemplate;
    private final long retrySeconds;
    private final long maxRetrySeconds;
    private final int batchSize;
    private final int maxAttempts;
    private final Counter failures;
    private final Counter abandoned;

    public WebhookWorker(WebhookEventRepository repository,
                         WebhookProcessor processor,
                         PaymentProvider provider,
                         PlatformTransactionManager transactionManager,
                         MeterRegistry meterRegistry,
                         @Value("${walletflow.bank.webhook.retry-after}") Duration retryAfter,
                         @Value("${walletflow.bank.webhook.max-retry-after}") Duration maxRetryAfter,
                         @Value("${walletflow.bank.webhook.batch-size}") int batchSize,
                         @Value("${walletflow.bank.webhook.max-attempts}") int maxAttempts) {
        if (maxRetryAfter.compareTo(retryAfter) < 0) {
            throw new IllegalStateException("walletflow.bank.webhook.max-retry-after (" + maxRetryAfter
                    + ") phải >= retry-after (" + retryAfter + ")");
        }
        // Trần dưới: 0 nghĩa là bỏ cuộc trước khi thử lần nào. Trần trên: một con số gõ nhầm biến "có trần"
        // thành "vô hạn" mà build vẫn xanh (cùng lý lẽ với max-confirm-attempts ở PendingChargeReconciler).
        if (maxAttempts < 1 || maxAttempts > 50) {
            throw new IllegalStateException("walletflow.bank.webhook.max-attempts (" + maxAttempts
                    + ") phải nằm trong [1, 50]");
        }
        this.repository = repository;
        this.processor = processor;
        this.provider = provider;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.retrySeconds = retryAfter.toSeconds();
        this.maxRetrySeconds = maxRetryAfter.toSeconds();
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        // Đăng ký lúc khởi động để series tồn tại ở 0 — alert dựa vào increase()
        this.failures = meterRegistry.counter("walletflow.bank.webhook.failures");
        this.abandoned = meterRegistry.counter("walletflow.bank.webhook.abandoned");
    }

    /** @return số webhook đã lấy ra xử lý trong lượt này */
    @Scheduled(fixedDelayString = "${walletflow.bank.webhook.interval-ms}",
            initialDelayString = "${walletflow.bank.webhook.interval-ms}")
    public int process() {
        List<WebhookEvent> due = transactionTemplate.execute(status -> {
            List<WebhookEvent> rows = repository.lockDueUnprocessed(provider.name(), batchSize);
            if (!rows.isEmpty()) {
                repository.deferAttempts(ids(rows), retrySeconds, maxRetrySeconds);
            }
            return rows;
        });

        for (WebhookEvent event : due) {
            try {
                processor.process(event);
            } catch (RuntimeException failure) {
                // Một webhook lỗi không chặn các webhook khác; nó đã được lùi lịch nên sẽ được thử lại sau
                failures.increment();
                log.warn("Bank: chưa xử lý được webhook {} (ref {}) — thử lại sau: {}",
                        event.getId(), event.getProviderRef(), failure.getMessage());
            }
        }

        if (!due.isEmpty()) {
            // SAU khi chạy xong cả lô, và chỉ chạm hàng còn chưa xử lý: webhook vừa xong ở chính lượt chót
            // không bị đánh nhầm là bỏ cuộc (ca biên thật — câu trả lời tới ở lượt cuối cùng)
            Integer exhausted = transactionTemplate.execute(status -> repository.abandonExhausted(
                    ids(due), maxAttempts, EWebhookOutcome.ABANDONED.name()));
            if (exhausted != null && exhausted > 0) {
                abandoned.increment(exhausted);
                log.error("Bank: {} webhook hết {} lượt thử mà chưa xử lý được — thôi thử, cần người đối soát",
                        exhausted, maxAttempts);
            }
        }
        return due.size();
    }

    private static List<UUID> ids(List<WebhookEvent> rows) {
        return rows.stream().map(WebhookEvent::getId).toList();
    }
}
