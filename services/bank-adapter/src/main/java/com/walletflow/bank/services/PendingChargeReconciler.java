package com.walletflow.bank.services;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.common.tenant.TenantContext;
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

/**
 * Định kỳ hỏi lại provider cho các lần thu đang chờ (ADR-016).
 *
 * <p>Không có job này thì một lần thu chỉ kết thúc khi webhook tới — mà webhook có thể không bao giờ tới
 * (provider lỗi, mất mạng, endpoint chết lúc đó). Người dùng đã trả tiền mà ví không được cộng. Job này là
 * lưới an toàn: webhook (2.3) làm kết quả tới nhanh hơn, còn job này đảm bảo nó TỚI.
 *
 * <p>Chạy XUYÊN tenant, mỗi hàng trong {@code TenantContext} của chính nó. Lấy lô bằng
 * {@code SKIP LOCKED} rồi lùi hạn hỏi lại NGAY trong cùng transaction, trước khi gọi provider: nhiều instance
 * không hỏi trùng nhau, và provider chết thì mỗi hàng chỉ bị hỏi lại sau {@code recheck-after}, không
 * dội liên tục.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "walletflow.bank.status-check", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class PendingChargeReconciler {

    private final ProviderTransactionRepository repository;
    private final ChargeStatusService statusService;
    private final PaymentProvider provider;
    private final TransactionTemplate transactionTemplate;
    private final long recheckSeconds;
    private final int batchSize;
    private final Counter failures;

    public PendingChargeReconciler(ProviderTransactionRepository repository,
                                   ChargeStatusService statusService,
                                   PaymentProvider provider,
                                   PlatformTransactionManager transactionManager,
                                   MeterRegistry meterRegistry,
                                   @Value("${walletflow.bank.status-check.recheck-after}") Duration recheckAfter,
                                   @Value("${walletflow.bank.status-check.batch-size}") int batchSize) {
        this.repository = repository;
        this.statusService = statusService;
        this.provider = provider;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.recheckSeconds = recheckAfter.toSeconds();
        this.batchSize = batchSize;
        // Đăng ký lúc khởi động để series tồn tại ở 0 (xem SagaService của transaction-service)
        this.failures = meterRegistry.counter("walletflow.bank.status_check.failures");
    }

    /** @return số lần thu đã hỏi lại trong lượt này */
    @Scheduled(fixedDelayString = "${walletflow.bank.status-check.interval-ms}",
            initialDelayString = "${walletflow.bank.status-check.interval-ms}")
    public int reconcile() {
        List<ProviderTransaction> due = transactionTemplate.execute(status -> {
            List<ProviderTransaction> rows = repository.lockDuePendingAcrossTenants(provider.name(), batchSize);
            if (!rows.isEmpty()) {
                repository.deferChecks(rows.stream().map(ProviderTransaction::getId).toList(), recheckSeconds);
            }
            return rows;
        });

        for (ProviderTransaction row : due) {
            try {
                TenantContext.runWith(row.getTenantId(), () -> statusService.refresh(row));
            } catch (RuntimeException failure) {
                // Một lần thu lỗi không chặn các lần khác; nó đã được lùi hạn nên sẽ được hỏi lại sau
                failures.increment();
                log.warn("Bank: chưa hỏi được trạng thái lần thu {} (giao dịch {}) — thử lại sau: {}",
                        row.getId(), row.getTransactionId(), failure.getMessage());
            }
        }
        return due.size();
    }
}
