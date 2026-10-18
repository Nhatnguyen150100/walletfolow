package com.walletflow.bank.services;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.ProviderStatus;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.common.tenant.TenantContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Hỏi lại provider và chốt kết quả của một lần thu đang chờ (ADR-016).
 *
 * <p>Đây là NƠI DUY NHẤT một lần thu được kết luận thành công/thất bại sau khi phiên đã tạo. Job đối chiếu
 * gọi nó định kỳ; webhook (2.3) sẽ gọi nó khi provider báo tin — "webhook là gợi ý, không phải sự thật",
 * nên webhook cũng chỉ kích hoạt việc hỏi lại chứ không tự ghi kết quả.
 *
 * <p>Hai chốt chặn trước khi tin câu trả lời:
 * <ul>
 *   <li><b>Đúng provider:</b> chỉ hỏi provider đã tạo phiên đó. Hỏi provider khác (sau khi đổi
 *       {@code BANK_PROVIDER}, giữa lúc rolling deploy) thì "không tìm thấy" có thể bị hiểu thành thất bại
 *       — người dùng đã trả tiền mà nhận báo thất bại.</li>
 *   <li><b>Đúng số tiền:</b> event thành công mang số tiền mà ledger sẽ cộng, nên số đó phải là số provider
 *       THỰC THU. Lệch số/loại tiền thì không kết luận gì — giữ PENDING, kêu to, chờ người (2.5).</li>
 * </ul>
 */
@Slf4j
@Service
public class ChargeStatusService {

    private static final String DEFAULT_FAILURE_CODE = "PAYMENT_FAILED";

    private final ProviderTransactionRepository repository;
    private final PaymentProvider provider;
    private final BankEventPublisher events;
    private final ProviderResponses responses;
    private final TransactionTemplate transactionTemplate;
    private final Counter amountMismatches;

    public ChargeStatusService(ProviderTransactionRepository repository,
                               PaymentProvider provider,
                               BankEventPublisher events,
                               ProviderResponses responses,
                               PlatformTransactionManager transactionManager,
                               MeterRegistry meterRegistry) {
        this.repository = repository;
        this.provider = provider;
        this.events = events;
        this.responses = responses;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        // Đăng ký lúc khởi động để series tồn tại ở 0 — alert ProviderAmountMismatch dựa vào increase()
        this.amountMismatches = meterRegistry.counter("walletflow.bank.amount_mismatch");
    }

    /**
     * Phải chạy trong {@code TenantContext} của chính lần thu đó.
     *
     * @return trạng thái sau khi hỏi lại
     * @throws com.walletflow.bank.provider.ProviderUnavailableException provider không trả lời — không
     *         kết luận gì, lần sau hỏi lại
     */
    public EProviderTxnStatus refresh(ProviderTransaction txn) {
        if (!txn.getTenantId().equals(TenantContext.require())) {
            throw new IllegalStateException("Lần thu " + txn.getId() + " không thuộc tenant hiện tại");
        }
        if (txn.getStatus() != EProviderTxnStatus.PENDING) {
            return txn.getStatus();
        }
        if (!txn.getProvider().equals(provider.name())) {
            throw new IllegalStateException("Lần thu " + txn.getId() + " thuộc provider " + txn.getProvider()
                    + ", không hỏi " + provider.name() + " về nó");
        }
        // Gọi provider NGOÀI transaction DB
        ProviderStatus status = provider.getStatus(txn.getProviderRef());
        if (status.state() == EProviderState.SUCCEEDED && !capturedAsRequested(txn, status)) {
            amountMismatches.increment();
            log.error("Bank: provider báo đã thu {} {} cho lần thu {} (giao dịch {}) nhưng yêu cầu là {} {} — "
                            + "KHÔNG báo thành công, cần người đối chiếu",
                    status.capturedAmountMinor(), status.capturedCurrency(), txn.getId(),
                    txn.getTransactionId(), txn.getAmountMinor(), txn.getCurrency());
            return EProviderTxnStatus.PENDING;
        }
        return transactionTemplate.execute(tx -> apply(txn, status));
    }

    private static boolean capturedAsRequested(ProviderTransaction txn, ProviderStatus status) {
        return status.capturedAmountMinor() == txn.getAmountMinor()
                && status.capturedCurrency() == txn.getCurrency();
    }

    private EProviderTxnStatus apply(ProviderTransaction txn, ProviderStatus status) {
        int changed = switch (status.state()) {
            case PENDING -> 0;
            case SUCCEEDED -> repository.markSucceeded(txn.getTenantId(), txn.getId(),
                    responses.json(status.rawResponse()));
            case FAILED -> repository.markFailed(txn.getTenantId(), txn.getId(),
                    EProviderTxnStatus.PENDING.name(),
                    ProviderResponses.failureCode(status.failureCode(), DEFAULT_FAILURE_CODE),
                    responses.json(status.rawResponse()));
        };
        ProviderTransaction current = repository.findByTenantIdAndId(txn.getTenantId(), txn.getId())
                .orElseThrow();
        // Chỉ luồng thực sự chuyển trạng thái mới phát event; luồng thua (UPDATE trả 0) im lặng
        if (changed == 1) {
            log.info("Bank: lần thu của giao dịch {} -> {}", current.getTransactionId(), current.getStatus());
            events.publishOutcome(current);
        }
        return current.getStatus();
    }
}
