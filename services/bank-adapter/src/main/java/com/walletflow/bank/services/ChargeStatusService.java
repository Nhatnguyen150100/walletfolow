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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;

/**
 * Hỏi lại provider và chốt kết quả của một lần thu đang chờ (ADR-016, ADR-017).
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
 *
 * <p><b>Hết hạn (ADR-017):</b> lần thu kết thúc {@code FAILED(EXPIRED)} chỉ khi CẢ HAI cùng đúng — đã quá
 * hạn phiên + grace (đồng hồ DB), VÀ provider vừa khẳng định chưa thu ({@link EProviderState#PENDING}).
 * Provider không trả lời được, hay trả lời lơ lửng ({@link EProviderState#UNRESOLVED}), thì quá hạn bao lâu
 * cũng không kết luận: "không biết" không bao giờ được biến thành "thất bại" (roadmap §5.7).
 */
@Slf4j
@Service
public class ChargeStatusService {

    private static final String DEFAULT_FAILURE_CODE = "PAYMENT_FAILED";
    static final String EXPIRED_CODE = "EXPIRED";

    private final ProviderTransactionRepository repository;
    private final PaymentProvider provider;
    private final BankEventPublisher events;
    private final ProviderResponses responses;
    private final TransactionTemplate transactionTemplate;
    private final Counter amountMismatches;
    private final Counter expiredNeedsReview;
    private final long expiryGraceSeconds;
    private final long expiredConfirmAfterSeconds;

    public ChargeStatusService(ProviderTransactionRepository repository,
                               PaymentProvider provider,
                               BankEventPublisher events,
                               ProviderResponses responses,
                               PlatformTransactionManager transactionManager,
                               MeterRegistry meterRegistry,
                               @Value("${walletflow.bank.session.expiry-grace}") Duration expiryGrace,
                               @Value("${walletflow.bank.session.expired-confirm-after}") Duration expiredConfirmAfter) {
        if (expiryGrace.isNegative() || expiredConfirmAfter.isNegative()) {
            throw new IllegalStateException("walletflow.bank.session.expiry-grace (" + expiryGrace
                    + ") và expired-confirm-after (" + expiredConfirmAfter + ") không được âm");
        }
        this.repository = repository;
        this.provider = provider;
        this.events = events;
        this.responses = responses;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        // Đăng ký lúc khởi động để series tồn tại ở 0 — alert dựa vào increase()
        this.amountMismatches = meterRegistry.counter("walletflow.bank.amount_mismatch");
        this.expiredNeedsReview = meterRegistry.counter("walletflow.bank.expired_needs_review");
        this.expiryGraceSeconds = expiryGrace.toSeconds();
        this.expiredConfirmAfterSeconds = expiredConfirmAfter.toSeconds();
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
        requireSameOrigin(txn);
        // Gọi provider NGOÀI transaction DB
        ProviderStatus status = provider.getStatus(txn.getProviderRef(), txn.getProviderCreatedAt());
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

    /**
     * Hỏi XÁC NHẬN một lần thu đã FAILED(EXPIRED): tiền có về muộn không (ADR-017). Chỉ phát hiện — trạng thái
     * không bao giờ bị đổi tự động: event {@code failed} đã phát, saga đã kết thúc; sửa là việc của người (2.5)
     * với bằng chứng trong {@code raw_response} của provider.
     *
     * <p>Provider báo đã thu ({@code SUCCEEDED}) hoặc trạng thái lơ lửng ({@code UNRESOLVED}) → metric
     * {@code walletflow.bank.expired_needs_review} + log lỗi (alert page). Provider không trả lời → nổ ra, lần sau
     * hỏi lại (hàng đã được lùi hạn khi lấy lô).
     */
    public void confirmExpired(ProviderTransaction txn) {
        if (!txn.getTenantId().equals(TenantContext.require())) {
            throw new IllegalStateException("Lần thu " + txn.getId() + " không thuộc tenant hiện tại");
        }
        if (txn.getStatus() != EProviderTxnStatus.FAILED || !EXPIRED_CODE.equals(txn.getFailureCode())) {
            return;
        }
        requireSameOrigin(txn);
        ProviderStatus status = provider.getStatus(txn.getProviderRef(), txn.getProviderCreatedAt());
        if (status.state() == EProviderState.SUCCEEDED || status.state() == EProviderState.UNRESOLVED) {
            expiredNeedsReview.increment();
            log.error("Bank: lần thu {} (giao dịch {}) đã báo EXPIRED nhưng provider nay báo {} (đã thu {} {}) — "
                            + "người dùng có thể đã mất tiền mà ví không được cộng, cần người đối chiếu",
                    txn.getId(), txn.getTransactionId(), status.state(), status.capturedAmountMinor(),
                    status.capturedCurrency());
        }
        transactionTemplate.executeWithoutResult(tx -> repository.finishExpiredConfirmation(txn.getTenantId(),
                txn.getId()));
    }

    /**
     * Chỉ hỏi đúng nơi đã tạo phiên — provider khác hay tài khoản merchant khác đều có thể trả "không tìm thấy"
     * cho một phiên đã trả tiền (ADR-016 §6b, ADR-017).
     */
    private void requireSameOrigin(ProviderTransaction txn) {
        if (!txn.getProvider().equals(provider.name()) || !txn.belongsToAccount(provider.account())) {
            throw new IllegalStateException("Lần thu " + txn.getId() + " thuộc provider " + txn.getProvider()
                    + " / tài khoản " + txn.getProviderAccount() + ", không hỏi " + provider.name() + " / tài khoản "
                    + provider.account() + " về nó");
        }
    }

    private static boolean capturedAsRequested(ProviderTransaction txn, ProviderStatus status) {
        return status.capturedAmountMinor() == txn.getAmountMinor()
                && status.capturedCurrency() == txn.getCurrency();
    }

    private EProviderTxnStatus apply(ProviderTransaction txn, ProviderStatus status) {
        int changed = switch (status.state()) {
            // Provider khẳng định chưa thu: kết thúc nếu (và chỉ nếu) đã quá hạn + grace — câu SQL tự kiểm mốc
            case PENDING -> repository.markExpired(txn.getTenantId(), txn.getId(), expiryGraceSeconds,
                    expiredConfirmAfterSeconds, EXPIRED_CODE, responses.json(status.rawResponse()));
            case UNRESOLVED -> {
                log.warn("Bank: provider báo trạng thái lơ lửng cho lần thu {} (giao dịch {}) — giữ PENDING, "
                        + "không tự hết hạn, cần người xem", txn.getId(), txn.getTransactionId());
                yield 0;
            }
            case SUCCEEDED -> repository.markSucceeded(txn.getTenantId(), txn.getId(),
                    responses.json(status.rawResponse()));
            case FAILED -> repository.markFailed(txn.getTenantId(), txn.getId(),
                    EProviderTxnStatus.PENDING.name(),
                    ProviderResponses.failureCode(status.failureCode(), DEFAULT_FAILURE_CODE),
                    responses.json(status.rawResponse()));
        };
        ProviderTransaction current = repository.findByTenantIdAndId(txn.getTenantId(), txn.getId())
                .orElseThrow();
        if (status.state() == EProviderState.SUCCEEDED && current.getStatus() == EProviderTxnStatus.FAILED) {
            // Luồng khác vừa kết luận thất bại (hết hạn) đúng lúc provider báo đã thu: không lật lại, nhưng kêu
            expiredNeedsReview.increment();
            log.error("Bank: provider báo đã thu cho lần thu {} (giao dịch {}) nhưng hàng đã FAILED({}) — "
                    + "cần người đối chiếu", current.getId(), current.getTransactionId(), current.getFailureCode());
        }
        // Chỉ luồng thực sự chuyển trạng thái mới phát event; luồng thua (UPDATE trả 0) im lặng
        if (changed == 1) {
            log.info("Bank: lần thu của giao dịch {} -> {} {}", current.getTransactionId(), current.getStatus(),
                    current.getFailureCode() == null ? "" : current.getFailureCode());
            events.publishOutcome(current);
        }
        return current.getStatus();
    }
}
