package com.walletflow.bank.services;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.entity.WebhookEvent;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.enums.EWebhookOutcome;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.bank.repositories.WebhookEventRepository;
import com.walletflow.common.tenant.TenantContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;

/**
 * Xử lý MỘT webhook đã nhận (roadmap 2.3b, ADR-018 §2 và §4).
 *
 * <p>Việc của nó gọn trong ba bước: <b>tra tenant từ mã tham chiếu → bảo
 * {@link ChargeStatusService} hỏi lại provider → đóng hàng</b>.
 *
 * <p><b>Nó KHÔNG đọc kết quả trong payload webhook.</b> Chữ ký hợp lệ chỉ chứng minh *provider đã gửi cái
 * này*, không chứng minh *tiền đã về*: một IPN thật, chụp lại và phát lại đúng lúc, vẫn là một IPN thật.
 * Kết quả chỉ được chốt bằng câu trả lời mà adapter tự đi hỏi ({@code getStatus}). Webhook rút ngắn thời
 * gian chờ, không thay thế việc hỏi — xoá bước hỏi đi là biến mọi người gửi được một IPN hợp lệ thành người
 * nạp được tiền vào ví.
 *
 * <p>Tenant lấy từ hàng {@code provider_transactions} tra ra, không bao giờ từ payload (ADR-010, ADR-018 §6).
 */
@Slf4j
@Service
public class WebhookProcessor {

    private final PaymentProvider provider;
    private final ProviderTransactionRepository providerTransactions;
    private final WebhookEventRepository repository;
    private final ChargeStatusService statusService;
    private final TransactionTemplate transactionTemplate;
    private final Counter unresolved;
    private final Counter foreignAccount;

    public WebhookProcessor(PaymentProvider provider,
                            ProviderTransactionRepository providerTransactions,
                            WebhookEventRepository repository,
                            ChargeStatusService statusService,
                            PlatformTransactionManager transactionManager,
                            MeterRegistry meterRegistry) {
        this.provider = provider;
        this.providerTransactions = providerTransactions;
        this.repository = repository;
        this.statusService = statusService;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        // Đăng ký lúc khởi động để series tồn tại ở 0 — alert dựa vào increase()
        this.unresolved = meterRegistry.counter("walletflow.bank.webhook.unresolved");
        this.foreignAccount = meterRegistry.counter("walletflow.bank.webhook.foreign_account");
    }

    /**
     * @throws com.walletflow.bank.provider.ProviderUnavailableException provider không trả lời — hàng KHÔNG
     *         được đóng, worker sẽ thử lại theo lịch backoff
     */
    public void process(WebhookEvent event) {
        Optional<ProviderTransaction> found = Optional.ofNullable(event.getProviderRef())
                .flatMap(ref -> providerTransactions.findChargeByProviderRef(provider.name(), ref));
        if (found.isEmpty()) {
            unresolved.increment();
            // WARN chứ không DEBUG như lúc nhận: tới được đây nghĩa là chữ ký ĐÃ hợp lệ, nên đây không phải
            // người lạ gõ cửa mà là ai đó ký được bằng khoá của ta — môi trường khác dùng chung tài khoản
            // merchant là giải thích thường gặp nhất, và nó đáng để biết.
            log.warn("Bank: webhook {} (ref {}) không tra ra lần thu nào — đóng lại, không xử lý",
                    event.getId(), event.getProviderRef());
            close(event, null, EWebhookOutcome.UNRESOLVED_REF);
            return;
        }

        ProviderTransaction txn = found.get();
        if (!txn.belongsToAccount(provider.account())) {
            // LỖI NGHIỆP VỤ, không phải lỗi kỹ thuật: chỉ tài khoản merchant đã tạo phiên mới tra được phiên
            // đó, nên thử lại bao nhiêu lần cũng thế. Để nó rơi vào vòng retry là bắn nhầm alert
            // BankWebhookAbandoned ("provider không trả lời") cho một ca đổi cấu hình.
            foreignAccount.increment();
            log.warn("Bank: webhook {} (ref {}) thuộc tài khoản merchant {}, đang chạy {} — đóng lại, không hỏi",
                    event.getId(), event.getProviderRef(), txn.getProviderAccount(), provider.account());
            close(event, txn.getTenantId(), EWebhookOutcome.FOREIGN_ACCOUNT);
            return;
        }

        // Gọi provider NGOÀI transaction DB (CLAUDE.md): ChargeStatusService tự mở transaction cho phần ghi
        TenantContext.runWith(txn.getTenantId(), () -> {
            if (txn.getStatus() == EProviderTxnStatus.PENDING) {
                statusService.refresh(txn);
            } else {
                // Hàng đã chốt. Ca đáng tiền ở đây là FAILED(EXPIRED) rồi webhook báo thành công tới muộn:
                // phát hiện và kêu (metric expired_needs_review + alert), KHÔNG lật trạng thái — event failed
                // đã phát, saga đã kết thúc, sửa là việc của người (2.5). Mọi trạng thái chốt khác bị bỏ qua.
                //
                // detectLateMoney chứ KHÔNG confirmExpired: lượt hỏi theo lịch ở T+expired-confirm-after là
                // thứ che độ trễ chốt sổ của ngân hàng, và một câu trả lời "chưa thu" sẽ đóng nó vĩnh viễn.
                // Webhook tới vài phút sau khi hết hạn — lúc provider đương nhiên còn nói "chưa thu" — mà
                // dùng chung đường đó là xoá mất lượt hỏi ở T+1h, và tiền về sau đó không còn ai thấy.
                statusService.detectLateMoney(txn);
            }
        });
        close(event, txn.getTenantId(), EWebhookOutcome.RESOLVED);
    }

    /** Đóng hàng SAU khi đã hỏi xong: đóng trước là tự nhận "đã xử lý" cho một việc có thể chưa xảy ra. */
    private void close(WebhookEvent event, UUID tenantId, EWebhookOutcome outcome) {
        transactionTemplate.executeWithoutResult(tx ->
                repository.markProcessed(event.getId(), tenantId, outcome.name()));
    }
}
