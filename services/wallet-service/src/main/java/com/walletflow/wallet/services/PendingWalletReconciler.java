package com.walletflow.wallet.services;

import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.entity.Wallet;
import com.walletflow.wallet.repositories.WalletRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Limit;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Gửi lại lệnh mở tài khoản cho ví kẹt ở {@code PENDING}.
 *
 * <p>Ví kẹt khi lệnh hoặc câu trả lời bị mất ở đâu đó (DLT, sự cố relay, ledger chết lâu). Không có
 * job này thì ví kẹt mãi và người dùng không làm được gì — không lỗi nào được báo ra.
 *
 * <p>Gửi lại là AN TOÀN vì ledger được thiết kế cho đúng việc này (ADR-011): mở tài khoản
 * idempotent, và LUÔN trả lời bằng {@code AccountOpened} kể cả khi tài khoản đã có. Vì vậy không cần
 * khoá phân tán: hai instance cùng gửi lại một ví chỉ tốn thêm một message.
 *
 * <p>Chạy XUYÊN tenant, nên mỗi ví được xử lý trong {@code TenantContext} của chính nó — outbox lấy
 * tenant từ đó để gắn vào envelope.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "walletflow.wallet.pending-resend", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class PendingWalletReconciler {

    /** Mỗi lượt chỉ gửi lại tối đa chừng này ví — ledger chết lâu thì outbox không bị dội. */
    static final int BATCH = 100;

    private final WalletRepository walletRepository;
    private final WalletService walletService;
    private final Duration staleAfter;

    public PendingWalletReconciler(
            WalletRepository walletRepository,
            WalletService walletService,
            @Value("${walletflow.wallet.pending-resend.after:PT2M}") Duration staleAfter) {
        this.walletRepository = walletRepository;
        this.walletService = walletService;
        this.staleAfter = staleAfter;
    }

    /** @return số ví đã gửi lại lệnh */
    @Scheduled(fixedDelayString = "${walletflow.wallet.pending-resend.interval-ms:60000}",
            initialDelayString = "${walletflow.wallet.pending-resend.interval-ms:60000}")
    public int resendStale() {
        List<Wallet> stale = walletRepository.findPendingCreatedBeforeAcrossTenants(
                Instant.now().minus(staleAfter), Limit.of(BATCH));

        for (Wallet wallet : stale) {
            TenantContext.runWith(wallet.getTenantId(), () -> walletService.requestLedgerAccount(wallet));
        }
        if (!stale.isEmpty()) {
            log.warn("Wallet: gửi lại lệnh mở tài khoản cho {} ví kẹt PENDING quá {}", stale.size(), staleAfter);
        }
        return stale.size();
    }
}
