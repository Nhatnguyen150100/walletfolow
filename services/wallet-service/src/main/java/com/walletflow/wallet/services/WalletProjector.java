package com.walletflow.wallet.services;

import com.walletflow.common.event.AccountOpenedEvent;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.Leg;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.entity.Wallet;
import com.walletflow.wallet.enums.EWalletStatus;
import com.walletflow.wallet.repositories.WalletEntryRepository;
import com.walletflow.wallet.repositories.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Dựng read model của ví từ event của sổ cái (roadmap 1.1).
 *
 * <p><b>Projection phải GIAO HOÁN.</b> {@code LedgerPosted} được partition theo ví bị ghi nợ, nên
 * với lệnh chuyển A→B, vế của B đi trên partition của A — không có thứ tự nào so với các event
 * khác của B. Vì vậy ở đây chỉ CỘNG DELTA (thứ tự không quan trọng), không bao giờ gán
 * {@code balance = X} (thứ tự quyết định kết quả).
 *
 * <p><b>Khử trùng theo (ví, bút toán), không theo event</b> — xem {@code V1__init_wallet.sql}.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WalletProjector {

    private final WalletRepository walletRepository;
    private final WalletEntryRepository entryRepository;

    /**
     * Áp một bút toán lên các ví của tenant hiện tại mà nó chạm tới.
     *
     * <p>Trình tự trong MỘT transaction: khoá các ví (theo thứ tự id) → với mỗi ví, chèn hàng lịch
     * sử → chỉ khi chèn được (lần đầu) mới cộng số dư. Lịch sử và số dư cùng sống hoặc cùng chết, nên
     * {@code SUM(wallet_entries) = balance_minor} luôn đúng.
     *
     * @param postedAt thời điểm ghi sổ, lấy từ {@code envelope.occurredAt}
     */
    @Transactional
    public void applyPosted(LedgerPostedEvent event, Instant postedAt) {
        UUID tenantId = TenantContext.require();

        Map<UUID, Long> deltaByWallet = walletDeltas(event.legs());
        if (deltaByWallet.isEmpty()) {
            return;   // bút toán chỉ chạm tài khoản hệ thống
        }

        List<Wallet> wallets = walletRepository.lockAll(tenantId, deltaByWallet.keySet(), event.currency());

        for (Wallet wallet : wallets) {
            long delta = deltaByWallet.remove(wallet.getId());
            int firstTime = entryRepository.insertIfAbsent(tenantId, wallet.getId(), event.entryId(),
                    event.transactionId(), event.type().name(), delta, postedAt);
            if (firstTime == 1) {
                walletRepository.addToBalance(wallet.getId(), delta);
            } else {
                log.debug("Wallet: bút toán {} đã áp cho ví {} — bỏ qua", event.entryId(), wallet.getId());
            }
        }

        // Ref dạng UUID mà không phải ví của mình trong tenant này. Bình thường không xảy ra (mọi tài
        // khoản USER của ledger đều do service này mở), nên phải kêu to: hoặc dữ liệu ví bị mất,
        // hoặc có ai mở tài khoản sổ cái không qua wallet-service.
        deltaByWallet.keySet().forEach(unknown -> log.warn(
                "Wallet: bút toán {} chạm ref {} nhưng không có ví nào như vậy ở tenant {} ({})",
                event.entryId(), unknown, tenantId, event.currency()));
    }

    /**
     * Áp trạng thái tài khoản — chỉ khi {@code statusVersion} mới hơn bản đang có. Không cần bảng
     * dedup: bản cũ hoặc bản lặp thành câu UPDATE chạm 0 hàng.
     */
    @Transactional
    public void applyAccountOpened(AccountOpenedEvent event) {
        UUID tenantId = TenantContext.require();
        Optional<UUID> walletId = asWalletId(event.accountRef());
        if (walletId.isEmpty()) {
            log.warn("Wallet: AccountOpened với ref không phải wallet id: {}", event.accountRef());
            return;
        }

        int updated = walletRepository.applyStatusIfNewer(tenantId, walletId.get(),
                EWalletStatus.from(event.status()).name(), event.statusVersion());
        if (updated == 0) {
            log.debug("Wallet: bỏ qua AccountOpened v{} cho ví {} (đã có bản mới hơn hoặc không có ví)",
                    event.statusVersion(), walletId.get());
        }
    }

    /**
     * Gộp các vế theo ví. Ref của ví là UUID dạng chuẩn (ADR-011); tài khoản hệ thống
     * ({@code EXTERNAL_BANK}, {@code HOLD_...}) không phải, nên bị bỏ qua tự nhiên.
     */
    private static Map<UUID, Long> walletDeltas(List<Leg> legs) {
        Map<UUID, Long> deltas = new LinkedHashMap<>();
        for (Leg leg : legs) {
            asWalletId(leg.accountRef())
                    .ifPresent(id -> deltas.merge(id, leg.amountMinor(), Math::addExact));
        }
        return deltas;
    }

    private static Optional<UUID> asWalletId(String ref) {
        if (ref == null) {
            return Optional.empty();
        }
        try {
            UUID id = UUID.fromString(ref);
            return id.toString().equals(ref) ? Optional.of(id) : Optional.empty();
        } catch (IllegalArgumentException notUuid) {
            return Optional.empty();
        }
    }
}
