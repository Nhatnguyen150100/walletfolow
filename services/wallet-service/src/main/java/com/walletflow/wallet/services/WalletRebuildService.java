package com.walletflow.wallet.services;

import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.ledger.AccountStatement;
import com.walletflow.common.ledger.StatementLine;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.client.LedgerClient;
import com.walletflow.wallet.entity.Wallet;
import com.walletflow.wallet.entity.WalletEntry;
import com.walletflow.wallet.enums.EWalletStatus;
import com.walletflow.wallet.repositories.WalletEntryRepository;
import com.walletflow.wallet.repositories.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Dựng lại read model của MỘT ví từ sổ cái (roadmap 1.1).
 *
 * <p>Là bằng chứng sống rằng số dư ở đây thực sự <i>suy ra được</i> từ ledger, và là công cụ khôi
 * phục khi projection hỏng. Ledger luôn thắng: hàng lịch sử thừa bị gỡ, hàng thiếu được thêm, số dư
 * bị ghi đè bằng tổng sao kê.
 *
 * <p><b>Thứ tự KHOÁ VÍ → ĐỌC SỔ CÁI là điều làm nó đúng khi có event đang bay.</b> Với ví đã khoá,
 * mọi bút toán chạm ví chia làm hai nhóm, và cả hai đều được tính đúng một lần:
 * <ul>
 *   <li><b>Có trong sao kê</b> — ta chèn hàng lịch sử của nó. Nếu projector đang chờ khoá để áp
 *       chính bút toán đó, sau khi ta commit nó sẽ chèn trượt (đã có hàng) và KHÔNG cộng thêm.</li>
 *   <li><b>Ghi sổ sau khi ta đọc</b> — không có trong sao kê. Projector áp nó sau khi ta nhả khoá,
 *       cộng delta lên trên con số ta vừa ghi.</li>
 * </ul>
 * Đảo thứ tự (đọc sổ rồi mới khoá) thì một bút toán ghi sổ SAU lần đọc nhưng được projector áp
 * TRƯỚC khi ta khoá sẽ bị ghi đè mất: số dư bằng tổng sao kê cũ, còn hàng lịch sử của nó đã có nên
 * không bao giờ được áp lại.
 *
 * <p>Lời gọi ledger nằm TRONG transaction đang giữ khoá ví — chấp nhận được cho một thao tác quản
 * trị hiếm, với điều kiện client có timeout ngắn (xem {@code HttpLedgerClient}).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WalletRebuildService {

    private final WalletRepository walletRepository;
    private final WalletEntryRepository entryRepository;
    private final LedgerClient ledgerClient;

    public record RebuildResult(
            UUID walletId,
            long balanceBefore,
            long balanceAfter,
            int entriesAdded,
            int entriesRemoved,
            EWalletStatus status) {
    }

    @Transactional
    public RebuildResult rebuild(UUID walletId) {
        UUID tenantId = TenantContext.require();

        Wallet wallet = walletRepository.lock(tenantId, walletId)
                .orElseThrow(() -> new ResourceNotFoundException("Không có ví " + walletId));
        long before = wallet.getBalanceMinor();

        Optional<AccountStatement> found = ledgerClient.statementOf(walletId.toString(), wallet.getCurrency());
        if (found.isEmpty()) {
            // Ledger chưa có tài khoản: ví còn PENDING, chưa thể có giao dịch nào. Không đụng gì.
            log.info("Rebuild ví {}: ledger chưa có tài khoản — giữ nguyên", walletId);
            return new RebuildResult(walletId, before, before, 0, 0, wallet.getStatus());
        }
        AccountStatement statement = found.get();

        Map<UUID, Long> existing = new HashMap<>();
        for (WalletEntry entry : entryRepository.findByTenantIdAndWalletId(tenantId, walletId)) {
            existing.put(entry.getEntryId(), entry.getAmountMinor());
        }

        // Hàng sai số tiền cũng bị gỡ rồi chèn lại — ledger thắng, kể cả về từng dòng
        List<UUID> toRemove = new ArrayList<>();
        Map<UUID, StatementLine> inLedger = new HashMap<>();
        for (StatementLine line : statement.lines()) {
            inLedger.put(line.entryId(), line);
        }
        existing.forEach((entryId, amount) -> {
            StatementLine line = inLedger.get(entryId);
            if (line == null || line.amountMinor() != amount) {
                toRemove.add(entryId);
            }
        });
        if (!toRemove.isEmpty()) {
            entryRepository.deleteEntries(walletId, toRemove);
        }

        int added = 0;
        for (StatementLine line : statement.lines()) {
            added += entryRepository.insertIfAbsent(tenantId, walletId, line.entryId(),
                    line.transactionId(), line.type().name(), line.amountMinor(), line.postedAt());
        }

        long after = statement.balanceMinor();
        walletRepository.overwriteBalance(walletId, after);
        walletRepository.applyStatusIfNewer(tenantId, walletId,
                EWalletStatus.from(statement.status()).name(), statement.statusVersion());

        Wallet rebuilt = walletRepository.findByTenantIdAndId(tenantId, walletId).orElseThrow();
        log.warn("Rebuild ví {}: số dư {} -> {}, +{} / -{} dòng lịch sử, trạng thái {}",
                walletId, before, after, added, toRemove.size(), rebuilt.getStatus());
        return new RebuildResult(walletId, before, after, added, toRemove.size(), rebuilt.getStatus());
    }
}
