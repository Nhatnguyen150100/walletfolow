package com.walletflow.wallet.services;

import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.dto.EntryPage;
import com.walletflow.wallet.dto.EntryView;
import com.walletflow.wallet.dto.WalletView;
import com.walletflow.wallet.entity.Wallet;
import com.walletflow.wallet.entity.WalletEntry;
import com.walletflow.wallet.repositories.WalletEntryRepository;
import com.walletflow.wallet.repositories.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletService {

    static final int MAX_PAGE_SIZE = 100;

    private final WalletRepository walletRepository;
    private final WalletEntryRepository entryRepository;
    private final OutboxPublisher outboxPublisher;

    /** Kết quả mở ví: {@code created = false} nghĩa là người này đã có ví loại tiền này. */
    public record OpenResult(WalletView wallet, boolean created) {
    }

    /**
     * Mở ví — idempotent theo (tenant, người dùng, loại tiền).
     *
     * <p>Hàng ví ({@code PENDING}) và lệnh mở tài khoản sổ cái đi ra trong CÙNG transaction qua
     * outbox: không có trạng thái "có ví mà sổ cái không bao giờ biết", cũng không có "sổ cái mở tài
     * khoản cho một ví không tồn tại". Ví thành {@code ACTIVE} khi ledger xác nhận
     * ({@code AccountOpened}).
     */
    @Transactional
    public OpenResult open(UUID userId, ECurrency currency) {
        UUID tenantId = TenantContext.require();

        int created = walletRepository.insertIfAbsent(UUID.randomUUID(), tenantId, userId, currency.name());
        Wallet wallet = walletRepository.findByTenantIdAndUserIdAndCurrency(tenantId, userId, currency)
                .orElseThrow(() -> new IllegalStateException("Không đọc lại được ví vừa tạo/đã có"));

        if (created == 1) {
            requestLedgerAccount(wallet);
            log.info("Wallet: mở ví {} ({}) cho user {} — chờ ledger xác nhận", wallet.getId(), currency, userId);
        }
        return new OpenResult(WalletView.of(wallet), created == 1);
    }

    /**
     * Gửi (lại) lệnh mở tài khoản sổ cái cho một ví. An toàn khi gọi nhiều lần: ledger mở tài khoản
     * idempotent và LUÔN trả lời bằng {@code AccountOpened} (ADR-011).
     */
    @Transactional
    public void requestLedgerAccount(Wallet wallet) {
        outboxPublisher.publish(
                WalletTopics.CMD_OPEN_ACCOUNT,
                wallet.getId().toString(),
                UUID.randomUUID(),
                "OpenAccount",
                new OpenAccountCommand(wallet.getId().toString(), wallet.getCurrency()));
    }

    @Transactional(readOnly = true)
    public List<WalletView> walletsOf(UUID userId) {
        return walletRepository.findByTenantIdAndUserIdOrderByCurrency(TenantContext.require(), userId)
                .stream().map(WalletView::of).toList();
    }

    /**
     * Lịch sử giao dịch theo cursor, mới nhất trước.
     *
     * <p>Không dùng offset: với offset, một giao dịch mới chen vào giữa hai lần gọi sẽ đẩy mọi hàng
     * xuống một bậc — trang sau lặp lại hàng cuối của trang trước. Và {@code OFFSET 100000} bắt DB
     * đọc rồi bỏ 100 000 hàng.
     *
     * @param cursor {@code null} = trang đầu
     */
    @Transactional(readOnly = true)
    public EntryPage history(UUID userId, ECurrency currency, String cursor, int limit) {
        UUID tenantId = TenantContext.require();
        Wallet wallet = walletRepository.findByTenantIdAndUserIdAndCurrency(tenantId, userId, currency)
                .orElseThrow(() -> new ResourceNotFoundException("Chưa có ví " + currency));

        int size = Math.clamp(limit, 1, MAX_PAGE_SIZE);
        // Lấy THỪA một hàng để biết còn trang sau hay không mà không cần COUNT(*)
        List<WalletEntry> rows = entryRepository.pageBefore(
                tenantId, wallet.getId(), decodeCursor(cursor), Limit.of(size + 1));

        boolean hasMore = rows.size() > size;
        List<WalletEntry> page = hasMore ? rows.subList(0, size) : rows;
        String next = hasMore ? String.valueOf(page.get(page.size() - 1).getSeq()) : null;

        return new EntryPage(page.stream().map(EntryView::of).toList(), next);
    }

    private static long decodeCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return Long.MAX_VALUE;
        }
        try {
            long value = Long.parseLong(cursor);
            if (value > 0) {
                return value;
            }
        } catch (NumberFormatException ignored) {
            // rơi xuống dưới
        }
        throw new BadRequestException("INVALID_CURSOR", "Cursor không hợp lệ");
    }
}
