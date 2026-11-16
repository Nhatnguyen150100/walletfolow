package com.walletflow.bank.support;

import com.walletflow.bank.client.LedgerClient;
import com.walletflow.bank.client.LedgerUnavailableException;
import com.walletflow.common.ledger.TopupEntryRef;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sổ cái giả cho đối soát bank-adapter ↔ sổ cái: test tự khai "sổ cái nói gì" về từng lệnh tiền.
 *
 * <p>Phân theo tenant như ledger thật: tenant đọc từ {@code TenantContext.require()} NGAY TRONG lời gọi, nên
 * code production gọi mà quên đặt tenant (hoặc đặt nhầm tenant) thì thấy lỗi/thiếu dữ liệu ngay, thay vì
 * "chạy đúng" trên một thế giới không có tenant.
 *
 * <p>Ba thứ để dựng các ca khó mà không cần ledger thật:
 * <ul>
 *   <li><b>Chèn lỗi</b> — {@link #failNextMovementCalls}, {@link #failTopups}: ledger chập chờn/chết.</li>
 *   <li><b>Đổi trạng thái GIỮA HAI LẦN ĐỌC</b> — {@link #afterMovementCall}: saga ghi sổ ngay sau lần đọc
 *       thứ nhất của đối soát (ca "double read").</li>
 *   <li><b>Ghi lại mọi lời gọi</b> — để test khẳng định đúng tenant, đúng lô.</li>
 * </ul>
 */
public class FakeLedger implements LedgerClient {

    /** Một lời gọi {@code externalMovementsOf} đã nhận. */
    public record MovementCall(UUID tenantId, List<UUID> transactionIds) {
    }

    /** Một lời gọi {@code topupsAfter} đã nhận. */
    public record TopupCall(Instant afterPostedAt, UUID afterId, Instant before, int limit) {
    }

    private record Key(UUID tenantId, UUID transactionId, ECurrency currency) {
    }

    private final Map<Key, Long> nets = new ConcurrentHashMap<>();
    private final List<TopupEntryRef> topups = new CopyOnWriteArrayList<>();
    private final List<MovementCall> movementCalls = new CopyOnWriteArrayList<>();
    private final List<TopupCall> topupCalls = new CopyOnWriteArrayList<>();

    private final AtomicInteger failMovements = new AtomicInteger();
    private volatile boolean failTopups;
    private volatile int hookAfterCall = -1;
    private volatile Runnable hook = () -> { };

    public void reset() {
        nets.clear();
        topups.clear();
        movementCalls.clear();
        topupCalls.clear();
        failMovements.set(0);
        failTopups = false;
        hookAfterCall = -1;
        hook = () -> { };
    }

    // ------------------------------------------------------------------
    // Khai dữ liệu
    // ------------------------------------------------------------------

    /** Sổ cái ghi ròng {@code netInMinor} qua EXTERNAL_BANK cho lệnh này (0 = có bút toán nhưng triệt tiêu). */
    public void declareNet(UUID tenantId, UUID transactionId, ECurrency currency, long netInMinor) {
        nets.put(new Key(tenantId, transactionId, currency), netInMinor);
    }

    /** Như trên cho VND, KHÔNG kèm bút toán TOPUP trong danh sách quét ngược. */
    public void declareNet(UUID tenantId, UUID transactionId, long netInMinor) {
        declareNet(tenantId, transactionId, ECurrency.VND, netInMinor);
    }

    /** Bút toán TOPUP xuất hiện trong trang quét ngược (không tự đặt số tiền — dùng {@link #declareNet}). */
    public TopupEntryRef declareTopupEntry(UUID tenantId, UUID transactionId, Instant postedAt) {
        TopupEntryRef ref = new TopupEntryRef(tenantId, UUID.randomUUID(), transactionId, postedAt);
        topups.add(ref);
        return ref;
    }

    /** Một lần nạp VND đầy đủ như ledger thật: bút toán TOPUP (để quét ngược thấy) + ròng {@code netInMinor}. */
    public TopupEntryRef declareTopup(UUID tenantId, UUID transactionId, long netInMinor, Instant postedAt) {
        declareNet(tenantId, transactionId, netInMinor);
        return declareTopupEntry(tenantId, transactionId, postedAt);
    }

    // ------------------------------------------------------------------
    // Chèn lỗi / móc
    // ------------------------------------------------------------------

    /** {@code externalMovementsOf} ném {@link LedgerUnavailableException} cho {@code n} lời gọi kế tiếp. */
    public void failNextMovementCalls(int n) {
        failMovements.set(n);
    }

    /** Mọi {@code topupsAfter} sau đó ném {@link LedgerUnavailableException}. */
    public void failTopups() {
        failTopups = true;
    }

    /**
     * Chạy {@code action} NGAY SAU khi lời gọi {@code externalMovementsOf} thứ {@code k} (tính từ 1, kể cả
     * lời gọi lỗi) đã tính xong câu trả lời nhưng TRƯỚC khi trả — câu trả lời đó vẫn là trạng thái cũ, còn
     * lần đọc kế tiếp thấy trạng thái mới.
     */
    public void afterMovementCall(int k, Runnable action) {
        this.hookAfterCall = k;
        this.hook = action;
    }

    // ------------------------------------------------------------------
    // Quan sát
    // ------------------------------------------------------------------

    public List<MovementCall> movementCalls() {
        return List.copyOf(movementCalls);
    }

    public List<TopupCall> topupCalls() {
        return List.copyOf(topupCalls);
    }

    // ------------------------------------------------------------------
    // LedgerClient
    // ------------------------------------------------------------------

    @Override
    public Map<UUID, Map<ECurrency, Long>> externalMovementsOf(Collection<UUID> transactionIds) {
        UUID tenantId = TenantContext.require();
        movementCalls.add(new MovementCall(tenantId, List.copyOf(transactionIds)));
        int callNumber = movementCalls.size();

        if (failMovements.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            runHookIfDue(callNumber);
            throw new LedgerUnavailableException();
        }

        Map<UUID, Map<ECurrency, Long>> result = new LinkedHashMap<>();
        for (UUID transactionId : transactionIds) {
            for (ECurrency currency : ECurrency.values()) {
                Long net = nets.get(new Key(tenantId, transactionId, currency));
                if (net != null) {
                    result.computeIfAbsent(transactionId, id -> new LinkedHashMap<>()).put(currency, net);
                }
            }
        }
        runHookIfDue(callNumber);
        return result;
    }

    @Override
    public List<TopupEntryRef> topupsAfter(Instant afterPostedAt, UUID afterId, Instant before, int limit) {
        topupCalls.add(new TopupCall(afterPostedAt, afterId, before, limit));
        if (failTopups) {
            throw new LedgerUnavailableException();
        }
        Comparator<TopupEntryRef> order = Comparator.comparing(TopupEntryRef::postedAt)
                .thenComparing(TopupEntryRef::entryId);
        List<TopupEntryRef> page = new ArrayList<>();
        topups.stream()
                .filter(t -> t.postedAt().isBefore(before))
                .filter(t -> afterPostedAt == null || afterId == null
                        || t.postedAt().isAfter(afterPostedAt)
                        || (t.postedAt().equals(afterPostedAt) && t.entryId().compareTo(afterId) > 0))
                .sorted(order)
                .limit(limit)
                .forEach(page::add);
        return page;
    }

    private void runHookIfDue(int callNumber) {
        if (callNumber == hookAfterCall) {
            hook.run();
        }
    }
}
