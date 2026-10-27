package com.walletflow.transaction.support;

import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.client.LedgerEntriesClient;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ledger giả cho đường ĐỌC bút toán (2.5 PR3), phân theo tenant như thật.
 *
 * <p>Phân biệt ba trạng thái mà test phải kiểm riêng từng cái: <b>có bút toán</b>,
 * <b>chưa có bút toán nào</b> ({@code TransactionEntries} rỗng), và <b>không hỏi được</b>
 * ({@code Optional.empty()} — {@link #goDown()}). Gộp hai cái sau làm một chính là lỗi mà
 * {@code unavailable} sinh ra để chặn.
 */
public class FakeLedgerEntriesClient implements LedgerEntriesClient {

    private final Map<String, TransactionEntries> entries = new ConcurrentHashMap<>();
    private final AtomicInteger calls = new AtomicInteger();
    private volatile boolean down;

    public void reset() {
        entries.clear();
        down = false;
        calls.set(0);
    }

    public void put(UUID tenantId, TransactionEntries value) {
        entries.put(key(tenantId, value.transactionId()), value);
    }

    /** Ledger không hỏi được — client thật nuốt lỗi và trả {@code Optional.empty()}. */
    public void goDown() {
        down = true;
    }

    public void goUp() {
        down = false;
    }

    /** Số lần bị hỏi — để test chứng minh một đường (vd. 404) KHÔNG đi sang service này. */
    public int calls() {
        return calls.get();
    }

    @Override
    public Optional<TransactionEntries> entriesOf(UUID transactionId) {
        calls.incrementAndGet();
        if (down) {
            return Optional.empty();
        }
        UUID tenantId = TenantContext.require();
        return Optional.of(entries.getOrDefault(key(tenantId, transactionId),
                TransactionEntries.empty(transactionId)));
    }

    private static String key(UUID tenantId, UUID transactionId) {
        return tenantId + "/" + transactionId;
    }
}
