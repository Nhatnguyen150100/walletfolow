package com.walletflow.transaction.support;

import com.walletflow.common.bank.ProviderContext;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.client.ProviderContextClient;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * bank-adapter giả cho đường ĐỌC ngữ cảnh provider (2.5 PR3). Xem
 * {@link FakeLedgerEntriesClient} về ba trạng thái phải phân biệt.
 */
public class FakeProviderContextClient implements ProviderContextClient {

    private final Map<String, ProviderContext> contexts = new ConcurrentHashMap<>();
    private final AtomicInteger calls = new AtomicInteger();
    private volatile boolean down;

    public void reset() {
        contexts.clear();
        down = false;
        calls.set(0);
    }

    public void put(UUID tenantId, ProviderContext value) {
        contexts.put(key(tenantId, value.transactionId()), value);
    }

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
    public Optional<ProviderContext> contextOf(UUID transactionId) {
        calls.incrementAndGet();
        if (down) {
            return Optional.empty();
        }
        UUID tenantId = TenantContext.require();
        return Optional.of(contexts.getOrDefault(key(tenantId, transactionId),
                ProviderContext.empty(transactionId)));
    }

    private static String key(UUID tenantId, UUID transactionId) {
        return tenantId + "/" + transactionId;
    }
}
