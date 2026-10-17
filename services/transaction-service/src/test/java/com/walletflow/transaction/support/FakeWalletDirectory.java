package com.walletflow.transaction.support;

import com.walletflow.common.exception.AppException;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.common.wallet.WalletInfo;
import com.walletflow.transaction.client.WalletDirectory;
import org.springframework.http.HttpStatus;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** wallet-service giả, phân theo tenant như thật. Đếm số lần được hỏi để test kiểm "không hỏi lại". */
public class FakeWalletDirectory implements WalletDirectory {

    private final Map<String, WalletInfo> wallets = new ConcurrentHashMap<>();
    private final AtomicInteger lookups = new AtomicInteger();
    private final AtomicReference<Runnable> beforeNextLookup = new AtomicReference<>();
    private volatile boolean down;

    public void reset() {
        wallets.clear();
        lookups.set(0);
        beforeNextLookup.set(null);
        down = false;
    }

    /** Chạy MỘT lần, đúng lúc lần tra cứu kế tiếp bắt đầu — để chen một việc vào giữa request. */
    public void beforeNextLookup(Runnable action) {
        beforeNextLookup.set(action);
    }

    public UUID add(UUID tenantId, UUID owner, ECurrency currency, EWalletStatus status) {
        UUID id = UUID.randomUUID();
        put(tenantId, new WalletInfo(id, owner, currency, status));
        return id;
    }

    public void setStatus(UUID tenantId, UUID walletId, EWalletStatus status) {
        WalletInfo current = wallets.get(key(tenantId, walletId));
        put(tenantId, new WalletInfo(current.id(), current.userId(), current.currency(), status));
    }

    public void goDown() {
        down = true;
    }

    public int lookups() {
        return lookups.get();
    }

    @Override
    public Optional<WalletInfo> find(UUID walletId) {
        Runnable hook = beforeNextLookup.getAndSet(null);
        if (hook != null) {
            hook.run();
        }
        lookups.incrementAndGet();
        if (down) {
            throw new AppException(HttpStatus.SERVICE_UNAVAILABLE, "WALLET_SERVICE_UNAVAILABLE", "giả lập sự cố");
        }
        return Optional.ofNullable(wallets.get(key(TenantContext.require(), walletId)));
    }

    private void put(UUID tenantId, WalletInfo wallet) {
        wallets.put(key(tenantId, wallet.id()), wallet);
    }

    private static String key(UUID tenantId, UUID walletId) {
        return tenantId + "/" + walletId;
    }
}
