package com.walletflow.wallet.support;

import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.ledger.AccountStatement;
import com.walletflow.common.ledger.StatementLine;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.client.LedgerClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sổ cái giả cho test rebuild: test tự khai "sổ cái nói gì" về một tài khoản. Phân theo tenant như
 * ledger thật — hỏi bằng tenant khác thì không thấy gì.
 */
public class FakeLedger implements LedgerClient {

    private final Map<String, AccountStatement> statements = new ConcurrentHashMap<>();

    /** Chạy ĐÚNG LÚC rebuild đang đọc sổ cái — để test chen một việc vào giữa "khoá" và "đọc". */
    private volatile Runnable duringRead = () -> { };

    public void reset() {
        statements.clear();
        duringRead = () -> { };
    }

    public void duringRead(Runnable action) {
        this.duringRead = action;
    }

    public void declare(UUID tenantId, UUID wallet, EAccountStatus status, int version, List<StatementLine> lines) {
        statements.put(key(tenantId, wallet.toString()), new AccountStatement(
                UUID.randomUUID(), wallet.toString(), ECurrency.VND, status, version, List.copyOf(lines)));
    }

    @Override
    public Optional<AccountStatement> statementOf(String accountRef, ECurrency currency) {
        AccountStatement statement = statements.get(key(TenantContext.require(), accountRef));
        duringRead.run();
        return Optional.ofNullable(statement);
    }

    private static String key(UUID tenantId, String ref) {
        return tenantId + "/" + ref;
    }
}
