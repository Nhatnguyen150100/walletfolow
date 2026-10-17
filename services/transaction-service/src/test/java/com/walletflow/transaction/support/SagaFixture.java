package com.walletflow.transaction.support;

import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.LedgerRejectedEvent;
import com.walletflow.common.event.Leg;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.transaction.dto.TransferRequest;
import com.walletflow.transaction.services.TransferService;

import java.util.List;
import java.util.UUID;

/** Nhận một lệnh chuyển thật qua {@link TransferService}, và dựng câu trả lời của ledger cho nó. */
public final class SagaFixture {

    private SagaFixture() {
    }

    public record Accepted(UUID tenantId, UUID transactionId, UUID from, UUID to, long amount) {
    }

    public static Accepted accept(TransferService transferService, FakeWalletDirectory wallets,
                                  UUID tenantId, long amount) {
        UUID user = UUID.randomUUID();
        UUID from = wallets.add(tenantId, user, ECurrency.VND, EWalletStatus.ACTIVE);
        UUID to = wallets.add(tenantId, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
        TransferService.AcceptResult result = TenantContext.callWith(tenantId, () -> transferService.accept(
                user, "k-" + UUID.randomUUID(), new TransferRequest(from, to, amount, ECurrency.VND, null)));
        return new Accepted(tenantId, result.transactionId(), from, to, amount);
    }

    public static LedgerPostedEvent posted(Accepted txn) {
        return posted(txn, EJournalType.TRANSFER);
    }

    public static LedgerPostedEvent posted(Accepted txn, EJournalType type) {
        return new LedgerPostedEvent(txn.transactionId(), UUID.randomUUID(), type, ECurrency.VND,
                List.of(new Leg(txn.from().toString(), -txn.amount()), new Leg(txn.to().toString(), txn.amount())));
    }

    public static LedgerRejectedEvent rejected(Accepted txn, String code) {
        return new LedgerRejectedEvent(txn.transactionId(), EJournalType.TRANSFER, code,
                "Insufficient funds. Balance = 12, required = 99");
    }
}
