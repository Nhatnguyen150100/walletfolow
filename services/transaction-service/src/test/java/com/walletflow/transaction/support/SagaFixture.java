package com.walletflow.transaction.support;

import com.walletflow.common.event.BankChargePendingEvent;
import com.walletflow.common.event.BankCompletedEvent;
import com.walletflow.common.event.BankFailedEvent;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.LedgerRejectedEvent;
import com.walletflow.common.event.Leg;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.transaction.dto.TopupRequest;
import com.walletflow.transaction.dto.TransferRequest;
import com.walletflow.transaction.services.AcceptResult;
import com.walletflow.transaction.services.TopupService;
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
        AcceptResult result = TenantContext.callWith(tenantId, () -> transferService.accept(
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

    // ------------------------------------------------------------------
    // Nạp tiền (2.4a) — {@code from} luôn null: tiền đến từ ngân hàng, không từ ví nào
    // ------------------------------------------------------------------

    public static final String PROVIDER = "mock";

    public static Accepted acceptTopup(TopupService topupService, FakeWalletDirectory wallets,
                                       UUID tenantId, long amount) {
        UUID user = UUID.randomUUID();
        UUID wallet = wallets.add(tenantId, user, ECurrency.VND, EWalletStatus.ACTIVE);
        AcceptResult result = TenantContext.callWith(tenantId, () -> topupService.accept(
                user, "k-" + UUID.randomUUID(), new TopupRequest(wallet, amount, ECurrency.VND)));
        return new Accepted(tenantId, result.transactionId(), null, wallet, amount);
    }

    public static BankChargePendingEvent chargePending(Accepted txn) {
        String ref = "MOCK-" + txn.transactionId();
        return new BankChargePendingEvent(txn.transactionId(), txn.to(), PROVIDER, ref,
                "https://sandbox.example/pay/" + ref);
    }

    public static BankCompletedEvent bankCompleted(Accepted txn) {
        return bankCompleted(txn, txn.to(), txn.amount(), ECurrency.VND);
    }

    /** Biến thể lệch dùng cho ca "xác nhận không khớp lệnh đã gửi". */
    public static BankCompletedEvent bankCompleted(Accepted txn, UUID wallet, long amount, ECurrency currency) {
        return new BankCompletedEvent(txn.transactionId(), EBankOperation.CHARGE, wallet, PROVIDER,
                "MOCK-" + txn.transactionId(), amount, currency);
    }

    public static BankFailedEvent bankFailed(Accepted txn, String failureCode) {
        return new BankFailedEvent(txn.transactionId(), EBankOperation.CHARGE, txn.to(), PROVIDER,
                "MOCK-" + txn.transactionId(), failureCode);
    }

    public static LedgerPostedEvent topupPosted(Accepted txn) {
        return new LedgerPostedEvent(txn.transactionId(), UUID.randomUUID(), EJournalType.TOPUP,
                ECurrency.VND, List.of(new Leg("EXTERNAL_BANK", -txn.amount()),
                        new Leg(txn.to().toString(), txn.amount())));
    }

    public static LedgerRejectedEvent topupRejected(Accepted txn, String code) {
        return new LedgerRejectedEvent(txn.transactionId(), EJournalType.TOPUP, code,
                "Account CLOSED. accountRef = " + txn.to());
    }
}
