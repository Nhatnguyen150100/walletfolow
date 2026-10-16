package com.walletflow.wallet.support;

import com.walletflow.common.event.AccountOpenedEvent;
import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.Leg;
import com.walletflow.common.ledger.StatementLine;
import com.walletflow.common.money.ECurrency;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Dựng event của ledger cho test — đúng hình dạng ledger thật phát ra. */
public final class Events {

    public static final String BANK = "EXTERNAL_BANK";

    private Events() {
    }

    /** Nạp tiền: EXTERNAL_BANK -amount, ví +amount. */
    public static LedgerPostedEvent topUp(UUID wallet, long amount) {
        return posted(EJournalType.TOPUP, new Leg(BANK, -amount), new Leg(wallet.toString(), amount));
    }

    /** Rút tiền: ví -amount, EXTERNAL_BANK +amount. */
    public static LedgerPostedEvent withdraw(UUID wallet, long amount) {
        return posted(EJournalType.WITHDRAW, new Leg(wallet.toString(), -amount), new Leg(BANK, amount));
    }

    public static LedgerPostedEvent transfer(UUID from, UUID to, long amount) {
        return posted(EJournalType.TRANSFER, new Leg(from.toString(), -amount), new Leg(to.toString(), amount));
    }

    public static LedgerPostedEvent posted(EJournalType type, Leg... legs) {
        return new LedgerPostedEvent(UUID.randomUUID(), UUID.randomUUID(), type, ECurrency.VND, List.of(legs));
    }

    public static AccountOpenedEvent opened(UUID wallet, EAccountStatus status, int version) {
        return new AccountOpenedEvent(UUID.randomUUID(), wallet.toString(), ECurrency.VND, status, version);
    }

    /** Dòng sao kê mà ledger sẽ trả cho {@code wallet} ứng với bút toán {@code event}. */
    public static StatementLine lineFor(UUID wallet, LedgerPostedEvent event) {
        long amount = event.legs().stream()
                .filter(leg -> leg.accountRef().equals(wallet.toString()))
                .mapToLong(Leg::amountMinor)
                .sum();
        return new StatementLine(event.entryId(), event.transactionId(), event.type(), amount, Instant.now());
    }
}
