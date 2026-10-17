package com.walletflow.transaction.services;

import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.outbox.OutboxPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Nơi DUY NHẤT dựng lệnh ghi sổ cho một lệnh chuyển — dùng cho cả lần gửi đầu (nhận lệnh) lẫn các
 * lần gửi lại (job quét saga treo). Hai nơi tự dựng lệnh riêng thì sớm muộn sẽ lệch nhau ở vế, dấu
 * hay partition key, và lần gửi lại thành một bút toán KHÁC lần đầu.
 */
@Component
@RequiredArgsConstructor
public class LedgerCommandSender {

    private final OutboxPublisher outboxPublisher;

    /**
     * Xếp lệnh ghi sổ vào outbox, trong transaction của người gọi.
     *
     * @param transactionId khoá idempotency của ledger — GIỐNG NHAU ở mọi lần gửi lại, nên ledger chỉ
     *                      ghi một bút toán dù nhận bao nhiêu lệnh
     * @param eventId       KHÁC NHAU ở mỗi lần gửi. Ledger bỏ qua eventId đã xử lý mà KHÔNG phát lại
     *                      kết quả; gửi lại đúng eventId cũ khi kết quả đầu đã mất là saga treo vĩnh
     *                      viễn. eventId mới thì ledger xử lý lại, gặp bút toán đã có, và phát lại
     *                      {@code LedgerPosted} với eventId = entryId cố định.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void sendTransfer(UUID transactionId, UUID fromWallet, UUID toWallet, long amountMinor,
                             ECurrency currency, UUID eventId) {
        outboxPublisher.publish(
                WalletTopics.CMD_POST_JOURNAL,
                // Key = ví bị ghi nợ: mọi lệnh của một ví đi cùng partition, đúng thứ tự (WalletTopics)
                fromWallet.toString(),
                eventId,
                "PostJournal",
                new PostJournalCommand(transactionId, EJournalType.TRANSFER, null, currency,
                        List.of(new Leg(fromWallet.toString(), -amountMinor),
                                new Leg(toWallet.toString(), amountMinor))));
    }
}
