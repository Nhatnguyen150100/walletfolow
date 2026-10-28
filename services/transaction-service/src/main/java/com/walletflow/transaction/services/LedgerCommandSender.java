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

    /**
     * Tài khoản hệ thống đại diện cho tiền nằm ở ngân hàng/cổng thanh toán. Ledger tự tạo nó cho mỗi
     * tenant ({@code SystemAccountProvisioner}); tên phải khớp từng ký tự, sai tên là ledger từ chối
     * dứt khoát chứ không tạo nhầm một tài khoản mới.
     */
    static final String EXTERNAL_BANK = "EXTERNAL_BANK";

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

    /**
     * Lệnh ghi sổ cho một lần nạp tiền: tiền rời tài khoản hệ thống {@code EXTERNAL_BANK} của tenant
     * và vào ví. Cùng hình dạng hai vế như chuyển tiền, nên bất biến {@code SUM = 0} vẫn đúng và số
     * tiền hệ thống đang nợ ngân hàng luôn đọc được từ chính sổ cái.
     *
     * <p>CHỈ được gọi sau khi provider đã XÁC NHẬN thu được tiền ({@code BankCompleted}) — webhook
     * hay lời báo tạo phiên đều không đủ (ADR-016 §1). Gọi sớm là ghi vào ví số tiền chưa ai trả.
     *
     * @param transactionId khoá idempotency của ledger: mọi lần gửi lại chỉ ra MỘT bút toán
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void sendTopup(UUID transactionId, UUID walletId, long amountMinor, ECurrency currency,
                          UUID eventId) {
        outboxPublisher.publish(
                WalletTopics.CMD_POST_JOURNAL,
                // Key = ví được ghi có. Vế còn lại là tài khoản hệ thống dùng chung, không làm key được:
                // mọi lần nạp sẽ dồn vào một partition.
                walletId.toString(),
                eventId,
                "PostJournal",
                new PostJournalCommand(transactionId, EJournalType.TOPUP, null, currency,
                        List.of(new Leg(EXTERNAL_BANK, -amountMinor),
                                new Leg(walletId.toString(), amountMinor))));
    }

    /**
     * Lệnh ĐẢO một bút toán nạp đã ghi nhầm (2.5 PR4, ADR-022): hai vế của {@link #sendTopup} đổi
     * dấu, nên ví bị ghi nợ đúng số đã được cộng và {@code EXTERNAL_BANK} nhận lại.
     *
     * <p><b>Vế phải đối xứng CHÍNH XÁC với bút toán gốc</b> — ledger cưỡng chế điều đó
     * ({@code REVERSAL_MISMATCH}, ADR-011 §6) và đó là hàng rào thật, không phải hình thức: một
     * "REVERSAL" trỏ vào bút toán có thật nhưng mang vế tuỳ ý là một đường rút tiền khỏi ví bất kỳ,
     * kể cả ví đang bị đóng băng. Vì vậy số tiền ở đây phải lấy từ chính hàng giao dịch, đừng bao
     * giờ nhận từ đầu vào của người gọi API.
     *
     * <p>Cùng {@code transactionId} với lệnh nạp gốc: khoá idempotency của ledger là
     * {@code (tenant, transactionId, type)}, nên {@code REVERSAL} không va với {@code TOPUP} đã có
     * mà vẫn tự chống ghi hai lần cho chính nó. Lớp thứ hai là
     * {@code uk_journal_one_reversal (tenant_id, reference_id)} — mỗi bút toán chỉ đảo được MỘT lần,
     * kể cả khi có hai lệnh đảo được gửi từ hai nơi.
     *
     * @param originalEntryId bút toán bị đảo; thành {@code reference_id} của bút toán mới, và là thứ
     *                        duy nhất nối hai bút toán với nhau khi đối soát
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void sendReversal(UUID transactionId, UUID originalEntryId, UUID walletId,
                             long amountMinor, ECurrency currency, UUID eventId) {
        outboxPublisher.publish(
                WalletTopics.CMD_POST_JOURNAL,
                // Cùng key với lệnh nạp gốc: lệnh đảo phải đi SAU nó trên cùng partition, không bao
                // giờ vượt lên trước.
                walletId.toString(),
                eventId,
                "PostJournal",
                new PostJournalCommand(transactionId, EJournalType.REVERSAL, originalEntryId, currency,
                        List.of(new Leg(walletId.toString(), -amountMinor),
                                new Leg(EXTERNAL_BANK, amountMinor))));
    }
}
