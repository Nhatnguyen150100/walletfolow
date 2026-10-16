package com.walletflow.ledger.services;

import com.walletflow.common.dedup.EventDeduplicator;
import com.walletflow.common.event.AccountOpenedEvent;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.ledger.entity.Account;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.ledger.dto.JournalView;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.LedgerRejectedEvent;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.ledger.entity.JournalEntry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Nối sổ cái với Kafka: xử lý {@link PostJournalCommand} rồi phát event kết quả (plan §4.3, §8.1).
 *
 * <p>Tách khỏi {@link LedgerService} vì hai lớp này có hai trách nhiệm khác nhau: {@code LedgerService}
 * là miền nghiệp vụ thuần (không biết Kafka tồn tại), còn lớp này là biên messaging — dedup, đóng gói
 * event, chọn partition key.
 *
 * <p><b>Vì sao thành công và từ chối là HAI transaction riêng:</b> khi bút toán bị từ chối,
 * transaction hiện tại đã bị đánh dấu rollback, nên không thể ghi bản ghi outbox "đã từ chối" trong
 * cùng transaction đó — nó sẽ bị cuốn theo. Phần thất bại phải rollback trọn vẹn (kể cả dấu dedup),
 * rồi một transaction mới ghi lại dấu dedup + event từ chối.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LedgerCommandHandler {

    /** Tên consumer trong bảng dedup — mỗi service một tên riêng (plan §7.5). */
    public static final String CONSUMER = "ledger-service";

    private final LedgerService ledgerService;
    private final AccountService accountService;
    private final EventDeduplicator deduplicator;
    private final OutboxPublisher outboxPublisher;

    /**
     * Ghi bút toán và phát {@code LedgerPostedEvent}, tất cả trong MỘT transaction cùng với dấu dedup
     * và bản ghi outbox. Đây là điểm mấu chốt của plan §7.1 + §7.5: không có khe hở nào giữa "đã ghi
     * sổ", "đã đánh dấu đã xử lý" và "đã xếp event chờ gửi" — cả ba cùng sống hoặc cùng chết.
     *
     * @return {@code true} nếu đã xử lý, {@code false} nếu bỏ qua vì message trùng
     */
    @Transactional
    public boolean handle(UUID eventId, PostJournalCommand command) {
        if (!deduplicator.markProcessed(eventId, CONSUMER)) {
            return false;
        }

        JournalEntry entry = ledgerService.postJournal(command);
        JournalView view = ledgerService.describe(entry);

        outboxPublisher.publish(
                WalletTopics.EVT_LEDGER_POSTED,
                view.primaryWalletRef(),
                // eventId = id của bút toán: TẤT ĐỊNH. Nếu lệnh được giao lại và ledger phát lại
                // event cho cùng bút toán đó, consumer thấy trùng eventId nên dedup được. Dùng
                // UUID ngẫu nhiên ở đây là cách chắc chắn nhất để cộng đôi số dư read model.
                entry.getId(),
                "LedgerPosted",
                new LedgerPostedEvent(
                        entry.getTransactionId(),
                        entry.getId(),
                        entry.getType(),
                        command.currency(),
                        view.legs()));

        return true;
    }

    /**
     * Ghi nhận việc TỪ CHỐI vì lý do nghiệp vụ và phát {@code LedgerRejectedEvent} để Saga biết mà
     * chuyển giao dịch sang {@code FAILED} / chạy bù trừ (plan §9).
     */
    @Transactional
    public void reject(UUID eventId, PostJournalCommand command, AppException rejection) {
        deduplicator.markProcessed(eventId, CONSUMER);

        outboxPublisher.publish(
                WalletTopics.EVT_LEDGER_REJECTED,
                rejectionPartitionKey(command),
                rejectionEventId(command),
                "LedgerRejected",
                new LedgerRejectedEvent(
                        command.transactionId(),
                        command.type(),
                        rejection.getErrorCode(),
                        rejection.getMessage()));
    }

    /**
     * Mở tài khoản cho một ví và phát {@code AccountOpenedEvent} (roadmap 1.0b, ADR-011).
     *
     * <p><b>Cố ý KHÔNG đi qua bảng dedup</b>, khác với {@link #handle}. Dedup theo {@code eventId}
     * có một hệ quả phụ: lệnh trùng bị bỏ qua <b>và không phát lại event</b>. Nếu event gốc bị mất
     * (relay chết giữa chừng, consumer xử lý hỏng), producer gửi lại đúng lệnh cũ sẽ không bao giờ
     * nhận được câu trả lời — ví nằm ở {@code PENDING} mãi mãi.
     *
     * <p>Ở đây không cần dedup vì thao tác vốn đã idempotent tự nhiên ({@code ON CONFLICT DO
     * NOTHING}) và event có {@code eventId} tất định (xem {@link #accountOpenedEventId}): xử lý lại
     * thì sổ không đổi, còn event phát lại thì consumer dedup được. Đổi lại, MỌI lần gửi lệnh đều
     * nhận được câu trả lời.
     */
    @Transactional
    public void openAccount(OpenAccountCommand command) {
        Account account = accountService.openUserAccount(command);

        outboxPublisher.publish(
                WalletTopics.EVT_ACCOUNT_OPENED,
                account.getExternalRef(),
                accountOpenedEventId(account),
                "AccountOpened",
                new AccountOpenedEvent(
                        account.getId(),
                        account.getExternalRef(),
                        account.getCurrency(),
                        account.getStatus()));
    }

    /**
     * eventId tất định theo {@code (accountId, status)} — KHÔNG chỉ theo {@code accountId}.
     *
     * <p>Quy tắc của dedup: cùng eventId phải nghĩa là cùng NỘI DUNG. Event này mang trạng thái HIỆN
     * TẠI của tài khoản, nên gửi lại lệnh mở sau khi ví bị đóng băng sinh ra một event có nội dung
     * khác ({@code FROZEN}). Nếu eventId chỉ là {@code accountId}, consumer coi nó là bản lặp của
     * event {@code ACTIVE} cũ và bỏ đi — read model tin ví còn hoạt động trong khi sổ cái đã chặn.
     * Gộp trạng thái vào eventId thì bản phát lại y hệt vẫn dedup được, còn nội dung mới thì không bị
     * nuốt.
     */
    private static UUID accountOpenedEventId(Account account) {
        String name = "ledger-account-opened:" + account.getId() + ":" + account.getStatus();
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * eventId tất định cho event từ chối.
     *
     * <p>Bút toán bị từ chối thì không có hàng nào trong DB để lấy id, nên sinh UUID theo TÊN từ
     * {@code (transactionId, type)} — cùng một lệnh bị từ chối luôn cho ra cùng một eventId, giữ được
     * tính dedup y như đường thành công.
     */
    private static UUID rejectionEventId(PostJournalCommand command) {
        String name = "ledger-rejected:" + command.transactionId() + ":" + command.type();
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Khi bị từ chối thì chưa resolve được tài khoản, nên không biết ví nào là ví "chủ động". Lấy vế
     * bị ghi nợ đầu tiên: với rút tiền/chuyển tiền đó chính là ví người dùng — đúng thứ ta cần để
     * event từ chối đi cùng partition với các event khác của ví đó.
     */
    private static String rejectionPartitionKey(PostJournalCommand command) {
        if (command.legs() == null) {
            return null;
        }
        return command.legs().stream()
                .filter(leg -> leg.amountMinor() < 0)
                .map(leg -> leg.accountRef())
                .findFirst()
                .orElse(null);
    }
}
