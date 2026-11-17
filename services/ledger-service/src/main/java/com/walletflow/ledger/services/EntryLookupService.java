package com.walletflow.ledger.services;

import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.ledger.EntryLine;
import com.walletflow.common.ledger.ExternalMovement;
import com.walletflow.common.ledger.ExternalMovementsRequest;
import com.walletflow.common.ledger.JournalEntryView;
import com.walletflow.common.ledger.TopupEntryRef;
import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.dto.EntryLineRow;
import com.walletflow.ledger.repositories.JournalEntryRepository;
import com.walletflow.ledger.repositories.PostingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tra bút toán theo lệnh tiền — đường đọc phục vụ người trực (roadmap 2.5 PR3, ADR-021).
 *
 * <p>Tách khỏi {@link AccountService} vì trả lời một câu hỏi khác: sao kê phục vụ việc dựng lại read
 * model theo TÀI KHOẢN, còn đây phục vụ việc chữa cháy theo LỆNH.
 *
 * <p>Chỉ đọc, không bao giờ ghi: mọi thay đổi sổ cái vẫn chỉ đi qua Kafka command.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EntryLookupService {

    /** Con trỏ đầu tiên: UUID nhỏ nhất, nên {@code id > đây} là mọi bút toán cùng mốc. */
    private static final UUID BEFORE_FIRST = new UUID(0L, 0L);

    private final PostingRepository postingRepository;
    private final JournalEntryRepository journalEntryRepository;

    /**
     * Mọi bút toán ledger đã ghi cho một {@code transactionId}.
     *
     * <p><b>Không 404 khi rỗng.</b> "Ledger chưa ghi gì cho lệnh này" là một sự thật đúng và thường
     * là sự thật quan trọng nhất với người đang xử lý ca — nó phân biệt "tiền chưa bao giờ vào sổ"
     * với "tiền đã vào sổ rồi". Trả 404 ở đây sẽ buộc người gọi coi hai thứ đó như nhau.
     *
     * <p>Một truy vấn duy nhất nên không cần mức cô lập cao hơn mặc định: không có khoảng trống nào
     * giữa hai câu để dữ liệu đổi.
     */
    @Transactional(readOnly = true)
    public TransactionEntries entriesOf(UUID transactionId) {
        UUID tenantId = TenantContext.require();
        List<EntryLineRow> rows = postingRepository.entryLinesOf(tenantId, transactionId);

        // LinkedHashMap: giữ nguyên thứ tự ghi sổ mà câu ORDER BY đã sắp.
        Map<UUID, List<EntryLineRow>> byEntry = new LinkedHashMap<>();
        for (EntryLineRow row : rows) {
            byEntry.computeIfAbsent(row.entryId(), id -> new ArrayList<>()).add(row);
        }

        List<JournalEntryView> entries = byEntry.values().stream().map(EntryLookupService::toView).toList();
        warnIfUnbalanced(tenantId, transactionId, entries);
        return new TransactionEntries(transactionId, entries);
    }

    /**
     * Tiền ròng qua {@code EXTERNAL_BANK} của nhiều lệnh tiền — phía sổ cái của đối soát
     * bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024 §2).
     *
     * <p>Lệnh không có mặt trong kết quả = sổ cái chưa đưa đồng nào qua cửa ngoài cho lệnh đó. Không
     * 404, cùng lý lẽ với {@link #entriesOf}: "chưa vào sổ" là câu trả lời, không phải lỗi.
     */
    @Transactional(readOnly = true)
    public List<ExternalMovement> externalMovementsOf(ExternalMovementsRequest request) {
        UUID tenantId = TenantContext.require();
        List<UUID> ids = request == null ? null : request.transactionIds();
        if (ids == null || ids.isEmpty()) {
            throw new BadRequestException("MISSING_TRANSACTION_IDS", "Phải có ít nhất một transactionId");
        }
        if (ids.size() > ExternalMovementsRequest.MAX_IDS) {
            throw new BadRequestException("TOO_MANY_TRANSACTION_IDS",
                    "Tối đa " + ExternalMovementsRequest.MAX_IDS + " transactionId mỗi lượt, nhận " + ids.size());
        }
        if (ids.contains(null)) {
            throw new BadRequestException("NULL_TRANSACTION_ID", "transactionId không được rỗng");
        }

        return postingRepository.externalMovementsOf(tenantId, ids).stream()
                .map(row -> new ExternalMovement(row.getTransactionId(),
                        ECurrency.valueOf(row.getCurrency()), row.getNetInMinor()))
                .toList();
    }

    /**
     * Một trang bút toán {@code TOPUP} XUYÊN tenant, theo con trỏ — đường quét chiều ngược của đối
     * soát bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024 §3).
     *
     * <p>KHÔNG gọi {@code TenantContext.require()}, và đây là method duy nhất của service làm vậy: câu
     * hỏi là "có lần nạp nào bank-adapter không biết", nên không thể đặt sẵn tenant. Mỗi phần tử trả
     * về mang {@code tenantId} của chính nó. Chỉ đọc, và chỉ lộ id — không lộ số tiền hay tài khoản.
     *
     * @param afterPostedAt con trỏ; {@code null} = từ đầu
     * @param afterId       con trỏ; {@code null} = từ đầu
     * @param before        chỉ lấy bút toán ghi TRƯỚC mốc này — người gọi bỏ qua cái chưa yên
     */
    @Transactional(readOnly = true)
    public List<TopupEntryRef> topupsAfter(Instant afterPostedAt, UUID afterId, Instant before, int limit) {
        if (before == null) {
            throw new BadRequestException("MISSING_BEFORE", "Thiếu before");
        }
        // Nửa con trỏ là lỗi của người gọi, không phải "từ đầu": im lặng quét lại từ đầu thì một client hỏng
        // lặp mãi trang đầu mà không ai biết.
        if ((afterPostedAt == null) != (afterId == null)) {
            throw new BadRequestException("INVALID_CURSOR",
                    "afterPostedAt và afterId phải cùng có hoặc cùng vắng");
        }
        if (limit < 1 || limit > TopupEntryRef.MAX_PAGE) {
            throw new BadRequestException("INVALID_PAGE_LIMIT",
                    "limit phải nằm trong [1, " + TopupEntryRef.MAX_PAGE + "], nhận " + limit);
        }
        Instant fromPostedAt = afterPostedAt == null ? Instant.EPOCH : afterPostedAt;
        UUID fromId = afterId == null ? BEFORE_FIRST : afterId;

        return journalEntryRepository.topupsAfter(fromPostedAt, fromId, before, limit).stream()
                .map(row -> new TopupEntryRef(row.getTenantId(), row.getEntryId(),
                        row.getTransactionId(), row.getPostedAt()))
                .toList();
    }

    private static JournalEntryView toView(List<EntryLineRow> lines) {
        EntryLineRow first = lines.getFirst();
        List<EntryLine> entryLines = lines.stream()
                .map(r -> new EntryLine(r.accountId(), r.accountRef(), r.amountMinor()))
                .toList();
        return new JournalEntryView(first.entryId(), first.type(), first.referenceId(), first.postedAt(), entryLines);
    }

    /**
     * Một bút toán có tổng khác 0 là tiền tự sinh hoặc bốc hơi — bất biến vàng §5.1 đã vỡ.
     *
     * <p>Không ném: người trực đang xử lý sự cố cần NHÌN THẤY bút toán lệch đó, không phải nhận một
     * lỗi 500 giấu mất nó. Nhưng cũng không được đi qua trong im lặng, nên để lại log mức ERROR.
     */
    private static void warnIfUnbalanced(UUID tenantId, UUID transactionId, List<JournalEntryView> entries) {
        for (JournalEntryView entry : entries) {
            if (!entry.balanced()) {
                log.error("BẤT BIẾN VỠ: bút toán {} (tenant {}, lệnh {}) có tổng các vế = {} ≠ 0",
                        entry.entryId(), tenantId, transactionId, entry.sumMinor());
            }
        }
    }
}
