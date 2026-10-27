package com.walletflow.ledger.services;

import com.walletflow.common.ledger.EntryLine;
import com.walletflow.common.ledger.JournalEntryView;
import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.dto.EntryLineRow;
import com.walletflow.ledger.repositories.PostingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    private final PostingRepository postingRepository;

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
