package com.walletflow.ledger.dto;

import com.walletflow.ledger.enums.ECurrency;
import com.walletflow.ledger.enums.EJournalType;

import java.util.List;
import java.util.UUID;

/**
 * Bút toán đã được ghi vào sổ cái — sự thật về tiền đã thay đổi (plan §8.1).
 *
 * <p>Event mang theo <b>toàn bộ các vế</b> chứ không chỉ id, vì consumer chính của nó là
 * wallet-service: nó phải biết ví nào thay đổi bao nhiêu để cập nhật read model số dư. Nếu chỉ gửi
 * {@code entryId}, wallet-service sẽ buộc phải gọi ngược lại ledger để hỏi chi tiết — vừa thêm một
 * điểm chết (P12), vừa phá nguyên tắc "không service nào đọc dữ liệu của service khác" (plan §3).
 *
 * <p>Event tự mang đủ ngữ cảnh cũng chính là điều cho phép consumer xử lý đúng dù message đến hơi
 * lệch thứ tự (plan §7.5 điểm 3): cập nhật số dư là phép cộng dồn theo delta, có tính giao hoán.
 */
public record LedgerPostedEvent(
        UUID transactionId,
        UUID entryId,
        EJournalType type,
        ECurrency currency,
        List<Leg> legs
) {
}
