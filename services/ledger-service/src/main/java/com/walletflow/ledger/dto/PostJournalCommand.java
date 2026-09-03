package com.walletflow.ledger.dto;

import com.walletflow.ledger.enums.ECurrency;
import com.walletflow.ledger.enums.EJournalType;

import java.util.List;
import java.util.UUID;

/**
 * Lệnh ghi bút toán, do transaction-service (Saga orchestrator) phát tới ledger (plan §4.3, §11).
 *
 * @param transactionId khoá idempotency nghiệp vụ: cùng {@code (transactionId, type)} thì ledger chỉ
 *                      ghi MỘT lần, gửi lại bao nhiêu lần cũng trả về đúng bút toán đã ghi
 * @param type          loại bút toán; cùng một giao dịch có thể có một bút toán gốc và một
 *                      {@code REVERSAL} bù trừ, nên type là một phần của khoá idempotency
 * @param referenceId   id của bút toán GỐC, bắt buộc khi {@code type = REVERSAL} và phải để trống ở
 *                      các loại khác. Không có nó thì một bút toán bù trừ trở thành một bút toán
 *                      trôi nổi không biết đang bù cho cái gì — mất khả năng kiểm toán, mà kiểm toán
 *                      lại chính là lý do sổ cái tồn tại (plan §5.2, §14)
 * @param currency      loại tiền của toàn bộ bút toán; mọi vế phải cùng loại tiền
 * @param legs          các vế nợ/có, tổng phải bằng 0
 */
public record PostJournalCommand(
        UUID transactionId,
        EJournalType type,
        UUID referenceId,
        ECurrency currency,
        List<Leg> legs
) {
}
