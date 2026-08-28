package com.walletflow.ledger.dto;

import com.walletflow.ledger.enums.EJournalType;

import java.util.UUID;

/**
 * Ledger TỪ CHỐI ghi bút toán vì lý do nghiệp vụ (plan §8.1) — không đủ số dư, bút toán lệch, tài
 * khoản không tồn tại. Saga orchestrator nhận event này để chuyển giao dịch sang {@code FAILED} và
 * chạy bù trừ nếu bước trước đã chạm thế giới ngoài.
 *
 * <p>Phân biệt rạch ròi với LỖI KỸ THUẬT (DB chết, message hỏng): lỗi kỹ thuật KHÔNG sinh event này
 * mà để exception nổ ra cho Kafka retry rồi vào DLT. Nhập nhằng hai loại này là một lỗi đắt: coi lỗi
 * kỹ thuật tạm thời là "từ chối nghiệp vụ" sẽ khiến Saga bù trừ (hoàn tiền) một giao dịch mà đáng lẽ
 * chỉ cần thử lại là xong.
 *
 * @param reasonCode mã lỗi ổn định để orchestrator xử lý theo mã (vd {@code INSUFFICIENT_FUNDS}),
 *                   không phải parse chuỗi {@code reason}
 */
public record LedgerRejectedEvent(
        UUID transactionId,
        EJournalType type,
        String reasonCode,
        String reason
) {
}
