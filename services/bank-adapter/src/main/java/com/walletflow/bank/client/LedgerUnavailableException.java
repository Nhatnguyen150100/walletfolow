package com.walletflow.bank.client;

/**
 * Không hỏi được sổ cái — lỗi KỸ THUẬT, không phải câu trả lời (CLAUDE.md §7).
 *
 * <p>Đối soát bắt nó THEO LÔ: lô đó bị bỏ qua, lượt kết thúc {@code FAILED} và alert
 * {@code ReconciliationRunFailed} bắn (ADR-023 *Amendments* A2). Không bao giờ được hiểu thành "ledger
 * không có bút toán" — đó sẽ là một break giả cho mọi lệnh trong lô.
 *
 * <p>Message cố định, không mang URL nội bộ hay lý do kết nối: chi tiết đã vào log ở chỗ ném.
 */
public class LedgerUnavailableException extends RuntimeException {

    public LedgerUnavailableException() {
        super("Không hỏi được sổ cái");
    }
}
