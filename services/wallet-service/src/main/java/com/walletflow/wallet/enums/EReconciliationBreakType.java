package com.walletflow.wallet.enums;

/**
 * Các loại lệch mà đối soát TẦNG 2 phát hiện được (roadmap 3.1, ADR-023 §3).
 *
 * <p>Riêng của wallet-service: tầng 1 ở ledger-service có tập loại khác hẳn. Lưu vào DB dạng chuỗi
 * để thêm loại mới không cần migration.
 */
public enum EReconciliationBreakType {

    /**
     * {@code balance_minor} ≠ {@code SUM(wallet_entries)} — ví tự mâu thuẫn với chính nó (tầng 2a).
     *
     * <p>Tính chất này được viết ra từ {@code V1__init_wallet.sql}: "một phép đối soát nội bộ miễn
     * phí, không cần hỏi ledger". Lệch ở đây là lỗi của projector hoặc một câu {@code UPDATE} gõ
     * tay — thấy được mà không phải hỏi ai.
     */
    PROJECTION_SELF_MISMATCH,

    /**
     * {@code balance_minor} ≠ {@code SUM(postings)} bên ledger — ví lệch khỏi NGUỒN SỰ THẬT (tầng 2b).
     *
     * <p>Nặng hơn {@link #PROJECTION_SELF_MISMATCH}: ví có thể tự khớp hoàn hảo mà vẫn sai, khi
     * projector bỏ sót CẢ hàng lịch sử lẫn số dư. Đó là lý do 2a không thay thế được 2b.
     */
    LEDGER_MISMATCH,

    /**
     * Ví đã {@code ACTIVE} và có số dư khác 0, nhưng ledger không có tài khoản nào cho nó.
     *
     * <p>Không phải lệch số — là một ví đang giữ tiền không tồn tại trong sổ cái. Ví còn
     * {@code PENDING} thì KHÔNG tính: lệnh mở tài khoản chưa tới ledger là trạng thái bình thường,
     * và {@code PendingWalletReconciler} đã lo việc đó.
     */
    LEDGER_ACCOUNT_MISSING
}
