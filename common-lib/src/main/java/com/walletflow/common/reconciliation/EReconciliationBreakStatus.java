package com.walletflow.common.reconciliation;

/**
 * Vòng đời của một chỗ LỆCH (roadmap 3.1, ADR-023 §5).
 *
 * <p>Chỉ có hai trạng thái, và không có đường nào từ {@link #CLOSED} quay lại: một break đã được
 * người xem xong thì xong. Chỗ đó lệch lại lần nữa sẽ là một hàng MỚI — nhờ
 * {@code uk_recon_break_open} chỉ ràng buộc các hàng {@code OPEN}.
 */
public enum EReconciliationBreakStatus {

    /**
     * Chưa có người xử lý. Kể cả khi máy đã tự sửa được hậu quả ({@code auto_repaired = true}):
     * rebuild chép lại con số đúng, nó không sửa cái bug đã làm lệch. "Break phải được đóng bởi
     * người, không tự biến mất" (roadmap §3.1).
     */
    OPEN,

    /** Đã có người kết luận. Ai và vì sao nằm ở {@code closed_by} / {@code closed_note}. */
    CLOSED
}
