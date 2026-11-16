package com.walletflow.common.reconciliation;

/**
 * Kết cục của MỘT lượt đối soát (roadmap 3.1, ADR-023).
 *
 * <p>Ở common-lib vì mọi service chạy đối soát phải dùng chung đúng ba giá trị này: báo cáo tổng hợp
 * của 3.2 gộp nhiều nguồn, và hai service gọi cùng một trạng thái bằng hai cái tên là chỗ báo cáo
 * bắt đầu nói dối. Đây là enum thuần — KHÔNG phải entity, nên nó không bị {@code @EntityScan} kéo
 * vào service không có bảng đối soát.
 */
public enum EReconciliationRunStatus {

    /**
     * Đang quét, hoặc đã chết giữa chừng.
     *
     * <p>Hai thứ đó cố ý KHÔNG phân biệt được: không có tiến trình nào dọn hàng bỏ lại, vì một lượt
     * chết là thông tin cần nhìn thấy chứ không phải rác cần quét (ADR-023 §6). Cửa sổ đó không bao
     * giờ chạy lại, nên {@code last_success} già đi và alert bắn.
     */
    RUNNING,

    /** Quét xong. KHÔNG có nghĩa là không có lệch — lệch nằm ở {@code break_count}. */
    SUCCEEDED,

    /**
     * Quét hỏng vì lỗi KỸ THUẬT (DB đứt, ledger không trả lời) — không phải vì lệch tiền.
     * Phân biệt hai thứ này là nguyên tắc §7 của CLAUDE.md: lệch tiền là kết quả, hỏng là sự cố.
     */
    FAILED
}
