package com.walletflow.transaction.enums;

/** Vòng đời một yêu cầu 4-eyes (roadmap 2.5 PR2). */
public enum EResolutionStatus {
    /** Đã đề xuất, đang chờ một người KHÁC duyệt. Mỗi giao dịch tối đa một hàng ở trạng thái này. */
    PENDING,
    /** Đã duyệt VÀ đã thực thi — hai việc trong cùng một transaction, không có trạng thái ở giữa. */
    APPROVED,
    /** Người duyệt từ chối. Ca quay về hàng đợi để người khác đề xuất cách khác. */
    REJECTED
}
