package com.walletflow.ledger.enums;

/**
 * Các loại lệch mà đối soát TẦNG 1 phát hiện được (roadmap 3.1, ADR-023 §2).
 *
 * <p>Riêng của ledger-service: tầng 2 ở wallet-service có tập loại khác hẳn. Lưu vào DB dạng chuỗi
 * để thêm loại mới không cần migration.
 */
public enum EReconciliationBreakType {

    /**
     * Một nhóm {@code (tenant, currency)} có tổng posting khác 0 — <b>báo động đỏ</b>.
     *
     * <p>Tiền tự sinh hoặc bốc hơi trong sổ của một khách. Không có cách tự sửa: sổ cái append-only
     * và không tồn tại "số đúng" nào để chép về. Gọi người ngay.
     */
    LEDGER_IMBALANCE,

    /**
     * Tổng posting TOÀN HỆ THỐNG khác 0 trong khi từng nhóm đều cân.
     *
     * <p>Nghe như không thể, nhưng đây đúng là ca mà javadoc của {@code PostingRepository.totalOfSystem}
     * cảnh báo theo chiều ngược lại: một vế trỏ vào tài khoản đã mất hoặc lệch tenant bị {@code JOIN
     * accounts} làm biến mất khỏi phép kiểm theo nhóm, nhưng vẫn nằm trong tổng toàn cục. Hai phép
     * kiểm bắt hai loại lỗi khác nhau.
     */
    LEDGER_IMBALANCE_GLOBAL
}
