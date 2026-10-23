package com.walletflow.auth.enums;

/**
 * Vai của một tài khoản. Tách theo thứ người ta ĐƯỢC LÀM, không theo chức danh (roadmap 2.5,
 * ADR-021). Giá trị đi vào claim {@code role} của JWT và ra tới service qua header
 * {@code X-User-Role}; danh sách này phải khớp với CHECK {@code ck_users_role} ở auth V3.
 */
public enum ERole {
  /** Khách hàng. Không thấy gì trong khu vực /admin. */
  USER,
  /** Duyệt được thao tác đụng tiền (4-eyes — 2.5 PR2), và làm được mọi việc của OPS. */
  ADMIN,
  /** Xem hàng đợi cần can thiệp và mở ca xử lý, nhưng KHÔNG tự quyết chuyện tiền. */
  OPS,
  /** Chỉ đọc — để người kiểm tra sổ sách không phải mượn một tài khoản có quyền ghi. */
  AUDITOR
}
