package com.walletflow.common.event;

/**
 * Loại thao tác với cổng thanh toán mà bank-adapter thực hiện (roadmap 2.2, ADR-016).
 *
 * <p>Khai đủ ba giá trị ngay từ v1 dù 2.2a mới dùng {@code CHARGE}: thêm giá trị enum vào schema sau
 * này là thay đổi phá vỡ với consumer đang kiểm enum chặt, tức phải lên {@code .v2}.
 */
public enum EBankOperation {
  /** Thu tiền của người dùng — nạp tiền (top-up). */
  CHARGE,
  /** Hoàn lại một lần thu — bù trừ khi ledger từ chối ghi sổ (2.4). */
  REFUND,
  /** Chi tiền ra ngoài — rút tiền; bị chặn chờ kết luận pháp lý (ADR-015). */
  PAYOUT
}
