package com.walletflow.common.ledger;

import java.time.Instant;
import java.util.UUID;

/**
 * Một bút toán {@code TOPUP} trong sổ cái — một phần tử của
 * {@code GET /internal/entries/topups}, đường quét CHIỀU NGƯỢC của đối soát bank-adapter ↔ sổ cái
 * (roadmap 3.1 PR2a, ADR-024 §3).
 *
 * <p>Chiều xuôi (từ bảng {@code provider_transactions}) không bao giờ thấy một lần nạp mà bank-adapter
 * không có hàng nào — ví dụ một lệnh ghi sổ gõ tay, hay một bug gửi lệnh nạp không qua saga. Chiều
 * ngược tồn tại để bắt đúng ca đó: sổ cái nói tiền đã vào, mà không có cổng nào thu.
 *
 * <p>Mang {@code tenantId} vì đường quét này XUYÊN tenant: bank-adapter không biết trước tenant nào
 * có bút toán, và lấy danh sách tenant từ DB của nó (railsdb) sẽ bỏ sót đúng tenant mà nó chưa từng thấy.
 *
 * @param postedAt cùng với {@code entryId} làm con trỏ phân trang {@code (postedAt, entryId)}
 */
public record TopupEntryRef(
    UUID tenantId,
    UUID entryId,
    UUID transactionId,
    Instant postedAt
) {

  /** Trần số bút toán mỗi trang — hằng ở đây để người gọi xin đúng con số ledger cưỡng chế. */
  public static final int MAX_PAGE = 500;
}
