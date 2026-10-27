package com.walletflow.common.ledger;

import com.walletflow.common.event.EJournalType;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Một bút toán nhìn từ bên ngoài ledger, đủ cả hai vế.
 *
 * @param entryId     id bút toán — thứ mà {@code REVERSAL} cần để trỏ về bút toán gốc
 *                    (ADR-021 <i>Amendments</i> A1). Cũng là {@code eventId} của
 *                    {@code LedgerPostedEvent} tương ứng.
 * @param referenceId bút toán gốc khi đây LÀ một {@code REVERSAL}; {@code null} với loại khác. Có nó
 *                    thì người trực thấy ngay một ca đã được đảo rồi, khỏi đảo lần hai.
 */
public record JournalEntryView(
    UUID entryId,
    EJournalType type,
    UUID referenceId,
    Instant postedAt,
    List<EntryLine> lines
) {

  /** Xem {@link TransactionEntries} — một list {@code null} ở đây sẽ thành NPE trong {@link #sumMinor()}. */
  public JournalEntryView {
    lines = List.copyOf(lines);
  }

  /**
   * Tổng các vế — phải bằng 0 ở mọi bút toán đã ghi (bất biến vàng §5.1).
   *
   * <p>Để lộ ra đây, thay vì chỉ kiểm trong ledger, vì màn hình của người trực là nơi cuối cùng một
   * bút toán lệch còn có thể bị một con người nhìn thấy.
   */
  public long sumMinor() {
    long total = 0;
    for (EntryLine line : lines) {
      total = Math.addExact(total, line.amountMinor());
    }
    return total;
  }

  public boolean balanced() {
    return sumMinor() == 0;
  }
}
