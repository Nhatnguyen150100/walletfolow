package com.walletflow.common.event;

/**
 * Trạng thái tài khoản sổ cái. Ledger cưỡng chế, wallet-service phản chiếu (ledger V5).
 *
 * <p>Nằm ở {@code common.event} vì nó đi trên dây trong {@link AccountOpenedEvent}.
 */
public enum EAccountStatus {
  /** Bình thường. */
  ACTIVE,
  /** Chặn ghi nợ, vẫn nhận tiền — để hoàn tiền về ví bị đóng băng vẫn chạy được. */
  FROZEN,
  /** Chặn cả hai chiều. */
  CLOSED
}
