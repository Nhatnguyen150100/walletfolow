package com.walletflow.common.event;

/**
 * Tên các Kafka topic dùng chung trong luồng tiền của WalletFlow.
 *
 * <p>Quy ước đặt tên: {@code <domain>.<loại>.<tên>.v<version>}.
 * Đặt tập trung ở common-lib để mọi service tham chiếu cùng một hằng số, tránh gõ sai tên topic
 * (lỗi cực khó debug trong hệ phân tán).
 *
 * <p>Mô hình orchestration: {@code transaction-service} là "nhạc trưởng" phát COMMAND tới các
 * participant (ledger, bank-adapter); participant thực thi rồi phát EVENT kết quả để orchestrator
 * quyết định bước kế tiếp hoặc bù trừ (compensation).
 *
 * <p>Partition key cho mọi message của một ví/tài khoản = {@code walletId/accountId} để đảm bảo
 * thứ tự xử lý theo từng ví (bài toán P10), vẫn song song hoá giữa các ví khác nhau.
 */
public final class WalletTopics {

  private WalletTopics() {
  }

  // ---- COMMAND: transaction-service (orchestrator) -> participant ----
  public static final String CMD_POST_JOURNAL = "txn.command.post-journal.v1";   // -> ledger-service
  public static final String CMD_BANK_CHARGE = "txn.command.bank-charge.v1";      // -> bank-adapter (top-up)
  public static final String CMD_BANK_PAYOUT = "txn.command.bank-payout.v1";      // -> bank-adapter (withdraw)
  public static final String CMD_BANK_REFUND = "txn.command.bank-refund.v1";      // -> bank-adapter (bù trừ)

  // ---- EVENT: participant -> orchestrator / read model ----
  public static final String EVT_LEDGER_POSTED = "ledger.event.posted.v1";
  public static final String EVT_LEDGER_REJECTED = "ledger.event.rejected.v1";
  public static final String EVT_BANK_COMPLETED = "bank.event.completed.v1";
  public static final String EVT_BANK_FAILED = "bank.event.failed.v1";

  // ---- EVENT: trạng thái cuối của giao dịch -> notification-service ----
  public static final String EVT_TXN_COMPLETED = "txn.event.completed.v1";
  public static final String EVT_TXN_FAILED = "txn.event.failed.v1";

  /** Hậu tố dead-letter topic (poison message, bài toán P9). */
  public static final String DLT_SUFFIX = ".DLT";
}
