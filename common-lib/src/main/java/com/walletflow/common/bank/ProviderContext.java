package com.walletflow.common.bank;

import java.util.List;
import java.util.UUID;

/**
 * Mọi thao tác bank-adapter đã làm với provider cho MỘT lệnh tiền — hợp đồng của API nội bộ
 * {@code GET /internal/provider-transactions?transactionId=...} (bank-adapter → transaction-service,
 * roadmap 2.5 PR3).
 *
 * <p>Đây là nửa còn lại của bức tranh mà người trực cần: saga nói "tôi đã gửi lệnh thu", ledger nói
 * "tôi chưa ghi gì" — chỉ cái này trả lời được "vậy provider đã lấy tiền của khách chưa". Trước PR3,
 * câu trả lời đó chỉ lấy được bằng {@code psql} vào railsdb theo runbook.
 *
 * <p>Danh sách RỖNG nghĩa là bank-adapter chưa từng nhận lệnh nào cho giao dịch này — khác hẳn với
 * "không hỏi được bank-adapter". Người gọi phải giữ được sự phân biệt đó.
 *
 * @param operations theo thứ tự thời gian; thường một {@code CHARGE}, thêm một {@code REFUND} khi
 *                   saga đã bù trừ (ADR-020)
 */
public record ProviderContext(UUID transactionId, List<ProviderOperationView> operations) {

  /** Xem {@link com.walletflow.common.ledger.TransactionEntries} về lý do nổ sớm khi list là null. */
  public ProviderContext {
    operations = List.copyOf(operations);
  }

  public static ProviderContext empty(UUID transactionId) {
    return new ProviderContext(transactionId, List.of());
  }
}
