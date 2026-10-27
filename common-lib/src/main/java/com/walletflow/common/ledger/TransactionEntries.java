package com.walletflow.common.ledger;

import java.util.List;
import java.util.UUID;

/**
 * Mọi bút toán của MỘT lệnh tiền — hợp đồng của API nội bộ
 * {@code GET /internal/entries?transactionId=...} (ledger → transaction-service, roadmap 2.5 PR3).
 *
 * <p>Khác {@link AccountStatement} ở câu hỏi nó trả lời: sao kê hỏi "tài khoản này đã chạm những bút
 * toán nào" (dựng read model), còn đây hỏi "lệnh này đã ghi sổ những gì" — câu hỏi của người đang
 * chữa cháy một ca kẹt. Vì thế mỗi bút toán hiện ra ĐỦ CẢ HAI VẾ, không gộp về một tài khoản: biết
 * tiền đi từ đâu sang đâu mới phân loại được ca.
 *
 * <p>Danh sách RỖNG là câu trả lời hợp lệ và thường là câu trả lời quan trọng nhất — "ledger chưa
 * từng ghi gì cho lệnh này". Người gọi phải phân biệt nó với "không hỏi được ledger"; hai thứ đó dẫn
 * tới hai quyết định vận hành ngược nhau.
 *
 * @param entries theo thứ tự ghi sổ; thường một bút toán, hai khi có {@code REVERSAL}
 */
public record TransactionEntries(UUID transactionId, List<JournalEntryView> entries) {

  /**
   * {@code List.copyOf} nổ ngay nếu {@code entries} là {@code null} — và đó là điều muốn.
   *
   * <p>Một câu trả lời 200 thiếu hẳn trường {@code entries} (phiên bản lệch, proxy cắt body) sẽ cho
   * {@code null} và tạo ra trạng thái THỨ BA trên màn hình người trực, bên cạnh "rỗng" và "không hỏi
   * được" — trạng thái mà runbook không định nghĩa và không ai biết đọc. Nổ ở client để nó rơi vào
   * nhánh "không hỏi được", thứ runbook có dạy cách xử lý.
   */
  public TransactionEntries {
    entries = List.copyOf(entries);
  }

  public static TransactionEntries empty(UUID transactionId) {
    return new TransactionEntries(transactionId, List.of());
  }
}
