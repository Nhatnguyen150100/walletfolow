package com.walletflow.common.outbox.interfaces;

import com.walletflow.common.outbox.OutboxEvent;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface OutboxReader {

  /** Batch các event chưa gửi, đã khoá hàng để nhiều relay chạy song song không giẫm lên nhau. */
  List<OutboxEvent> getBatch();

  /** Số event đang chờ gửi — nguồn của gauge {@code walletflow.outbox.pending}. */
  long countPending();

  /**
   * Thời điểm tạo của event chờ lâu nhất, rỗng nếu không còn gì chờ.
   *
   * <p>Đây là chỉ số phát hiện sự cố TỐT HƠN số lượng hàng chờ: outbox có 5 hàng là bình thường nếu
   * chúng vừa được ghi, nhưng là báo động nếu hàng cũ nhất đã nằm đó 10 phút — nghĩa là relay đã
   * chết hoặc Kafka không nhận. Một relay chết hoàn toàn sẽ giữ số lượng hàng chờ ở mức thấp và ổn
   * định nếu tải thấp, nên chỉ nhìn số lượng thì không thấy gì bất thường.
   */
  Optional<Instant> oldestPendingCreatedAt();

  /**
   * Xoá các event ĐÃ GỬI có {@code publishedAt} trước {@code cutoff}.
   *
   * @param batchSize giới hạn số hàng mỗi lần để không khoá bảng quá lâu
   * @return số hàng đã xoá
   */
  int deletePublishedBefore(Instant cutoff, int batchSize);
}
