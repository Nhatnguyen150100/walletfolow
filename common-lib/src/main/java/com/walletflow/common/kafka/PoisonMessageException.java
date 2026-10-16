package com.walletflow.common.kafka;

/**
 * Message KHÔNG THỂ xử lý được, dù có thử lại bao nhiêu lần — "poison message" (plan §12, bài toán P9).
 *
 * <p>Phân biệt với lỗi kỹ thuật tạm thời (DB mất kết nối, Kafka rebalance) là điều quyết định hệ
 * thống có tự khỏi được hay không:
 * <ul>
 *   <li><b>Lỗi tạm thời</b> → thử lại có ích, backoff rồi retry.</li>
 *   <li><b>Poison message</b> (JSON hỏng, thiếu trường bắt buộc, sai kiểu) → thử lại là <b>vô ích</b>.
 *       Retry 5 lần chỉ làm trễ thêm vài giây rồi vẫn vào DLT, đồng thời <b>chặn cả partition</b>:
 *       mọi message hợp lệ xếp sau nó phải chờ. Với topic lệnh tiền, điều đó nghĩa là các giao
 *       dịch của những ví khác bị treo vì một message rác.</li>
 * </ul>
 *
 * <p>Vì vậy exception này được khai trong {@code addNotRetryableExceptions} của
 * {@code DefaultErrorHandler} → đi thẳng DLT ngay lần đầu.
 *
 * <p>Kế thừa {@link IllegalArgumentException} để tương thích ngược với hợp đồng cũ của
 * {@link com.walletflow.common.event.EnvelopeReader} (từng ném {@code IllegalArgumentException}),
 * đồng thời cho phép phân loại chính xác hơn ở tầng xử lý lỗi Kafka.
 */
public class PoisonMessageException extends IllegalArgumentException {

  public PoisonMessageException(String message, Throwable cause) {
    super(message, cause);
  }

  public PoisonMessageException(String message) {
    super(message);
  }
}
