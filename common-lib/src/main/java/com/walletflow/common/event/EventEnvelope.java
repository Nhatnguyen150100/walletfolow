package com.walletflow.common.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Bao bì chuẩn (envelope) cho MỌI message Kafka trong hệ thống (plan §11).
 *
 * <p>Hai trường quan trọng nhất về mặt kiến trúc phân tán:
 * <ul>
 *   <li>{@code eventId}: định danh DUY NHẤT của message — phía consumer dùng để KHỬ TRÙNG
 *       (dedup), giải quyết việc Kafka giao at-least-once có thể gửi lặp (bài toán P5).</li>
 *   <li>{@code traceId}: nối ngữ cảnh trace xuyên suốt nhiều service để quan sát 1 giao dịch
 *       trên một dòng thời gian (bài toán P11). Đi trong THÂN message chứ không chỉ trong header
 *       Kafka, vì message được relay gửi đi trên một luồng nền — lúc đó span của giao dịch nghiệp
 *       vụ đã đóng từ lâu. traceId phải được "đóng băng" vào lúc ghi outbox.</li>
 * </ul>
 *
 * <p>{@code version} cho phép tiến hoá schema: thêm field tương thích ngược; đổi phá vỡ thì
 * tăng version + dùng topic mới (vd {@code *.v2}).
 *
 * @param <T> kiểu payload nghiệp vụ
 */
public record EventEnvelope<T>(
    UUID eventId,
    String eventType,
    int version,
    Instant occurredAt,
    String traceId,
    T payload) {

  public static final int CURRENT_VERSION = 1;

  /**
   * Tạo envelope với {@code eventId} do người gọi chỉ định.
   *
   * <p><b>Luôn dùng dạng này khi event có khoá tất định.</b> Ví dụ event "đã ghi bút toán" nên lấy
   * {@code eventId = journalEntry.id}: nếu command bị Kafka giao lại và ledger phát lại event
   * (đúng theo ngữ nghĩa idempotent ở plan §7.2), consumer sẽ thấy CÙNG một {@code eventId} nên
   * dedup được. Nếu dùng {@code eventId} ngẫu nhiên, mỗi lần phát lại là một event "mới" đối với
   * consumer → số dư read model bị cộng hai lần. Đây là cái bẫy chết người của cặp
   * "at-least-once + dedup theo eventId".
   */
  public static <T> EventEnvelope<T> of(
      UUID eventId, String eventType, String traceId, T payload, Instant occurredAt) {
    return new EventEnvelope<>(eventId, eventType, CURRENT_VERSION, occurredAt, traceId, payload);
  }

  /**
   * Tạo envelope với {@code eventId} ngẫu nhiên — chỉ dùng cho event KHÔNG có khoá tất định
   * (vd thông báo, log nghiệp vụ) và việc xử lý trùng là vô hại.
   */
  public static <T> EventEnvelope<T> ofRandomId(
      String eventType, String traceId, T payload, Instant occurredAt) {
    return of(UUID.randomUUID(), eventType, traceId, payload, occurredAt);
  }
}
