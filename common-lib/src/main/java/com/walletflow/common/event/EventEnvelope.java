package com.walletflow.common.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Bao bì chuẩn (envelope) cho MỌI message Kafka trong hệ thống.
 *
 * <p>Hai trường quan trọng nhất về mặt kiến trúc phân tán:
 * <ul>
 *   <li>{@code eventId}: định danh DUY NHẤT của message — phía consumer dùng để KHỬ TRÙNG
 *       (dedup), giải quyết việc Kafka giao at-least-once có thể gửi lặp (bài toán P5).</li>
 *   <li>{@code traceId}: nối ngữ cảnh trace xuyên suốt nhiều service để quan sát 1 giao dịch
 *       trên một dòng thời gian (bài toán P11).</li>
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

  /** Tạo envelope phiên bản 1 với eventId ngẫu nhiên. */
  public static <T> EventEnvelope<T> of(String eventType, String traceId, T payload, Instant occurredAt) {
    return new EventEnvelope<>(UUID.randomUUID(), eventType, 1, occurredAt, traceId, payload);
  }
}
