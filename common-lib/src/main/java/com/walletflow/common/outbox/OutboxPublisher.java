package com.walletflow.common.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.event.EventEnvelope;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Nghiệp vụ gọi {@link #publish} để GHI message vào bảng outbox trong cùng transaction, thay cho
 * việc gọi thẳng {@code kafkaTemplate.send(...)}.
 *
 * <p>Dùng {@link Propagation#MANDATORY}: bắt buộc phải được gọi BÊN TRONG một transaction nghiệp vụ
 * đang mở — nếu không sẽ ném lỗi. Đây chính là điều kiện để outbox và dữ liệu nghiệp vụ "cùng sống
 * cùng chết" (plan §7.1).
 *
 * <p>Message được bọc trong {@link EventEnvelope} NGAY TẠI ĐÂY, và cột {@code payload} lưu đúng
 * chuỗi JSON sẽ nằm trên Kafka. Nhờ vậy: (1) relay không cần biết gì về kiểu dữ liệu nghiệp vụ,
 * (2) nội dung trên wire và trong DB giống nhau nên debug/replay được, (3) {@code traceId} của
 * giao dịch nghiệp vụ được đóng băng vào message trước khi span đóng.
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxPublisher {

  private final ObjectMapper objectMapper;

  @PersistenceContext
  private EntityManager entityManager;

  /**
   * @param topic    topic Kafka đích
   * @param msgKey   partition key — dùng {@code walletId/accountId} để mọi message của cùng một ví
   *                 vào cùng partition, đảm bảo thứ tự theo từng ví (plan §11, bài toán P10)
   * @param eventId  định danh event, PHẢI TẤT ĐỊNH theo sự kiện nghiệp vụ (vd id của bút toán) để
   *                 lần phát lại vẫn trùng eventId và consumer dedup được — xem
   *                 {@link EventEnvelope#of}
   * @param eventType tên loại event dùng cho consumer/metric (vd {@code "LedgerPosted"})
   * @param payload  dữ liệu nghiệp vụ, sẽ nằm trong {@code envelope.payload}
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public void publish(String topic, String msgKey, UUID eventId, String eventType, Object payload) {
    EventEnvelope<Object> envelope =
        EventEnvelope.of(eventId, eventType, currentTraceId(), payload, Instant.now());
    try {
      OutboxEvent event = OutboxEvent.builder()
          .topic(topic)
          .msgKey(msgKey)
          .eventId(eventId)
          .eventType(eventType)
          .payload(objectMapper.writeValueAsString(envelope))
          .published(false)
          .build();
      entityManager.persist(event);
      log.debug("Outbox: đã ghi {} tới {} (key={}, eventId={})", eventType, topic, msgKey, eventId);
    } catch (Exception e) {
      // Lỗi serialize -> để transaction nghiệp vụ rollback cùng
      throw new IllegalStateException("Không serialize được payload outbox", e);
    }
  }

  /**
   * Lấy traceId đang hoạt động từ MDC.
   *
   * <p>Đọc qua MDC thay vì phụ thuộc {@code io.micrometer.tracing.Tracer} để common-lib không kéo
   * thêm dependency tracing lên những service không cần. Spring Boot khi bật tracing sẽ tự đưa
   * {@code traceId}/{@code spanId} vào MDC, nên cách này hoạt động sẵn; khi tracing tắt (vd trong
   * test) thì trả về {@code null} và envelope đơn giản là không có traceId.
   */
  private static String currentTraceId() {
    return MDC.get("traceId");
  }
}
