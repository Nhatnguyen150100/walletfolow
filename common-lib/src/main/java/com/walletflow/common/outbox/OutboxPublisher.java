package com.walletflow.common.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Nghiệp vụ gọi {@link #publish} để GHI message vào bảng outbox trong cùng transaction, thay cho
 * việc gọi thẳng {@code kafkaTemplate.send(...)}.
 *
 * <p>Dùng {@link Propagation#MANDATORY}: bắt buộc phải được gọi BÊN TRONG một transaction nghiệp vụ
 * đang mở — nếu không sẽ ném lỗi. Đây chính là điều kiện để outbox và dữ liệu nghiệp vụ "cùng sống
 * cùng chết".
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxPublisher {

  private final ObjectMapper objectMapper;

  @PersistenceContext
  private EntityManager entityManager;

  @Transactional(propagation = Propagation.MANDATORY)
  public void publish(String topic, String key, Object payload) {
    try {
      OutboxEvent event = OutboxEvent.builder()
          .topic(topic)
          .msgKey(key)
          .eventType(payload.getClass().getName())
          .payload(objectMapper.writeValueAsString(payload))
          .published(false)
          .build();
      entityManager.persist(event);
      log.debug("Outbox: đã ghi message tới {} (key={})", topic, key);
    } catch (Exception e) {
      // Lỗi serialize -> để transaction nghiệp vụ rollback cùng
      throw new IllegalStateException("Không serialize được payload outbox", e);
    }
  }
}
