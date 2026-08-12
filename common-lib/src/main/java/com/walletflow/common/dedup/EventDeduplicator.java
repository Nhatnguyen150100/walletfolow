package com.walletflow.common.dedup;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Bộ khử trùng message cho consumer (inbox pattern) — dùng chung cho mọi service tiêu thụ Kafka
 * có JPA. Giải bài toán P5: Kafka giao at-least-once nên consumer có thể nhận trùng.
 *
 * <p>Cách dùng trong một listener (chạy trong CÙNG transaction với xử lý nghiệp vụ):
 * <pre>{@code
 *   @Transactional
 *   public void onMessage(EventEnvelope<X> evt) {
 *       if (!deduplicator.markProcessed(evt.eventId(), "wallet-service")) {
 *           return; // đã xử lý rồi -> bỏ qua an toàn
 *       }
 *       // ... xử lý nghiệp vụ ...
 *   }
 * }</pre>
 *
 * <p>Khoá chính kép (eventId, consumer) ở tầng DB là "chốt chặn" cuối: nếu hai luồng cùng chèn,
 * một luồng sẽ vi phạm ràng buộc khi commit và được xử lý lại (message redelivery), lần sau sẽ
 * thấy đã processed và bỏ qua.
 */
@Slf4j
@Component
public class EventDeduplicator {

  @PersistenceContext
  private EntityManager entityManager;

  /**
   * Đánh dấu event đã được xử lý bởi consumer.
   *
   * @return {@code true} nếu đây là lần ĐẦU (hãy xử lý nghiệp vụ);
   *         {@code false} nếu event đã được xử lý trước đó (hãy bỏ qua).
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public boolean markProcessed(UUID eventId, String consumer) {
    ProcessedEvent.Key key = new ProcessedEvent.Key(eventId, consumer);
    if (entityManager.find(ProcessedEvent.class, key) != null) {
      log.debug("Dedup: event {} đã được {} xử lý -> bỏ qua", eventId, consumer);
      return false;
    }
    entityManager.persist(new ProcessedEvent(eventId, consumer, Instant.now()));
    return true;
  }
}
