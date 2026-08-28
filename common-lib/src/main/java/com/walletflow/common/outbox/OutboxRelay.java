package com.walletflow.common.outbox;

import com.walletflow.common.outbox.interfaces.OutboxReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Tiến trình nền (message relay) của Outbox pattern: định kỳ quét các bản ghi {@link OutboxEvent}
 * chưa gửi, publish lên Kafka rồi đánh dấu đã gửi (plan §7.1).
 *
 * <p>Chỉ đánh dấu {@code published=true} SAU KHI Kafka xác nhận ({@code .get()}). Nếu publish lỗi
 * → để nguyên, lần quét sau thử lại (at-least-once → consumer phải idempotent, plan §7.5).
 *
 * <p><b>Relay cố tình "mù" với nghiệp vụ:</b> nó gửi đúng chuỗi JSON đã lưu trong
 * {@code payload}, không deserialize về kiểu Java rồi serialize lại. Điều này quan trọng hơn vẻ
 * ngoài của nó:
 * <ul>
 *   <li>Không có {@code Class.forName}, nên đổi tên/di chuyển một class event không biến các hàng
 *       outbox đang chờ thành rác không thể gửi mãi mãi.</li>
 *   <li>Byte trên Kafka trùng khít nội dung trong DB → đối soát và replay được.</li>
 *   <li>Message không bị serialize hai lần (mỗi lần là một cơ hội sai khác).</li>
 * </ul>
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxRelay {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final OutboxReader outboxReader;

  @Scheduled(fixedDelayString = "${outbox.relay.delay-ms:2000}")
  @Transactional
  public void publishPending() {
    List<OutboxEvent> batch = outboxReader.getBatch();
    if (batch.isEmpty()) {
      return;
    }
    for (OutboxEvent event : batch) {
      try {
        // Gửi đồng bộ + chờ xác nhận trước khi đánh dấu đã gửi
        kafkaTemplate.send(event.getTopic(), event.getMsgKey(), event.getPayload()).get();
        event.setPublished(true);
        event.setPublishedAt(Instant.now());
        log.debug("Outbox relay: đã publish {} ({}) -> {}",
            event.getEventId(), event.getEventType(), event.getTopic());
      } catch (Exception e) {
        // Không đánh dấu -> giữ lại để lần sau thử lại (consumer idempotent)
        log.error("Outbox relay: publish eventId={} thất bại, sẽ thử lại", event.getEventId(), e);
      }
    }
  }
}
