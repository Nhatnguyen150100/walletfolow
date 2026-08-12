package com.walletflow.common.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
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
 * chưa gửi, publish lên Kafka rồi đánh dấu đã gửi.
 *
 * <p>Chỉ đánh dấu {@code published=true} SAU KHI Kafka xác nhận ({@code .get()}). Nếu publish lỗi
 * → để nguyên, lần quét sau thử lại (at-least-once).
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxRelay {

  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final ObjectMapper objectMapper;
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
        Class<?> type = Class.forName(event.getEventType());
        Object payload = objectMapper.readValue(event.getPayload(), type);
        // Gửi đồng bộ + chờ xác nhận trước khi đánh dấu đã gửi
        kafkaTemplate.send(event.getTopic(), event.getMsgKey(), payload).get();
        event.setPublished(true);
        event.setPublishedAt(Instant.now());
        log.debug("Outbox relay: đã publish {} -> {}", event.getId(), event.getTopic());
      } catch (Exception e) {
        // Không đánh dấu -> giữ lại để lần sau thử lại (consumer idempotent)
        log.error("Outbox relay: publish {} thất bại, sẽ thử lại", event.getId(), e);
      }
    }
  }
}
