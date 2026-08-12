package com.walletflow.common.dedup;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Bảng INBOX / dedup — giải bài toán P5 (Kafka at-least-once làm consumer nhận trùng message).
 *
 * <p>Trước khi xử lý một message, consumer thử ghi (eventId, consumer) vào bảng này TRONG CÙNG
 * transaction với việc xử lý nghiệp vụ. Nếu (eventId, consumer) đã tồn tại → message đã xử lý rồi
 * → bỏ qua. Nhờ khoá chính kép, việc xử lý trở nên idempotent một cách bền vững.
 *
 * <p>Khoá chính kép (eventId, consumer): cùng một event có thể được nhiều consumer khác nhau xử lý
 * độc lập, nên mỗi consumer có "dấu đã xử lý" riêng.
 */
@Entity
@Table(name = "processed_event")
@IdClass(ProcessedEvent.Key.class)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

  @Id
  @Column(name = "event_id", nullable = false)
  private UUID eventId;

  @Id
  @Column(name = "consumer", nullable = false, length = 64)
  private String consumer;

  @Column(name = "processed_at", nullable = false)
  private Instant processedAt;

  /** Khoá chính kép. */
  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Key implements Serializable {
    private UUID eventId;
    private String consumer;
  }
}
