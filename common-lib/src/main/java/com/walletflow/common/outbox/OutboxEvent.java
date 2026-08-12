package com.walletflow.common.outbox;

import com.walletflow.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Bản ghi OUTBOX — trái tim của Transactional Outbox pattern (giải bài toán P1: dual-write).
 *
 * <p>Thay vì gửi thẳng message lên Kafka trong giao dịch nghiệp vụ (dễ mất message nếu commit DB
 * xong nhưng gửi Kafka lỗi), ta GHI message vào bảng này <b>trong cùng transaction</b> với dữ liệu
 * nghiệp vụ. Một tiến trình nền ({@link OutboxRelay}) đọc các bản ghi chưa gửi rồi publish lên Kafka.
 *
 * <p>Đánh đổi: giao hàng at-least-once (relay có thể gửi lặp khi crash) → consumer BẮT BUỘC
 * idempotent (dùng {@link com.walletflow.common.dedup.EventDeduplicator}).
 */
@Entity
@Table(name = "outbox_event", indexes = {
    @Index(name = "idx_outbox_unpublished", columnList = "published, createdAt")
})
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent extends BaseEntity {

  /** Topic Kafka đích. */
  @Column(nullable = false)
  private String topic;

  /** Key Kafka (thường là walletId/accountId) — giữ thứ tự message theo từng ví. */
  @Column(name = "msg_key")
  private String msgKey;

  /** Tên class đầy đủ của payload, để relay deserialize lại đúng kiểu khi publish. */
  @Column(nullable = false)
  private String eventType;

  /** Payload đã serialize sang JSON. */
  @Column(nullable = false, columnDefinition = "text")
  private String payload;

  /** false = chờ gửi; true = đã publish lên Kafka. */
  @Column(nullable = false)
  @Builder.Default
  private boolean published = false;

  private Instant publishedAt;
}
