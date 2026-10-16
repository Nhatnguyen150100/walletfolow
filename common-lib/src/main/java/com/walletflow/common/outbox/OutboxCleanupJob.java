package com.walletflow.common.outbox;

import com.walletflow.common.outbox.interfaces.OutboxReader;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

/**
 * Dọn các bản ghi outbox ĐÃ GỬI quá hạn giữ.
 *
 * <p>Không có job này, {@code outbox_event} chỉ có lớn lên: mỗi chuyển động tiền để lại ít nhất một
 * hàng, vĩnh viễn. Hậu quả không chỉ là tốn đĩa — chính đường ghi outbox của giao dịch đang chạy sẽ
 * chậm dần theo kích thước bảng và index.
 *
 * <p><b>Giữ lại bao lâu là một đánh đổi có thật:</b> bảng outbox là bằng chứng "message này đã được
 * phát đi" khi đối soát và khi điều tra sự cố. Mặc định 7 ngày đủ cho hầu hết cuộc điều tra, và
 * trùng với retention mặc định của topic Kafka nên hai nguồn bằng chứng hết hạn cùng lúc.
 *
 * <p>Hàng CHƯA gửi không bao giờ bị đụng tới, bất kể cũ cỡ nào — đó là message chưa đến đích.
 */
@Slf4j
public class OutboxCleanupJob {

  private final OutboxReader outboxReader;
  private final Duration retention;
  private final int batchSize;
  private final Counter deletedCounter;

  public OutboxCleanupJob(
      OutboxReader outboxReader,
      Duration retention,
      int batchSize,
      @Nullable MeterRegistry meterRegistry) {
    this.outboxReader = outboxReader;
    this.retention = retention;
    this.batchSize = batchSize;
    this.deletedCounter = meterRegistry == null ? null
        : meterRegistry.counter("walletflow.outbox.deleted");
  }

  /**
   * Chạy thưa (mặc định mỗi giờ) và xoá theo lô có giới hạn.
   *
   * <p>Cố tình KHÔNG dọn sạch trong một lần: nếu tồn đọng lớn, xoá hết trong một transaction sẽ giữ
   * khoá lâu và cạnh tranh trực tiếp với đường ghi outbox của giao dịch. Dọn dần qua nhiều vòng thì
   * chậm hơn nhưng không bao giờ cản đường đi của tiền.
   */
  @Scheduled(fixedDelayString = "${outbox.cleanup.delay-ms:3600000}")
  @Transactional
  public void deleteOldPublished() {
    Instant cutoff = Instant.now().minus(retention);
    try {
      int deleted = outboxReader.deletePublishedBefore(cutoff, batchSize);
      if (deleted > 0) {
        if (deletedCounter != null) {
          deletedCounter.increment(deleted);
        }
        log.info("Outbox cleanup: đã xoá {} event đã publish trước {}", deleted, cutoff);
      }
    } catch (Exception e) {
      log.error("Outbox cleanup: xoá thất bại, sẽ thử lại vòng sau", e);
    }
  }
}
