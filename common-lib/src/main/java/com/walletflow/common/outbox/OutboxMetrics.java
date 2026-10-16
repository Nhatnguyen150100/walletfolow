package com.walletflow.common.outbox;

import com.walletflow.common.outbox.interfaces.OutboxReader;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Xuất hai chỉ số sức khoẻ của outbox ra Prometheus.
 *
 * <p><b>{@code walletflow.outbox.oldest.age.seconds} là chỉ số quan trọng hơn trong hai cái.</b>
 * Số lượng hàng chờ ({@code walletflow.outbox.pending}) không phát hiện được relay chết: khi tải
 * thấp, một relay đã chết hẳn vẫn để lại con số nhỏ và trông rất bình thường. Nhưng tuổi của hàng
 * cũ nhất thì tăng tuyến tính và không bao giờ nói dối — nó là nền của alert
 * {@code OutboxStalled}.
 *
 * <p>Giá trị được làm mới theo lịch rồi lưu vào {@link AtomicLong}, chứ KHÔNG truy vấn DB trực tiếp
 * trong hàm gauge. Prometheus scrape bao nhiêu lần tuỳ nó, và một {@code count(*)} chạy theo nhịp
 * scrape của mọi replica là cách biến một chỉ số quan sát thành một nguồn tải.
 */
@Slf4j
public class OutboxMetrics {

  private final OutboxReader outboxReader;
  private final AtomicLong pending = new AtomicLong();
  private final AtomicLong oldestAgeSeconds = new AtomicLong();

  public OutboxMetrics(MeterRegistry meterRegistry, OutboxReader outboxReader) {
    this.outboxReader = outboxReader;

    Gauge.builder("walletflow.outbox.pending", pending, AtomicLong::get)
        .description("Số event outbox đang chờ publish lên Kafka")
        .register(meterRegistry);

    Gauge.builder("walletflow.outbox.oldest.age.seconds", oldestAgeSeconds, AtomicLong::get)
        .description("Tuổi của event outbox chờ lâu nhất — tăng đều nghĩa là relay đã chết")
        .baseUnit("seconds")
        .register(meterRegistry);
  }

  @Scheduled(fixedDelayString = "${outbox.metrics.refresh-ms:10000}")
  @Transactional(readOnly = true)
  public void refresh() {
    try {
      pending.set(outboxReader.countPending());
      oldestAgeSeconds.set(outboxReader.oldestPendingCreatedAt()
          .map(oldest -> Duration.between(oldest, Instant.now()).toSeconds())
          .orElse(0L));
    } catch (Exception e) {
      // Không để lỗi đo đạc làm chết scheduler — nhưng phải ghi log, vì metric đứng yên ở giá trị
      // cũ là trạng thái dễ gây hiểu nhầm nhất.
      log.warn("Outbox metrics: không làm mới được chỉ số", e);
    }
  }
}
