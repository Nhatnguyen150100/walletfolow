package com.walletflow.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.kafka.StringKafkaTemplates;
import com.walletflow.common.outbox.OutboxCleanupJob;
import com.walletflow.common.outbox.OutboxMetrics;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.common.outbox.OutboxRelay;
import com.walletflow.common.outbox.readers.JpaOutboxReader;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Duration;

/**
 * Tự động bật cơ chế Outbox CHỈ KHI service vừa có {@link KafkaTemplate} (publish Kafka) vừa có
 * {@link EntityManagerFactory} (JPA để lưu outbox). Nhờ vậy service không dùng Kafka (auth) tự
 * động KHÔNG nạp outbox — không cần cấu hình gì thêm.
 *
 * <p>Class này nằm NGOÀI package được component-scan của các service, chỉ nạp qua cơ chế
 * auto-configuration (file AutoConfiguration.imports) để các điều kiện {@code @ConditionalOnBean}
 * được đánh giá đúng thứ tự.
 *
 * <p>Service dùng outbox chỉ cần khai {@code @EntityScan("com.walletflow")} để JPA nạp được entity
 * {@code OutboxEvent} (ở common-lib, khác package gốc của service).
 */
@AutoConfiguration(after = {HibernateJpaAutoConfiguration.class, KafkaAutoConfiguration.class})
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnBean(EntityManagerFactory.class)
@EnableScheduling
public class OutboxAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public OutboxPublisher outboxPublisher(ObjectMapper objectMapper) {
    return new OutboxPublisher(objectMapper);
  }

  @Bean
  @ConditionalOnMissingBean
  public OutboxRelay outboxRelay(
      KafkaProperties kafkaProperties,
      ObjectProvider<SslBundles> sslBundles,
      ObjectProvider<MeterRegistry> meterRegistry,
      JpaOutboxReader jpaOutboxReader) {
    // Template dùng riêng cho relay, serializer ấn định trong code — xem StringKafkaTemplates về
    // lý do không dùng bean kafkaTemplate mặc định. Cố tình KHÔNG khai làm @Bean: thêm một bean
    // cùng kiểu KafkaTemplate sẽ gây nhập nhằng khi service khác inject theo kiểu.
    return new OutboxRelay(
        StringKafkaTemplates.create(kafkaProperties, sslBundles.getIfAvailable()),
        jpaOutboxReader,
        meterRegistry.getIfAvailable());
  }

  /**
   * Chỉ tạo khi service có registry metric (tức có actuator). Không có thì outbox vẫn chạy bình
   * thường, chỉ là không quan sát được.
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(MeterRegistry.class)
  public OutboxMetrics outboxMetrics(MeterRegistry meterRegistry, JpaOutboxReader jpaOutboxReader) {
    return new OutboxMetrics(meterRegistry, jpaOutboxReader);
  }

  /**
   * Tắt được bằng {@code outbox.cleanup.enabled=false} — hữu ích khi cần giữ toàn bộ lịch sử
   * outbox để điều tra một sự cố.
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnProperty(prefix = "outbox.cleanup", name = "enabled",
      havingValue = "true", matchIfMissing = true)
  public OutboxCleanupJob outboxCleanupJob(
      JpaOutboxReader jpaOutboxReader,
      ObjectProvider<MeterRegistry> meterRegistry,
      @Value("${outbox.cleanup.retention-days:7}") int retentionDays,
      @Value("${outbox.cleanup.batch-size:1000}") int batchSize) {
    return new OutboxCleanupJob(
        jpaOutboxReader, Duration.ofDays(retentionDays), batchSize, meterRegistry.getIfAvailable());
  }
}
