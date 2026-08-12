package com.walletflow.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.common.outbox.OutboxRelay;
import com.walletflow.common.outbox.readers.JpaOutboxReader;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

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
@AutoConfiguration(after = HibernateJpaAutoConfiguration.class)
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
      KafkaTemplate<String, Object> kafkaTemplate,
      ObjectMapper objectMapper,
      JpaOutboxReader jpaOutboxReader) {
    return new OutboxRelay(kafkaTemplate, objectMapper, jpaOutboxReader);
  }
}
