package com.walletflow.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.common.outbox.OutboxRelay;
import com.walletflow.common.outbox.readers.JpaOutboxReader;
import jakarta.persistence.EntityManagerFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;

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
      JpaOutboxReader jpaOutboxReader) {
    return new OutboxRelay(
        outboxKafkaTemplate(kafkaProperties, sslBundles.getIfAvailable()), jpaOutboxReader);
  }

  /**
   * KafkaTemplate DÙNG RIÊNG cho outbox relay, với serializer được ấn định TRONG CODE.
   *
   * <p>Vì sao không dùng bean {@code kafkaTemplate} mặc định của Spring Boot: serializer là ĐIỀU
   * KIỆN SỐNG của cơ chế outbox, không phải một tuỳ chọn cấu hình. Nếu để nó trong YAML thì nó có
   * thể bị quên, bị override, hoặc (như YAML của config-server) không được nạp khi chạy test — mà
   * hậu quả thì vô hình: {@link OutboxRelay} cố tình bắt mọi exception để thử lại, nên một
   * serializer sai làm outbox KHÔNG BAO GIỜ gửi được gì trong khi hệ thống chỉ lặng lẽ ghi log.
   *
   * <p>Dùng {@code StringSerializer} cho cả key và value: {@code payload} trong bảng outbox đã là
   * chuỗi JSON hoàn chỉnh của {@link com.walletflow.common.event.EventEnvelope}. Serialize lại bằng
   * {@code JsonSerializer} sẽ bọc thêm một lớp nữa (chuỗi JSON bị escape thành một JSON string).
   * Các thiết lập producer khác (acks, retries, bootstrap-servers, ...) vẫn lấy từ
   * {@code spring.kafka.*} như bình thường.
   *
   * <p>Trả về instance thường chứ KHÔNG khai làm {@code @Bean}: thêm một bean cùng kiểu
   * {@code KafkaTemplate} sẽ gây nhập nhằng khi service khác inject theo kiểu.
   */
  private static KafkaTemplate<String, String> outboxKafkaTemplate(
      KafkaProperties kafkaProperties, SslBundles sslBundles) {
    Map<String, Object> config = new HashMap<>(kafkaProperties.buildProducerProperties(sslBundles));
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
  }
}
