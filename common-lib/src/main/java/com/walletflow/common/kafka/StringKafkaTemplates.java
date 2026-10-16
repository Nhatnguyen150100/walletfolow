package com.walletflow.common.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Tạo {@link KafkaTemplate} gửi chuỗi thô, với serializer được ẤN ĐỊNH TRONG CODE.
 *
 * <p><b>Vì sao không dùng bean {@code kafkaTemplate} mặc định của Spring Boot:</b> với outbox và
 * DLT, serializer là ĐIỀU KIỆN SỐNG của cơ chế chứ không phải một tuỳ chọn cấu hình. Nếu để nó
 * trong YAML thì nó có thể bị quên, bị override, hoặc không được nạp khi chạy test — mà hậu quả
 * thì vô hình: cả {@code OutboxRelay} lẫn bộ ghi DLT đều bắt mọi exception để thử lại, nên một
 * serializer sai sẽ khiến message KHÔNG BAO GIỜ gửi được trong khi hệ thống chỉ lặng lẽ ghi log.
 *
 * <p>Dùng {@code StringSerializer} cho cả key và value vì ở cả hai chỗ, giá trị cần gửi ĐÃ là một
 * chuỗi JSON hoàn chỉnh (payload outbox, hoặc nguyên văn message gốc khi chuyển sang DLT).
 * Serialize lại bằng {@code JsonSerializer} sẽ bọc thêm một lớp nữa — chuỗi JSON bị escape thành
 * một JSON string, và message trên DLT không còn replay được.
 *
 * <p>Các thiết lập producer khác (acks, retries, bootstrap-servers…) vẫn lấy từ
 * {@code spring.kafka.*} như bình thường.
 */
public final class StringKafkaTemplates {

  private StringKafkaTemplates() {
  }

  public static KafkaTemplate<String, String> create(
      KafkaProperties kafkaProperties, SslBundles sslBundles) {
    Map<String, Object> config = new HashMap<>(kafkaProperties.buildProducerProperties(sslBundles));
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
  }
}
