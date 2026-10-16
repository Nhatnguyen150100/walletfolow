package com.walletflow.autoconfigure;

import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.kafka.PoisonMessageException;
import com.walletflow.common.kafka.StringKafkaTemplates;
import com.walletflow.common.kafka.WalletflowKafkaProperties;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;

/**
 * Xử lý lỗi cho MỌI Kafka listener: thử lại có backoff, rồi đẩy sang dead-letter topic
 * (plan §12, bài toán P9).
 *
 * <p><b>Đây là thứ bịt một lỗ mất tiền, không phải một tiện ích vận hành.</b> Mặc định của Spring
 * Kafka là {@code DefaultErrorHandler} với {@code FixedBackOff(0, 9)}: thử 10 lần liên tiếp không
 * nghỉ, rồi <b>ghi log và commit offset</b>. Nghĩa là một {@code PostJournalCommand} gặp sự cố kỹ
 * thuật kéo dài hơn mười lần thử — chỉ cần Postgres restart vài giây — sẽ <b>biến mất vĩnh viễn</b>:
 * giao dịch nằm lại ở {@code PENDING} mãi mãi, không ai biết, và không có gì để replay. Không có
 * cấu hình này thì câu "message lỗi đi vào DLT" trong javadoc của các listener là một lời hứa suông.
 *
 * <p><b>Hai nhóm lỗi, hai cách đối xử:</b>
 * <ul>
 *   <li><b>Lỗi tạm thời</b> (mất kết nối DB, rebalance, vi phạm UNIQUE do đua) → backoff tăng dần
 *       rồi thử lại. Phần lớn tự khỏi.</li>
 *   <li><b>Poison message</b> (JSON hỏng, sai kiểu) → vào DLT NGAY lần đầu. Thử lại không chỉ vô
 *       ích mà còn có hại: nó chặn cả partition, làm giao dịch của những ví khác phải chờ.</li>
 * </ul>
 *
 * <p>Lưu ý về phân công trách nhiệm: <b>từ chối nghiệp vụ</b> (không đủ số dư, bút toán lệch) KHÔNG
 * đi qua đây. Listener tự bắt {@code AppException} và phát event từ chối — xem
 * {@code LedgerCommandListener}. Nhầm hai thứ đó với nhau sẽ biến một lần hết tiền thành một message
 * nằm ở DLT chờ người xử lý.
 */
@Slf4j
@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnClass({KafkaTemplate.class, MeterRegistry.class})
@EnableConfigurationProperties(WalletflowKafkaProperties.class)
public class KafkaErrorHandlingAutoConfiguration {

  /**
   * Bean {@code DefaultErrorHandler} này được Spring Boot tự gắn vào container factory mặc định
   * ({@code ConcurrentKafkaListenerContainerFactoryConfigurer} nhận {@code CommonErrorHandler} qua
   * {@code ObjectProvider}), nên mọi {@code @KafkaListener} đều được bảo vệ mà không phải khai báo
   * thêm gì ở từng service.
   */
  @Bean
  @ConditionalOnMissingBean(DefaultErrorHandler.class)
  public DefaultErrorHandler kafkaErrorHandler(
      KafkaProperties kafkaProperties,
      ObjectProvider<SslBundles> sslBundles,
      ObjectProvider<MeterRegistry> meterRegistry,
      WalletflowKafkaProperties properties) {

    KafkaTemplate<String, String> dltTemplate =
        StringKafkaTemplates.create(kafkaProperties, sslBundles.getIfAvailable());

    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(dltTemplate, (record, exception) ->
            // partition = -1: để partitioner tự chọn theo key thay vì ép đúng số partition của
            // topic gốc. Giữ nguyên số partition sẽ vỡ nếu topic DLT được tạo với ít partition hơn,
            // và vì message vẫn mang nguyên key (walletId) nên thứ tự theo ví vẫn được bảo toàn.
            new TopicPartition(record.topic() + WalletTopics.DLT_SUFFIX, -1));

    WalletflowKafkaProperties.Retry retry = properties.getRetry();
    ExponentialBackOffWithMaxRetries backOff =
        new ExponentialBackOffWithMaxRetries(retry.getMaxAttempts());
    backOff.setInitialInterval(retry.getInitialIntervalMs());
    backOff.setMultiplier(retry.getMultiplier());
    backOff.setMaxInterval(retry.getMaxIntervalMs());

    DefaultErrorHandler handler =
        new DefaultErrorHandler(countThenRecover(recoverer, meterRegistry), backOff);

    // Những lỗi KHÔNG BAO GIỜ tự khỏi khi thử lại -> đi thẳng DLT, không chặn partition.
    handler.addNotRetryableExceptions(
        PoisonMessageException.class,
        DeserializationException.class,
        RecordDeserializationException.class,
        MessageConversionException.class);

    handler.setRetryListeners((record, exception, deliveryAttempt) ->
        log.warn("Kafka: thử lại lần {} cho {}-{}@{} vì {}",
            deliveryAttempt, record.topic(), record.partition(), record.offset(),
            exception.getClass().getSimpleName()));

    return handler;
  }

  /**
   * Bọc recoverer để đếm metric và ghi log TRƯỚC khi chuyển message sang DLT.
   *
   * <p>Metric {@code walletflow.dlt.messages} là tín hiệu cảnh báo quan trọng nhất của tầng
   * messaging: DLT có message nghĩa là có lệnh tiền chưa được xử lý và đang chờ người. Alert
   * {@code DltNotEmpty} (xem {@code deploy/prometheus/alerts.yml}) dựa trên chính counter này.
   *
   * <p>Bọc bằng lambda chứ không kế thừa {@link DeadLetterPublishingRecoverer}: lớp đó không lộ
   * template/destination-resolver ra ngoài nên không dựng lại được một instance tương đương.
   */
  private static ConsumerRecordRecoverer countThenRecover(
      DeadLetterPublishingRecoverer delegate, ObjectProvider<MeterRegistry> meterRegistry) {

    return (record, exception) -> {
      MeterRegistry registry = meterRegistry.getIfAvailable();
      if (registry != null) {
        registry.counter("walletflow.dlt.messages",
            "topic", record.topic(),
            "exception", rootCause(exception).getClass().getSimpleName()).increment();
      }
      log.error("Kafka: ĐẨY SANG DLT {}{} — offset {} của {}-{}. Cần người xem: lệnh này CHƯA được xử lý.",
          record.topic(), WalletTopics.DLT_SUFFIX, record.offset(),
          record.topic(), record.partition(), exception);
      delegate.accept(record, exception);
    };
  }

  private static Throwable rootCause(Throwable throwable) {
    Throwable current = throwable;
    while (current.getCause() != null && current.getCause() != current) {
      current = current.getCause();
    }
    return current;
  }
}
