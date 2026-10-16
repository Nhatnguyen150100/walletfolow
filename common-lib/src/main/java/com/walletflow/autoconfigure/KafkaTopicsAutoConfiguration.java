package com.walletflow.autoconfigure;

import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.kafka.WalletflowKafkaProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Khai báo TƯỜNG MINH toàn bộ topic của hệ thống, thay cho việc để broker tự tạo
 * ({@code auto.create.topics.enable}).
 *
 * <p><b>Vì sao tắt auto-create lại quan trọng:</b> khi broker tự tạo topic, một lỗi gõ sai tên
 * topic ở producer sẽ <b>không báo lỗi</b> — broker vui vẻ tạo topic mới và nhận message. Message
 * nằm yên ở đó, không consumer nào đăng ký, và hệ thống trông như đang hoạt động bình thường. Với
 * topic lệnh tiền, đó là những giao dịch lặng lẽ không bao giờ được thực thi. Khai báo tường minh
 * biến lỗi câm đó thành lỗi nổ ngay lúc gửi.
 *
 * <p>Phụ thêm: topic tự tạo lấy số partition/replication mặc định của broker, nên không kiểm soát
 * được mức song song hoá hay độ bền — hai thứ không nên là hiệu ứng phụ của cấu hình broker.
 *
 * <p><b>Ở môi trường thật nên tắt</b> ({@code walletflow.kafka.declare-topics=false}) và tạo topic
 * bằng công cụ vận hành: application không nên có quyền tạo topic, và retention/replication là
 * quyết định của hạ tầng. Mặc định bật để máy dev chỉ cần một lệnh là chạy được.
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnClass(KafkaAdmin.class)
@ConditionalOnProperty(prefix = "walletflow.kafka", name = "declare-topics",
    havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(WalletflowKafkaProperties.class)
public class KafkaTopicsAutoConfiguration {

  /** Mọi topic nghiệp vụ. Mỗi cái đều có consumer, nên mỗi cái đều cần một DLT đi kèm. */
  private static final List<String> BUSINESS_TOPICS = List.of(
      WalletTopics.CMD_POST_JOURNAL,
      WalletTopics.CMD_BANK_CHARGE,
      WalletTopics.CMD_BANK_PAYOUT,
      WalletTopics.CMD_BANK_REFUND,
      WalletTopics.CMD_OPEN_ACCOUNT,
      WalletTopics.EVT_LEDGER_POSTED,
      WalletTopics.EVT_LEDGER_REJECTED,
      WalletTopics.EVT_ACCOUNT_OPENED,
      WalletTopics.EVT_BANK_COMPLETED,
      WalletTopics.EVT_BANK_FAILED,
      WalletTopics.EVT_TXN_COMPLETED,
      WalletTopics.EVT_TXN_FAILED);

  /**
   * Message ở DLT phải chờ người xử lý, nên giữ lâu hơn hẳn topic thường. Mất một message DLT vì
   * hết retention nghĩa là mất luôn bằng chứng của một giao dịch chưa hoàn tất.
   */
  private static final long DLT_RETENTION_MS = TimeUnit.DAYS.toMillis(30);

  /**
   * Topic lệnh tiền cần giữ đủ lâu để replay được sau một sự cố kéo dài (plan §12). Mặc định của
   * Kafka cũng là 7 ngày, nhưng khai tường minh để nó không đổi theo cấu hình broker.
   */
  private static final long BUSINESS_RETENTION_MS = TimeUnit.DAYS.toMillis(7);

  @Bean
  public KafkaAdmin.NewTopics walletflowTopics(WalletflowKafkaProperties properties) {
    List<NewTopic> topics = new ArrayList<>();

    for (String topic : BUSINESS_TOPICS) {
      topics.add(TopicBuilder.name(topic)
          .partitions(properties.getPartitions())
          .replicas(properties.getReplicationFactor())
          .config("retention.ms", String.valueOf(BUSINESS_RETENTION_MS))
          .build());

      topics.add(TopicBuilder.name(topic + WalletTopics.DLT_SUFFIX)
          .partitions(properties.getPartitions())
          .replicas(properties.getReplicationFactor())
          .config("retention.ms", String.valueOf(DLT_RETENTION_MS))
          .build());
    }

    return new KafkaAdmin.NewTopics(topics.toArray(new NewTopic[0]));
  }
}
