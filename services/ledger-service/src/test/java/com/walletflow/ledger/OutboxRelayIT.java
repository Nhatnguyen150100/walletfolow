package com.walletflow.ledger;

import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.common.outbox.OutboxRelay;
import com.walletflow.ledger.dto.LedgerPostedEvent;
import com.walletflow.ledger.dto.Leg;
import com.walletflow.ledger.enums.ECurrency;
import com.walletflow.ledger.enums.EJournalType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test tích hợp cho Transactional Outbox (plan §7.1, §15) — chứng minh mắt xích mà trước đây
 * KHÔNG có test nào phủ: <b>ghi outbox rồi relay có thực sự đẩy được message lên Kafka hay không</b>.
 *
 * <p>Vì sao mắt xích này nguy hiểm nếu không test: {@link OutboxRelay} cố tình bắt mọi exception để
 * thử lại ở lần quét sau (điều kiện cần của at-least-once). Nên nếu producer bị cấu hình sai
 * serializer, mọi lần {@code send()} đều thất bại nhưng hệ thống KHÔNG hề báo lỗi ra ngoài — chỉ có
 * dòng log lặp lại mãi, còn tiền thì "treo" ở bảng outbox. Đây đúng là loại lỗi mà một test xanh
 * ở tầng service không bao giờ phát hiện được.
 *
 * <p>Chạy Kafka thật (Testcontainers) chứ không dùng mock: thứ đang được kiểm chứng chính là hành vi
 * serialize/gửi/nhận thật của Kafka client.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",          // test không cần service discovery
        "spring.cloud.config.enabled=false",    // test không cần config-server
        "outbox.relay.delay-ms=3600000",        // tự tay gọi relay để test tất định, scheduler không chen vào
        "spring.kafka.listener.auto-startup=false", // test này chỉ kiểm chứng đường publish
        "management.tracing.enabled=false"      // không xuất trace khi test
})
@Testcontainers
class OutboxRelayIT {

    private static final String TOPIC = WalletTopics.EVT_LEDGER_POSTED;

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("ledgerdb");

    @SuppressWarnings("resource")
    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    OutboxPublisher outboxPublisher;
    @Autowired
    OutboxRelay outboxRelay;
    @Autowired
    TransactionTemplate transactionTemplate;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    EnvelopeReader envelopeReader;

    @BeforeEach
    void clearOutbox() {
        jdbcTemplate.execute("DELETE FROM outbox_event");
    }

    // ------------------------------------------------------------------
    // Test 1 — ⭐ Outbox thực sự tới được Kafka
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Ghi outbox rồi chạy relay: message thật sự lên Kafka và hàng được đánh dấu đã gửi")
    void relay_publish_duoc_message_len_kafka() {
        UUID transactionId = UUID.randomUUID();
        UUID entryId = UUID.randomUUID();
        String partitionKey = "WALLET_A";

        // Nghiệp vụ chỉ ghi vào outbox trong transaction của mình, không gọi Kafka
        transactionTemplate.executeWithoutResult(status ->
                outboxPublisher.publish(TOPIC, partitionKey, entryId, "LedgerPosted",
                        new LedgerPostedEvent(transactionId, entryId, EJournalType.TOPUP,
                                ECurrency.VND, List.of(new Leg("WALLET_A", 100_000)))));

        assertThat(unpublishedCount()).as("trước khi relay chạy: hàng còn ở trạng thái chờ gửi").isOne();

        outboxRelay.publishPending();

        List<ConsumerRecord<String, String>> received =
                pollTopic(partitionKey, 1, Duration.ofSeconds(30));
        assertThat(received).as("phải nhận được đúng 1 message trên " + TOPIC).hasSize(1);

        ConsumerRecord<String, String> record = received.getFirst();
        assertThat(record.key())
                .as("partition key = walletId để giữ thứ tự theo từng ví (plan §11)")
                .isEqualTo(partitionKey);
        // Thân message là envelope, không phải payload trần: eventId phải có mặt để consumer dedup
        EventEnvelope<LedgerPostedEvent> envelope =
                envelopeReader.read(record.value(), LedgerPostedEvent.class);
        assertThat(envelope.eventId())
                .as("eventId phải TẤT ĐỊNH theo bút toán, không phải UUID ngẫu nhiên")
                .isEqualTo(entryId);
        assertThat(envelope.eventType()).isEqualTo("LedgerPosted");
        assertThat(envelope.version()).isEqualTo(EventEnvelope.CURRENT_VERSION);
        assertThat(envelope.occurredAt()).isNotNull();
        assertThat(envelope.payload().transactionId()).isEqualTo(transactionId);
        assertThat(envelope.payload().currency()).isEqualTo(ECurrency.VND);

        assertThat(unpublishedCount())
                .as("chỉ được đánh dấu đã gửi SAU KHI Kafka xác nhận")
                .isZero();
    }

    // ------------------------------------------------------------------
    // Test 2 — Relay quét lại không gửi lặp
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Relay quét lần thứ hai không publish lại message đã gửi")
    void relay_khong_gui_lap_o_lan_quet_sau() {
        UUID entryId = UUID.randomUUID();
        transactionTemplate.executeWithoutResult(status ->
                outboxPublisher.publish(TOPIC, "WALLET_B", entryId, "LedgerPosted",
                        new LedgerPostedEvent(UUID.randomUUID(), entryId, EJournalType.TRANSFER,
                                ECurrency.VND, List.of(new Leg("WALLET_B", 10_000)))));

        outboxRelay.publishPending();
        outboxRelay.publishPending();   // hàng đã published=true -> không còn được đọc lại

        // stopAfter = 2 nên vòng poll không dừng sớm mà chờ hết timeout: có bản sao thì phải thấy.
        assertThat(pollTopic("WALLET_B", 2, Duration.ofSeconds(10)))
                .as("vẫn đúng 1 message, không nhân đôi")
                .hasSize(1);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private int unpublishedCount() {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE published = false", Integer.class);
        return count == null ? 0 : count;
    }

    /**
     * Đọc topic từ đầu (earliest) và chỉ giữ message có {@code key} cho trước.
     *
     * <p>Phải lọc theo key vì Kafka container dùng chung cho cả class: topic KHÔNG được xoá giữa các
     * test (khác với bảng outbox), nên đọc từ earliest sẽ thấy cả message của test khác. Mỗi test
     * dùng một key riêng để độc lập với nhau và với thứ tự chạy.
     *
     * @param stopAfter đọc đủ số này thì dừng sớm (trường hợp xanh nhanh); đặt lớn hơn số mong đợi
     *                  để buộc vòng poll chờ hết {@code timeout} — cần thiết khi muốn chứng minh
     *                  KHÔNG có message thứ hai.
     */
    private List<ConsumerRecord<String, String>> pollTopic(String key, int stopAfter, Duration timeout) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-relay-it-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Đọc chuỗi JSON thô: test này kiểm chứng "message có ra tới Kafka không",
        // không phụ thuộc vào cấu hình deserializer phía consumer.
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline && collected.size() < stopAfter) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofSeconds(1));
                batch.records(TOPIC).forEach(record -> {
                    if (key.equals(record.key())) {
                        collected.add(record);
                    }
                });
            }
        }
        return collected;
    }
}
