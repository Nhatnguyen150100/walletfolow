package com.walletflow.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.services.TransferService;
import com.walletflow.transaction.support.FakeWalletDirectory;
import com.walletflow.transaction.support.FakeWalletDirectoryConfig;
import com.walletflow.transaction.support.SagaFixture;
import com.walletflow.transaction.support.SagaFixture.Accepted;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Saga đi trọn qua Kafka thật. Test đóng vai ledger: đọc lệnh trên topic lệnh, trả lời trên topic
 * kết quả. Thứ được kiểm là mối nối — outbox relay, listener, tenant trong envelope, event kết quả —
 * và ca khó nhất: câu trả lời đầu tiên BỊ MẤT.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=300",
        // Hạn chót ngắn + quét dày để ca "câu trả lời bị mất" chạy trong vài giây
        "walletflow.transaction.saga-deadline=PT2S",
        "walletflow.transaction.saga-scan.interval-ms=500",
        "walletflow.transaction.saga-scan.max-attempts=5",
        "management.tracing.enabled=false"
})
@Import(FakeWalletDirectoryConfig.class)
@Testcontainers
class SagaFlowIT {

    private static final UUID TENANT = TenantContext.DEFAULT_TENANT;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("txndb");

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
    TransferService transferService;
    @Autowired
    FakeWalletDirectory wallets;
    @Autowired
    EnvelopeReader envelopeReader;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        wallets.reset();
    }

    @Test
    @DisplayName("Lệnh ra Kafka → ledger ghi sổ → COMPLETED → TransactionCompleted ra Kafka, đúng schema")
    void vong_thanh_cong() {
        Accepted txn = SagaFixture.accept(transferService, wallets, TENANT, 12_000);
        EventEnvelope<PostJournalCommand> command = awaitCommands(txn, 1).get(0);

        reply(WalletTopics.EVT_LEDGER_POSTED, txn, SagaFixture.posted(txn));

        await(() -> "COMPLETED".equals(status(txn)));
        String completed = awaitRaw(WalletTopics.EVT_TXN_COMPLETED, txn.transactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_TXN_COMPLETED, completed);
        assertThat(command.tenantId()).isEqualTo(TENANT);
    }

    @Test
    @DisplayName("Ledger từ chối → FAILED → TransactionFailed ra Kafka với mã lỗi")
    void vong_bi_tu_choi() {
        Accepted txn = SagaFixture.accept(transferService, wallets, TENANT, 99_000);
        awaitCommands(txn, 1);

        reply(WalletTopics.EVT_LEDGER_REJECTED, txn, SagaFixture.rejected(txn, "INSUFFICIENT_FUNDS"));

        await(() -> "FAILED".equals(status(txn)));
        String failed = awaitRaw(WalletTopics.EVT_TXN_FAILED, txn.transactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_TXN_FAILED, failed);
        assertThat(failed).contains("INSUFFICIENT_FUNDS");
    }

    @Test
    @DisplayName("⭐ Câu trả lời đầu tiên BỊ MẤT: job quét gửi lại (eventId mới, cùng transactionId), trả lời lần sau → COMPLETED")
    void cau_tra_loi_bi_mat_van_hoan_tat() {
        Accepted txn = SagaFixture.accept(transferService, wallets, TENANT, 7_000);

        // Lệnh đầu tới ledger nhưng câu trả lời không bao giờ tới (ledger chết sau khi ghi sổ, event
        // vào DLT...). Không trả lời — chờ job quét gửi lại.
        List<EventEnvelope<PostJournalCommand>> commands = awaitCommands(txn, 2);

        assertThat(commands.get(1).eventId())
                .as("gửi lại đúng eventId cũ: ledger bỏ qua MÀ KHÔNG phát lại kết quả -> treo vĩnh viễn")
                .isNotEqualTo(commands.get(0).eventId());
        assertThat(commands.get(1).payload().transactionId())
                .as("cùng transactionId: ledger trả lại bút toán cũ nếu lệnh đầu đã được ghi")
                .isEqualTo(txn.transactionId());

        reply(WalletTopics.EVT_LEDGER_POSTED, txn, SagaFixture.posted(txn));

        await(() -> "COMPLETED".equals(status(txn)));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private void reply(String topic, Accepted txn, Object payload) {
        UUID eventId = payload instanceof LedgerPostedEvent posted ? posted.entryId() : UUID.randomUUID();
        EventEnvelope<Object> envelope = EventEnvelope.of(eventId,
                payload instanceof LedgerPostedEvent ? "LedgerPosted" : "LedgerRejected",
                txn.tenantId(), null, payload, Instant.now());
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(topic, txn.from().toString(),
                    objectMapper.writeValueAsString(envelope))).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private List<EventEnvelope<PostJournalCommand>> awaitCommands(Accepted txn, int count) {
        List<EventEnvelope<PostJournalCommand>> found = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of(WalletTopics.CMD_POST_JOURNAL));
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline && found.size() < count) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value().contains(txn.transactionId().toString())) {
                        EventSchemas.assertValid(WalletTopics.CMD_POST_JOURNAL, record.value());
                        found.add(envelopeReader.read(record.value(), PostJournalCommand.class));
                    }
                }
            }
        }
        assertThat(found).as("số lệnh ghi sổ trên Kafka cho " + txn.transactionId()).hasSizeGreaterThanOrEqualTo(count);
        return found;
    }

    private String awaitRaw(String topic, String marker) {
        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value().contains(marker)) {
                        return record.value();
                    }
                }
            }
        }
        throw new AssertionError("Không thấy message chứa " + marker + " trên " + topic);
    }

    private KafkaConsumer<String, String> consumer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "saga-flow-it-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(config);
    }

    private String status(Accepted txn) {
        return jdbcTemplate.queryForObject("SELECT status FROM transactions WHERE id = ?",
                String.class, txn.transactionId());
    }

    private void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("Điều kiện không đạt trong " + TIMEOUT);
    }
}
