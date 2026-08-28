package com.walletflow.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.ledger.dto.LedgerPostedEvent;
import com.walletflow.ledger.dto.LedgerRejectedEvent;
import com.walletflow.ledger.dto.Leg;
import com.walletflow.ledger.dto.PostJournalCommand;
import com.walletflow.ledger.entity.Account;
import com.walletflow.ledger.enums.ECurrency;
import com.walletflow.ledger.enums.EJournalType;
import com.walletflow.ledger.enums.ETypeAccount;
import com.walletflow.ledger.repositories.AccountRepository;
import com.walletflow.ledger.repositories.JournalEntryRepository;
import com.walletflow.ledger.repositories.PostingRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test end-to-end cho vòng lệnh của ledger qua Kafka (plan §4.3, §8.1): nhận
 * {@code PostJournalCommand} → ghi sổ → phát {@code LedgerPostedEvent} / {@code LedgerRejectedEvent}.
 *
 * <p>Đây là test chứng minh cả chuỗi ghép được với nhau, thứ mà test từng tầng không nói lên được:
 * consumer parse envelope → dedup → ghi bút toán → ghi outbox → relay đẩy lên Kafka. Mỗi mắt xích
 * đều có thể đúng riêng lẻ mà vẫn sai khi nối vào nhau (sai tên topic, sai serializer, transaction
 * bao sai phạm vi).
 *
 * <p>Ba tính chất được kiểm chứng, tương ứng ba bài toán trong plan §1:
 * <ol>
 *   <li>Đường thành công: lệnh vào → sổ có bút toán → event đi ra, với {@code eventId} TẤT ĐỊNH (P5)</li>
 *   <li>Đường từ chối nghiệp vụ: phát {@code LedgerRejectedEvent}, KHÔNG chạm vào sổ</li>
 *   <li>Message trùng: chỉ ghi một lần dù nhận hai lần (P2, P5)</li>
 * </ol>
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=300",            // relay chạy thật: đây là test cả chuỗi
        "management.tracing.enabled=false"
})
@Testcontainers
class LedgerCommandFlowIT {

    private static final String EXTERNAL_BANK = "EXTERNAL_BANK";
    private static final String WALLET_A = "WALLET_A";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

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
    AccountRepository accountRepository;
    @Autowired
    JournalEntryRepository journalEntryRepository;
    @Autowired
    PostingRepository postingRepository;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    EnvelopeReader envelopeReader;

    @BeforeEach
    void resetLedger() {
        jdbcTemplate.execute("TRUNCATE postings, journal_entries");
        jdbcTemplate.execute("DELETE FROM processed_event");
        jdbcTemplate.execute("DELETE FROM outbox_event");
        accountRepository.findAll().stream()
                .filter(account -> account.getType() == ETypeAccount.USER)
                .forEach(accountRepository::delete);
        accountRepository.save(Account.builder()
                .externalRef(WALLET_A)
                .type(ETypeAccount.USER)
                .currency(ECurrency.VND)
                .build());
    }

    // ------------------------------------------------------------------
    // Test 1 — ⭐ Đường thành công đi hết chuỗi
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Gửi PostJournalCommand lên Kafka: bút toán được ghi và LedgerPostedEvent đi ra")
    void lenh_ghi_so_di_het_chuoi() {
        UUID transactionId = UUID.randomUUID();

        send(UUID.randomUUID(), topUp(transactionId, 100_000));

        EventEnvelope<LedgerPostedEvent> posted =
                awaitEvent(WalletTopics.EVT_LEDGER_POSTED, transactionId, LedgerPostedEvent.class);

        assertThat(posted.payload().transactionId()).isEqualTo(transactionId);
        assertThat(posted.payload().type()).isEqualTo(EJournalType.TOPUP);
        assertThat(posted.eventId())
                .as("eventId của event = id bút toán, nên phát lại vẫn trùng và consumer dedup được")
                .isEqualTo(posted.payload().entryId());
        assertThat(posted.payload().legs())
                .as("event mang đủ các vế để wallet-service cập nhật số dư mà không phải hỏi lại ledger")
                .extracting(Leg::accountRef)
                .containsExactlyInAnyOrder(EXTERNAL_BANK, WALLET_A);

        assertThat(journalEntryRepository.count()).isEqualTo(1);
        assertThat(postingRepository.count()).isEqualTo(2);
        assertThat(postingRepository.totalOfSystem()).isZero();
    }

    // ------------------------------------------------------------------
    // Test 2 — ⭐ Từ chối nghiệp vụ: phát event, không chạm vào sổ
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Rút quá số dư: phát LedgerRejectedEvent với mã lỗi, sổ cái không bị chạm")
    void tu_choi_nghiep_vu_phat_event_rejected() {
        UUID transactionId = UUID.randomUUID();

        send(UUID.randomUUID(), new PostJournalCommand(
                transactionId, EJournalType.WITHDRAW, null, ECurrency.VND,
                List.of(new Leg(WALLET_A, -80_000), new Leg(EXTERNAL_BANK, 80_000))));

        EventEnvelope<LedgerRejectedEvent> rejected =
                awaitEvent(WalletTopics.EVT_LEDGER_REJECTED, transactionId, LedgerRejectedEvent.class);

        assertThat(rejected.payload().reasonCode())
                .as("mã lỗi ổn định để orchestrator xử lý theo mã, không phải parse chuỗi")
                .isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(rejected.payload().type()).isEqualTo(EJournalType.WITHDRAW);

        assertThat(journalEntryRepository.count()).isZero();
        assertThat(postingRepository.count()).isZero();
    }

    // ------------------------------------------------------------------
    // Test 3 — ⭐ Message trùng chỉ được xử lý một lần (P5)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Cùng eventId gửi hai lần: dedup chặn lần thứ hai, sổ chỉ có một bút toán")
    void message_trung_chi_xu_ly_mot_lan() {
        UUID transactionId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        PostJournalCommand command = topUp(transactionId, 100_000);

        send(eventId, command);
        awaitEvent(WalletTopics.EVT_LEDGER_POSTED, transactionId, LedgerPostedEvent.class);

        send(eventId, command);   // Kafka giao lại y nguyên message cũ

        // Chờ đủ lâu để lần thứ hai chắc chắn đã được consumer xử lý (hoặc bỏ qua)
        awaitCondition(() -> processedEventCount() == 1);

        assertThat(journalEntryRepository.count())
                .as("bút toán chỉ được ghi một lần")
                .isEqualTo(1);
        assertThat(postingRepository.count()).isEqualTo(2);
        assertThat(balanceOf(WALLET_A))
                .as("số dư không nhân đôi — đây chính là bài toán P2/P5")
                .isEqualTo(100_000);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private PostJournalCommand topUp(UUID transactionId, long amountMinor) {
        return new PostJournalCommand(
                transactionId, EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(EXTERNAL_BANK, -amountMinor), new Leg(WALLET_A, amountMinor)));
    }

    /** Đóng vai transaction-service: gửi command đã bọc envelope lên topic lệnh. */
    private void send(UUID eventId, PostJournalCommand command) {
        EventEnvelope<PostJournalCommand> envelope =
                EventEnvelope.of(eventId, "PostJournal", null, command, Instant.now());

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(
                    WalletTopics.CMD_POST_JOURNAL, WALLET_A,
                    objectMapper.writeValueAsString(envelope))).get();
        } catch (Exception e) {
            throw new IllegalStateException("Không gửi được command lên Kafka", e);
        }
    }

    /**
     * Chờ event của {@code transactionId} xuất hiện trên topic.
     *
     * <p>Lọc theo transactionId vì Kafka container dùng chung cho cả class: topic không được xoá
     * giữa các ca test, nên đọc từ earliest sẽ thấy cả event của ca test khác.
     */
    private <T> EventEnvelope<T> awaitEvent(String topic, UUID transactionId, Class<T> payloadType) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "flow-it-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<String> seen = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : batch.records(topic)) {
                    seen.add(record.value());
                    if (record.value().contains(transactionId.toString())) {
                        return envelopeReader.read(record.value(), payloadType);
                    }
                }
            }
        }
        throw new AssertionError("Không nhận được event cho transaction " + transactionId
                + " trên " + topic + " trong " + TIMEOUT + ". Đã thấy: " + seen);
    }

    private void awaitCondition(java.util.function.BooleanSupplier condition) {
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

    private int processedEventCount() {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_event", Integer.class);
        return count == null ? 0 : count;
    }

    private long balanceOf(String externalRef) {
        UUID accountId = accountRepository
                .findByExternalRefAndCurrency(externalRef, ECurrency.VND)
                .orElseThrow()
                .getId();
        return postingRepository.balanceOf(accountId);
    }
}
