package com.walletflow.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.ledger.dto.Leg;
import com.walletflow.ledger.dto.PostJournalCommand;
import com.walletflow.ledger.entity.Account;
import com.walletflow.ledger.enums.ECurrency;
import com.walletflow.ledger.enums.EJournalType;
import com.walletflow.ledger.enums.ETypeAccount;
import com.walletflow.ledger.repositories.AccountRepository;
import com.walletflow.ledger.repositories.JournalEntryRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chứng minh cơ chế dead-letter topic THẬT SỰ hoạt động (plan §12, bài toán P9).
 *
 * <p><b>Vì sao test này tồn tại:</b> trước khi có
 * {@code KafkaErrorHandlingAutoConfiguration}, javadoc của {@code LedgerCommandListener} hứa rằng
 * "message lỗi sẽ vào DLT" nhưng không có cấu hình nào thực hiện lời hứa đó. Mặc định của Spring
 * Kafka là thử 10 lần rồi <b>ghi log và bỏ qua</b> — message biến mất không dấu vết. Một lời hứa về
 * độ an toàn mà không có test thì không phân biệt được với một lời hứa suông, nên đây là test tồn
 * tại để lời hứa đó không âm thầm hỏng lại.
 *
 * <p>Hai tính chất được kiểm chứng, và tính chất thứ hai mới là tính chất khó:
 * <ol>
 *   <li>Message hỏng đi vào {@code <topic>.DLT}, <b>giữ nguyên văn</b> nội dung gốc để replay được.</li>
 *   <li>Message hỏng <b>KHÔNG chặn partition</b>: message hợp lệ nằm ngay sau nó, trên cùng
 *       partition, vẫn được xử lý. Đây chính là điều mà việc khai {@code PoisonMessageException}
 *       là non-retryable mua về — nếu thử lại 5 lần với backoff thì mọi giao dịch xếp sau phải chờ.</li>
 * </ol>
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=300",
        "management.tracing.enabled=false",
        // Một partition để bài test về "không chặn partition" là thật: hai message chắc chắn
        // nằm cùng một partition và được xử lý tuần tự bởi cùng một consumer.
        "walletflow.kafka.partitions=1"
})
@Testcontainers
class KafkaDltIT {

    private static final String EXTERNAL_BANK = "EXTERNAL_BANK";
    private static final String WALLET_A = "WALLET_A";
    private static final String DLT_TOPIC = WalletTopics.CMD_POST_JOURNAL + WalletTopics.DLT_SUFFIX;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** JSON cụt — Jackson không parse được, nên EnvelopeReader ném PoisonMessageException. */
    private static final String POISON = "{\"eventId\":\"khong-phai-uuid\", \"payload\": {";

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
    JdbcTemplate jdbcTemplate;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    MeterRegistry meterRegistry;

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
    // ⭐ Message hỏng vào DLT, và KHÔNG làm kẹt message hợp lệ phía sau
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Message hỏng đi vào DLT nguyên văn và không chặn message hợp lệ cùng partition")
    void message_hong_vao_dlt_va_khong_chan_partition() {
        UUID transactionId = UUID.randomUUID();

        // Cùng msgKey -> cùng partition -> message hợp lệ thực sự nằm SAU message hỏng
        sendRaw(WALLET_A, POISON);
        sendRaw(WALLET_A, envelopeJson(UUID.randomUUID(), topUp(transactionId, 100_000)));

        awaitCondition(() -> journalEntryRepository.count() == 1,
                "message hợp lệ phía sau message hỏng phải được xử lý — nếu treo ở đây nghĩa là "
                        + "message hỏng đang bị retry và chặn cả partition");

        ConsumerRecord<String, String> dead = awaitDeadLetter();

        assertThat(dead.value())
                .as("DLT phải giữ NGUYÊN VĂN message gốc thì mới replay được sau khi sửa lỗi")
                .isEqualTo(POISON);
        assertThat(dead.key())
                .as("giữ nguyên key để message replay vẫn về đúng partition của ví đó")
                .isEqualTo(WALLET_A);
        assertThat(header(dead, KafkaHeaders.DLT_ORIGINAL_TOPIC))
                .isEqualTo(WalletTopics.CMD_POST_JOURNAL);
        // DLT_EXCEPTION_FQCN luôn là lớp bọc của Spring Kafka (ListenerExecutionFailedException);
        // nguyên nhân thật nằm ở header *_CAUSE_FQCN. Người xử lý DLT cần cái sau, không phải cái trước.
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .as("mang theo nguyên nhân gốc để người xử lý không phải đoán")
                .contains("PoisonMessageException");
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                .contains("EventEnvelope<PostJournalCommand>");

        // Metric là thứ alert DltNotEmpty dựa vào. Không assert ở đây thì một lần đổi tên metric
        // sẽ làm alert câm lặng mà mọi test vẫn xanh — đúng kiểu hỏng mà Phase 0 tồn tại để chặn.
        Counter dltCounter = meterRegistry.find("walletflow.dlt.messages")
                .tag("topic", WalletTopics.CMD_POST_JOURNAL)
                .tag("exception", "InvalidFormatException")
                .counter();
        assertThat(dltCounter)
                .as("counter walletflow.dlt.messages phải tồn tại sau khi có message vào DLT")
                .isNotNull();
        assertThat(dltCounter.count()).isGreaterThanOrEqualTo(1.0);
    }

    // ------------------------------------------------------------------
    // Tên metric trên dây phải khớp chính xác với tên dashboard/alert dùng
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Tên metric xuất ra Prometheus khớp với tên dùng trong dashboard và alert rule")
    void ten_metric_khop_voi_dashboard_va_alert() {
        // Micrometer đổi dấu chấm thành gạch dưới và thêm hậu tố _total cho counter khi xuất sang
        // Prometheus. Dashboard (deploy/grafana/dashboards/) và alert (deploy/prometheus/alerts.yml)
        // viết tay tên ở DẠNG ĐÃ ĐỔI, nên giữa code và cấu hình quan sát không có gì ràng buộc nhau
        // ngoài test này. Đổi tên metric mà quên sửa alert thì alert không báo lỗi — nó chỉ im lặng
        // không bao giờ bắn nữa.
        assertThat(meterRegistry.find("walletflow.outbox.pending").gauge())
                .as("-> walletflow_outbox_pending").isNotNull();
        assertThat(meterRegistry.find("walletflow.outbox.oldest.age.seconds").gauge())
                .as("-> walletflow_outbox_oldest_age_seconds, nền của alert OutboxStalled").isNotNull();
    }

    // ------------------------------------------------------------------
    // Sổ cái không bị chạm bởi message hỏng
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Message hỏng không để lại dấu vết nào trong sổ cái")
    void message_hong_khong_cham_vao_so_cai() {
        sendRaw(WALLET_A, POISON);
        awaitDeadLetter();

        assertThat(journalEntryRepository.count()).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_event", Integer.class))
                .as("message không parse được thì chưa có eventId để đánh dấu đã xử lý")
                .isZero();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private PostJournalCommand topUp(UUID transactionId, long amountMinor) {
        return new PostJournalCommand(
                transactionId, EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(EXTERNAL_BANK, -amountMinor), new Leg(WALLET_A, amountMinor)));
    }

    private String envelopeJson(UUID eventId, PostJournalCommand command) {
        try {
            return objectMapper.writeValueAsString(
                    EventEnvelope.of(eventId, "PostJournal", null, command, Instant.now()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void sendRaw(String key, String value) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(WalletTopics.CMD_POST_JOURNAL, key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Không gửi được message lên Kafka", e);
        }
    }

    private ConsumerRecord<String, String> awaitDeadLetter() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-it-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(DLT_TOPIC));
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : batch.records(DLT_TOPIC)) {
                    return record;
                }
            }
        }
        throw new AssertionError("Không có message nào vào " + DLT_TOPIC + " trong " + TIMEOUT
                + ". Nghĩa là message hỏng đã bị NUỐT MẤT — đúng lỗi mà test này tồn tại để chặn.");
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private void awaitCondition(java.util.function.BooleanSupplier condition, String description) {
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
        throw new AssertionError("Điều kiện không đạt trong " + TIMEOUT + ": " + description);
    }
}
