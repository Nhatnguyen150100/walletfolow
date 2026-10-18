package com.walletflow.bank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.services.PendingChargeReconciler;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.BankFixtures;
import com.walletflow.bank.support.BankKafka;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.WalletTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Luồng thu tiền đi trọn qua Kafka thật: lệnh {@code txn.command.bank-charge.v1} → bank-adapter → event
 * {@code bank.event.*} (qua outbox + relay), và ba đường lỗi được PHÂN LOẠI đúng (CLAUDE.md, bất biến §5.7):
 * <ul>
 *   <li>provider từ chối = câu trả lời nghiệp vụ → event {@code failed};</li>
 *   <li>lệnh hỏng = bug producer → DLT ngay, không đụng dữ liệu;</li>
 *   <li>lỗi kỹ thuật của provider = KHÔNG BAO GIỜ thành {@code failed} → retry rồi DLT, hàng giữ ở NEW.</li>
 * </ul>
 * Test đóng vai transaction-service (gửi lệnh) và các consumer phía sau (đọc event, kiểm theo JSON Schema).
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "outbox.relay.delay-ms=300",
        "walletflow.bank.provider=mock",
        // Mock cần xác nhận thứ hai (PaymentProviderConfig) — chỉ stack dev/test bật
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        // Gọi reconcile() trực tiếp; recheck PT0S để hàng PENDING tới hạn ngay
        "walletflow.bank.status-check.interval-ms=3600000",
        "walletflow.bank.status-check.recheck-after=PT0S",
        "walletflow.bank.resilience.timeout=1s",
        // Rút ngắn retry: 1 lần giao + 2 lần thử lại rồi vào DLT, tổng < 1 giây
        "walletflow.kafka.retry.max-attempts=2",
        "walletflow.kafka.retry.initial-interval-ms=200",
        "walletflow.kafka.retry.max-interval-ms=400"
})
@Testcontainers
class ChargeFlowIT {

    private static final String CMD_TOPIC = WalletTopics.CMD_BANK_CHARGE;
    private static final String DLT_TOPIC = WalletTopics.CMD_BANK_CHARGE + WalletTopics.DLT_SUFFIX;

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("railsdb");

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
    PendingChargeReconciler reconciler;
    @Autowired
    MockProvider mock;
    @Autowired
    ResilientPaymentProvider resilientProvider;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    JdbcTemplate jdbcTemplate;

    BankDb db;
    BankKafka bus;
    UUID tenant;

    @BeforeEach
    void reset() {
        db = new BankDb(jdbcTemplate);
        bus = new BankKafka(kafka.getBootstrapServers());
        db.clean();
        mock.reset();
        resilientProvider.circuitBreaker().reset();
        tenant = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Đường thành công
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Lệnh thu → event charge-pending đúng schema, key = walletId, có paymentUrl; hàng PENDING")
    void lenh_thu_ra_event_charge_pending() throws Exception {
        BankChargeCommand command = BankFixtures.freshCharge(120_000);

        sendCommand(command);

        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_CHARGE_PENDING,
                command.transactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_CHARGE_PENDING, record.value());
        assertThat(record.key()).as("cùng partition với mọi message khác của ví").isEqualTo(command.walletId().toString());

        JsonNode envelope = objectMapper.readTree(record.value());
        assertThat(envelope.path("tenantId").asText()).isEqualTo(tenant.toString());
        JsonNode payload = envelope.path("payload");
        assertThat(payload.path("transactionId").asText()).isEqualTo(command.transactionId().toString());
        assertThat(payload.path("walletId").asText()).isEqualTo(command.walletId().toString());
        assertThat(payload.path("provider").asText()).isEqualTo("mock");
        assertThat(payload.path("providerRef").asText()).startsWith("MOCK-");
        assertThat(payload.path("paymentUrl").asText())
                .as("người dùng được chuyển hướng tới URL này để trả tiền")
                .endsWith(payload.path("providerRef").asText())
                .startsWith("http");

        assertThat(db.status(tenant, command.transactionId())).isEqualTo("PENDING");
        assertThat(mock.sessionCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Người dùng trả tiền (SUCCEEDED) + job đối chiếu → event completed đúng schema, mang đủ số tiền và loại tiền")
    void thanh_toan_thanh_cong_ra_event_completed() throws Exception {
        BankChargeCommand command = BankFixtures.freshCharge(250_000);
        sendCommand(command);
        String providerRef = awaitPendingRef(command);

        mock.settle(providerRef, EProviderState.SUCCEEDED);
        assertThat(reconciler.reconcile()).isEqualTo(1);

        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_COMPLETED,
                command.transactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_COMPLETED, record.value());
        assertThat(record.key()).isEqualTo(command.walletId().toString());
        JsonNode envelope = objectMapper.readTree(record.value());
        assertThat(envelope.path("tenantId").asText()).isEqualTo(tenant.toString());
        JsonNode payload = envelope.path("payload");
        assertThat(payload.path("operation").asText()).isEqualTo("CHARGE");
        assertThat(payload.path("amountMinor").asLong()).isEqualTo(250_000L);
        assertThat(payload.path("currency").asText()).isEqualTo("VND");
        assertThat(payload.path("providerRef").asText()).isEqualTo(providerRef);
        assertThat(payload.path("walletId").asText()).isEqualTo(command.walletId().toString());
        assertThat(db.status(tenant, command.transactionId())).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("Phiên hỏng (FAILED) + job đối chiếu → event failed với PAYMENT_FAILED và providerRef")
    void thanh_toan_hong_ra_event_failed() throws Exception {
        BankChargeCommand command = BankFixtures.freshCharge(75_000);
        sendCommand(command);
        String providerRef = awaitPendingRef(command);

        mock.settle(providerRef, EProviderState.FAILED);
        assertThat(reconciler.reconcile()).isEqualTo(1);

        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_FAILED,
                command.transactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_FAILED, record.value());
        JsonNode payload = objectMapper.readTree(record.value()).path("payload");
        assertThat(payload.path("failureCode").asText()).isEqualTo("PAYMENT_FAILED");
        assertThat(payload.path("providerRef").asText()).isEqualTo(providerRef);
        assertThat(payload.path("operation").asText()).isEqualTo("CHARGE");
        assertThat(db.status(tenant, command.transactionId())).isEqualTo("FAILED");
        assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_COMPLETED))
                .as("thất bại không bao giờ kèm 'completed'")
                .isEmpty();
    }

    @Test
    @DisplayName("Provider từ chối ngay (quá hạn mức): event failed với providerRef null, không có phiên nào ở provider")
    void provider_tu_choi_ra_event_failed_khong_co_phien() throws Exception {
        BankChargeCommand command = BankFixtures.freshCharge(5_000_000);
        mock.setDeclineAboveMinor(1_000_000);

        sendCommand(command);

        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_FAILED,
                command.transactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_FAILED, record.value());
        JsonNode payload = objectMapper.readTree(record.value()).path("payload");
        assertThat(payload.path("failureCode").asText()).isEqualTo("AMOUNT_LIMIT_EXCEEDED");
        assertThat(payload.hasNonNull("providerRef")).as("chưa từng có phiên -> providerRef null").isFalse();
        assertThat(mock.sessionCount()).isZero();
        assertThat(db.status(tenant, command.transactionId())).isEqualTo("FAILED");
        assertThat(db.providerRef(tenant, command.transactionId())).isNull();
    }

    // ------------------------------------------------------------------
    // Lệnh hỏng -> DLT ngay, không đụng dữ liệu
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Số tiền không dương: lệnh vào DLT nguyên văn (không retry), không tạo hàng, không gọi provider; lệnh hợp lệ sau đó vẫn chạy")
    void lenh_so_tien_khong_duong_vao_dlt_va_khong_chan_partition() {
        UUID walletId = UUID.randomUUID();
        BankChargeCommand invalid = BankFixtures.charge(UUID.randomUUID(), walletId, 0);
        BankChargeCommand valid = BankFixtures.charge(UUID.randomUUID(), walletId, 10_000);
        String invalidJson = BankFixtures.envelopeJson(objectMapper, tenant, invalid);

        // Cùng key (ví) -> cùng partition: lệnh hợp lệ nằm SAU lệnh hỏng
        bus.send(CMD_TOPIC, walletId.toString(), invalidJson);
        sendCommand(valid);

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, invalid.transactionId().toString());
        assertThat(dead.value()).as("DLT giữ nguyên văn để replay được").isEqualTo(invalidJson);
        assertThat(dead.key()).isEqualTo(walletId.toString());
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).contains("PoisonMessageException");
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("BANK_CHARGE_INVALID");

        assertThat(db.rowCountAnyTenant(invalid.transactionId())).isZero();
        assertThat(db.outboxCountAnyTopic(invalid.transactionId())).isZero();

        // Lệnh hợp lệ phía sau vẫn được xử lý -> lệnh hỏng không chặn partition
        bus.await(WalletTopics.EVT_BANK_CHARGE_PENDING, valid.transactionId().toString());
        assertThat(mock.sessionCount()).as("chỉ lệnh hợp lệ tạo phiên").isEqualTo(1);
        assertThat(mock.chargeCalls()).isEqualTo(1);
    }

    @Test
    @DisplayName("Cùng giao dịch, khác số tiền: lệnh thứ hai vào DLT với BANK_CHARGE_MISMATCH; hàng cũ và phiên provider không đổi")
    void lenh_gui_lai_khac_so_tien_vao_dlt_hang_cu_khong_doi() {
        BankChargeCommand original = BankFixtures.freshCharge(40_000);
        sendCommand(original);
        awaitPendingRef(original);
        String refBefore = db.providerRef(tenant, original.transactionId());

        BankChargeCommand mismatched = BankFixtures.charge(original.transactionId(), original.walletId(), 99_000);
        String mismatchedJson = BankFixtures.envelopeJson(objectMapper, tenant, mismatched);
        bus.send(CMD_TOPIC, original.walletId().toString(), mismatchedJson);

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, original.transactionId().toString());
        assertThat(dead.value()).isEqualTo(mismatchedJson);
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("BANK_CHARGE_MISMATCH");

        assertThat(db.row(tenant, original.transactionId()))
                .containsEntry("amount_minor", 40_000L)
                .containsEntry("status", "PENDING")
                .containsEntry("provider_ref", refBefore);
        assertThat(mock.chargeCalls()).isEqualTo(1);
        assertThat(mock.sessionCount()).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Lỗi kỹ thuật -> retry -> DLT, tuyệt đối không thành 'failed'
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Provider hỏng kéo dài: retry rồi DLT, hàng giữ ở NEW, KHÔNG có bank.event.failed; replay sau khi provider hồi → một phiên")
    void provider_hong_keo_dai_vao_dlt_khong_phat_failed() {
        BankChargeCommand command = BankFixtures.freshCharge(60_000);
        mock.failNextCalls(1_000);

        sendCommand(command);

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, command.transactionId().toString());
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .as("lỗi kỹ thuật chứ không phải PoisonMessageException")
                .contains("ProviderUnavailableException");
        assertThat(mock.chargeCalls()).as("được thử lại trước khi vào DLT").isGreaterThan(1);

        assertThat(db.status(tenant, command.transactionId()))
                .as("timeout/lỗi provider ≠ thất bại: không biết provider đã nhận chưa")
                .isEqualTo("NEW");
        assertThat(db.providerRef(tenant, command.transactionId())).isNull();
        assertThat(db.outboxCountAnyTopic(command.transactionId()))
                .as("không event nào được phát — nhất là không phải bank.event.failed")
                .isZero();
        assertThat(mock.sessionCount()).isZero();

        // Provider hồi lại, người vận hành replay lệnh từ DLT: dùng lại đúng hàng NEW
        mock.reset();
        resilientProvider.circuitBreaker().reset();
        bus.send(CMD_TOPIC, command.walletId().toString(),
                BankFixtures.envelopeJson(objectMapper, tenant, command));

        bus.await(WalletTopics.EVT_BANK_CHARGE_PENDING, command.transactionId().toString());
        assertThat(db.rowCount(tenant, command.transactionId())).isEqualTo(1);
        assertThat(db.status(tenant, command.transactionId())).isEqualTo("PENDING");
        assertThat(mock.sessionCount()).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private void sendCommand(BankChargeCommand command) {
        bus.send(CMD_TOPIC, command.walletId().toString(),
                BankFixtures.envelopeJson(objectMapper, tenant, command));
    }

    /** Chờ event pending của lệnh rồi trả providerRef trong đó (chứng tỏ hàng đã sang PENDING). */
    private String awaitPendingRef(BankChargeCommand command) {
        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_CHARGE_PENDING,
                command.transactionId().toString());
        try {
            return objectMapper.readTree(record.value()).path("payload").path("providerRef").asText();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
