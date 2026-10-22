package com.walletflow.bank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.provider.ProviderUnavailableException;
import com.walletflow.bank.provider.RefundRequest;
import com.walletflow.bank.provider.RefundResult;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.services.ChargeService;
import com.walletflow.bank.services.ChargeStatusService;
import com.walletflow.bank.services.RefundService;
import com.walletflow.bank.support.Await;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.BankFixtures;
import com.walletflow.bank.support.BankKafka;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.BankRefundCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Hoàn tiền một lần thu đã thành công (roadmap 2.4b, ADR-020) — bước bù trừ của saga nạp tiền, qua Kafka thật:
 * {@code txn.command.bank-refund.v1} → bank-adapter → {@code bank.event.completed/failed} với
 * {@code operation = REFUND}.
 *
 * <p>Tính chất về tiền: <b>một lần thu, tối đa MỘT lần hoàn, và chỉ hoàn trọn đúng số đã thu</b> — dù lệnh bị
 * gửi lại, nhiều luồng đua nhau, provider lỗi giữa chừng, hay lệnh sai ví/số tiền/tenant. Hoàn hai lần là
 * trả người dùng tiền của công ty; hoàn sai số là trả cho họ thứ họ chưa từng trả.
 *
 * <p>Thước đo "đã hoàn bao nhiêu" ở phía provider: sau khi hoàn trọn, một lần hoàn mới (khoá khác) dù chỉ 1 đồng
 * phải bị mock từ chối {@code NOT_REFUNDABLE}; ngược lại nếu adapter chưa từng hoàn thì lần hoàn trọn mới
 * (khoá khác) phải thành công. Hai phép thử này ({@link #assertFullyRefundedAtProvider} /
 * {@link #assertNothingRefundedAtProvider}) cho biết trạng thái THẬT ở provider, không phải điều ta tưởng.
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
        // Gọi refresh() trực tiếp, không đua với scheduler đối chiếu
        "walletflow.bank.status-check.interval-ms=3600000",
        "walletflow.bank.status-check.recheck-after=PT30S",
        "walletflow.bank.resilience.timeout=1s",
        // Rút ngắn retry: 1 lần giao + 2 lần thử lại rồi vào DLT, tổng < 1 giây
        "walletflow.kafka.retry.max-attempts=2",
        "walletflow.kafka.retry.initial-interval-ms=200",
        "walletflow.kafka.retry.max-interval-ms=400"
})
@Testcontainers
class RefundFlowIT {

    private static final String CMD_TOPIC = WalletTopics.CMD_BANK_REFUND;
    private static final String DLT_TOPIC = WalletTopics.CMD_BANK_REFUND + WalletTopics.DLT_SUFFIX;

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
    ChargeService chargeService;
    @Autowired
    ChargeStatusService chargeStatusService;
    @Autowired
    RefundService refundService;
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
        // Đường hoàn có mạch riêng từ 2.4c — lỗi kỹ thuật của lệnh hoàn mở đúng mạch này
        resilientProvider.refundCircuitBreaker().reset();
        tenant = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Đường thành công
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Hoàn lần đầu: hàng REFUND SUCCEEDED trỏ về lần thu, mã tham chiếu là của lần hoàn, đúng MỘT event completed(REFUND) đúng schema")
    void hoan_tien_lan_thu_da_thanh_cong() throws Exception {
        BankChargeCommand charge = succeededCharge(250_000);

        sendRefund(tenant, BankFixtures.refundOf(charge));

        awaitRefundStatus(charge, "SUCCEEDED");
        Map<String, Object> chargeRow = db.row(tenant, charge.transactionId(), "CHARGE");
        Map<String, Object> refundRow = db.row(tenant, charge.transactionId(), "REFUND");
        assertThat(refundRow.get("refund_of"))
                .as("đối soát đi từ lần hoàn về đúng lần thu của nó")
                .isEqualTo(chargeRow.get("id"));
        assertThat(refundRow.get("provider_ref"))
                .as("mã tham chiếu của LẦN HOÀN, không phải của lần thu")
                .isNotEqualTo(chargeRow.get("provider_ref"));
        assertThat((String) refundRow.get("provider_ref")).startsWith("MOCK-RF-");
        assertThat(refundRow.get("amount_minor")).isEqualTo(250_000L);
        assertThat(refundRow.get("idempotency_key"))
                .as("khoá gửi provider cho lần hoàn khác khoá của lần thu")
                .isNotEqualTo(chargeRow.get("idempotency_key"));
        assertThat(refundRow.get("failure_code")).isNull();
        assertThat(chargeRow.get("status")).as("lần thu không bị đổi").isEqualTo("SUCCEEDED");

        ConsumerRecord<String, String> record = bus.awaitMatching(WalletTopics.EVT_BANK_COMPLETED,
                isEventOf(charge.transactionId(), "REFUND"), "completed REFUND của " + charge.transactionId(), 1)
                .get(0);
        EventSchemas.assertValid(WalletTopics.EVT_BANK_COMPLETED, record.value());
        assertThat(record.key()).as("cùng partition với mọi message khác của ví")
                .isEqualTo(charge.walletId().toString());
        JsonNode envelope = objectMapper.readTree(record.value());
        assertThat(envelope.path("tenantId").asText()).isEqualTo(tenant.toString());
        JsonNode payload = envelope.path("payload");
        assertThat(payload.path("operation").asText()).isEqualTo("REFUND");
        assertThat(payload.path("amountMinor").asLong()).as("hoàn đúng số đã thu").isEqualTo(250_000L);
        assertThat(payload.path("currency").asText()).isEqualTo("VND");
        assertThat(payload.path("providerRef").asText()).isEqualTo(refundRow.get("provider_ref"));
        assertThat(payload.path("walletId").asText()).isEqualTo(charge.walletId().toString());

        assertThat(db.outboxEventIds(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND"))
                .as("đúng MỘT event hoàn xong")
                .hasSize(1);
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_FAILED, "REFUND")).isEmpty();
        assertFullyRefundedAtProvider(chargeRow, 250_000);
    }

    @Test
    @DisplayName("⭐ Provider trả mã hoàn TRÙNG mã thu: vẫn ghi được, không nổ khoá unique sau khi tiền đã đi")
    void ma_hoan_trung_ma_thu_van_ghi_duoc() {
        BankChargeCommand charge = succeededCharge(120_000);
        // Có provider thật đánh mã theo GIAO DỊCH chứ không theo THAO TÁC. Khoá cũ
        // UNIQUE (provider, provider_ref) nổ 23505 ở bước ghi câu trả lời — tức là SAU khi provider đã
        // hoàn tiền thật. Hàng kẹt ở NEW, lệnh được gửi lại, và lần gửi lại thường bị provider từ chối
        // ("đã hoàn rồi") → hàng FAILED vĩnh viễn trong khi tiền đã về thẻ người dùng.
        mock.reuseChargeRefForRefunds(true);

        sendRefund(tenant, BankFixtures.refundOf(charge));

        awaitRefundStatus(charge, "SUCCEEDED");
        Map<String, Object> chargeRow = db.row(tenant, charge.transactionId(), "CHARGE");
        Map<String, Object> refundRow = db.row(tenant, charge.transactionId(), "REFUND");
        assertThat(refundRow.get("provider_ref"))
                .as("mã của lần hoàn đúng là mã provider trả về, kể cả khi nó trùng mã lần thu")
                .isEqualTo(chargeRow.get("provider_ref"));
        assertThat(db.outboxEventIds(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND"))
                .as("hoàn xong thì phải báo hoàn xong — không được im lặng vì một ràng buộc của bảng")
                .hasSize(1);
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_FAILED, "REFUND"))
                .as("⚠ 'failed' ở đây là nói tiền chưa về trong khi nó đã về")
                .isEmpty();
        assertFullyRefundedAtProvider(chargeRow, 120_000);
    }

    @Test
    @DisplayName("⭐ Mã của lần THU vẫn duy nhất xuyên tenant — nền móng của đường suy ra tenant từ webhook")
    void ma_thu_van_duy_nhat_xuyen_tenant() {
        BankChargeCommand charge = succeededCharge(70_000);
        String chargeRef = (String) db.row(tenant, charge.transactionId(), "CHARGE").get("provider_ref");

        // V7 chỉ NỚI phía REFUND. Nếu ai đó nới nốt phía CHARGE thì webhook tới từ Internet có thể khớp
        // nhiều hàng và `findByProviderAndProviderRefAndOperation` chọn bừa một tenant — đó là một lỗ
        // cross-tenant, không chỉ là một ràng buộc dữ liệu (ADR-018 §6).
        assertThatThrownBy(() -> db.insertRow(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                70_000, "mock", "SUCCEEDED", chargeRef, null))
                .as("hai lần THU cùng mã ở hai tenant phải bị DB chặn")
                .isInstanceOf(DuplicateKeyException.class);
    }

    // ------------------------------------------------------------------
    // Gửi lại / đồng thời: một lần thu, một lần hoàn
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Gửi lại lệnh hoàn 3 lần (eventId khác nhau): đúng MỘT hàng REFUND, hoàn một lần ở provider, lần nào cũng nhận lại cùng câu trả lời")
    void gui_lai_lenh_hoan_khong_hoan_lan_hai() throws Exception {
        BankChargeCommand charge = succeededCharge(80_000);
        BankRefundCommand command = BankFixtures.refundOf(charge);

        for (int i = 0; i < 3; i++) {
            sendRefund(tenant, command);
        }

        Await.until(() -> db.outboxEventIds(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND")
                .size() == 3, "mỗi lệnh gửi lại đều nhận được câu trả lời (3 event completed REFUND)");

        assertThat(db.rowCount(tenant, charge.transactionId(), "REFUND")).as("một hàng cho một lệnh").isEqualTo(1);
        Map<String, Object> refundRow = db.row(tenant, charge.transactionId(), "REFUND");
        assertThat(refundRow.get("status")).isEqualTo("SUCCEEDED");

        List<Map<String, Object>> events =
                db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND");
        assertThat(events).extracting(row -> row.get("event_id"))
                .as("cùng eventId -> consumer dedup được bản phát lại")
                .containsOnly(events.get(0).get("event_id"));
        for (Map<String, Object> event : events) {
            assertThat(objectMapper.readTree((String) event.get("payload")).path("payload").path("providerRef")
                    .asText())
                    .as("mọi lần phát lại mang đúng mã của MỘT lần hoàn")
                    .isEqualTo(refundRow.get("provider_ref"));
        }
        assertThat(events.get(0).get("event_id"))
                .as("event hoàn khác event thu của cùng giao dịch")
                .isNotIn(db.outboxEventIds(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "CHARGE"));
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_FAILED, "REFUND")).isEmpty();
        assertFullyRefundedAtProvider(db.row(tenant, charge.transactionId(), "CHARGE"), 80_000);
    }

    @Test
    @DisplayName("⭐ 15 vòng × 10 lệnh hoàn giống hệt nhau ĐỒNG THỜI: mỗi vòng đúng một hàng REFUND, cùng mã hoàn, cùng eventId, provider chỉ hoàn một lần")
    void muoi_lenh_hoan_dong_thoi_chi_hoan_mot_lan() throws Exception {
        int rounds = 15;
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int round = 0; round < rounds; round++) {
                BankChargeCommand charge = succeededCharge(10_000 + round);
                BankRefundCommand command = BankFixtures.refundOf(charge);
                CountDownLatch ready = new CountDownLatch(threads);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<ProviderTransaction>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        return TenantContext.callWith(tenant, () -> refundService.refund(command));
                    }));
                }
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                go.countDown();

                List<ProviderTransaction> results = new ArrayList<>();
                for (Future<ProviderTransaction> future : futures) {
                    // ném ExecutionException nếu một lần giành quyền nổ lỗi unique
                    results.add(future.get(60, TimeUnit.SECONDS));
                }

                assertThat(db.rowCount(tenant, charge.transactionId(), "REFUND"))
                        .as("vòng %d: một hàng REFUND", round).isEqualTo(1);
                assertThat(results).as("vòng %d: mọi luồng nhận lời hoàn xong, không luồng nào nhận 'từ chối'", round)
                        .allSatisfy(result -> assertThat(result.getStatus()).isEqualTo(EProviderTxnStatus.SUCCEEDED));
                assertThat(results.stream().map(ProviderTransaction::getProviderRef).distinct().toList())
                        .as("vòng %d: mọi luồng nhận cùng mã của MỘT lần hoàn", round)
                        .hasSize(1);
                List<UUID> eventIds =
                        db.outboxEventIds(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND");
                assertThat(eventIds).isNotEmpty();
                assertThat(new HashSet<>(eventIds)).as("vòng %d: mọi lần phát cùng một eventId", round).hasSize(1);
                assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_FAILED, "REFUND"))
                        .as("vòng %d: không luồng nào làm lệnh hoàn bị coi là thất bại", round).isEmpty();
                assertFullyRefundedAtProvider(db.row(tenant, charge.transactionId(), "CHARGE"), 10_000 + round);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // Chưa thu được thì chưa có gì để hoàn — và KHÔNG được ghi nhận gì "dính"
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Lần thu còn PENDING: hoàn vào DLT với BANK_REFUND_CHARGE_NOT_SUCCEEDED, không hàng REFUND, không event, provider không bị gọi; thu xong rồi gửi lại thì hoàn được")
    void lan_thu_chua_thanh_cong_thi_vao_dlt_khong_ghi_hang() {
        BankChargeCommand charge = BankFixtures.freshCharge(90_000);
        ProviderTransaction pending = TenantContext.callWith(tenant, () -> chargeService.charge(charge));
        assertThat(pending.getStatus()).isEqualTo(EProviderTxnStatus.PENDING);
        // Nếu adapter lỡ gọi provider để hoàn, lời gọi đó ăn lỗi kỹ thuật này và nó không còn nguyên ở cuối
        mock.failNextCalls(1);
        String json = BankFixtures.refundEnvelopeJson(objectMapper, tenant, BankFixtures.refundOf(charge));

        bus.send(CMD_TOPIC, charge.walletId().toString(), json);

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, charge.transactionId().toString());
        assertThat(dead.value()).as("DLT giữ nguyên văn để replay được").isEqualTo(json);
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).contains("PoisonMessageException");
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                .contains("BANK_REFUND_CHARGE_NOT_SUCCEEDED");

        assertThat(db.refundRowCountAnyTenant(charge.transactionId()))
                .as("PENDING = chưa biết: ghi một hàng REFUND FAILED ở đây sẽ dính vĩnh viễn, lệnh gửi lại chỉ phát lại 'failed'")
                .isZero();
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND")).isEmpty();
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_FAILED, "REFUND")).isEmpty();
        assertThat(db.status(tenant, charge.transactionId(), "CHARGE")).isEqualTo("PENDING");
        assertThatThrownBy(() -> mock.getStatus(pending.getProviderRef(), null))
                .as("lỗi giả lập vẫn còn nguyên -> không lời gọi nào của luồng hoàn tới provider")
                .isInstanceOf(ProviderUnavailableException.class);

        // Người dùng trả tiền xong, saga/vận hành gửi lại lệnh hoàn: không có gì dính từ lần bị từ chối trước
        mock.settle(pending.getProviderRef(), EProviderState.SUCCEEDED);
        assertThat(TenantContext.callWith(tenant, () -> chargeStatusService.refresh(pending)))
                .isEqualTo(EProviderTxnStatus.SUCCEEDED);
        sendRefund(tenant, BankFixtures.refundOf(charge));

        awaitRefundStatus(charge, "SUCCEEDED");
        assertThat(db.rowCount(tenant, charge.transactionId(), "REFUND")).isEqualTo(1);
        assertThat(db.outboxEventIds(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND")).hasSize(1);
        assertFullyRefundedAtProvider(db.row(tenant, charge.transactionId(), "CHARGE"), 90_000);
    }

    @Test
    @DisplayName("Lần thu đã FAILED: hoàn vào DLT với BANK_REFUND_CHARGE_NOT_SUCCEEDED, không hàng REFUND, không event — không có tiền nào để hoàn")
    void lan_thu_that_bai_thi_vao_dlt_khong_ghi_hang() {
        BankChargeCommand charge = BankFixtures.freshCharge(45_000);
        ProviderTransaction pending = TenantContext.callWith(tenant, () -> chargeService.charge(charge));
        mock.settle(pending.getProviderRef(), EProviderState.FAILED);
        assertThat(TenantContext.callWith(tenant, () -> chargeStatusService.refresh(pending)))
                .isEqualTo(EProviderTxnStatus.FAILED);
        mock.failNextCalls(1);

        sendRefund(tenant, BankFixtures.refundOf(charge));

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, charge.transactionId().toString());
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                .contains("BANK_REFUND_CHARGE_NOT_SUCCEEDED");
        assertThat(db.refundRowCountAnyTenant(charge.transactionId())).isZero();
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND")).isEmpty();
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_FAILED, "REFUND")).isEmpty();
        assertThat(db.status(tenant, charge.transactionId(), "CHARGE")).isEqualTo("FAILED");
        assertThatThrownBy(() -> mock.getStatus(pending.getProviderRef(), null))
                .as("lỗi giả lập vẫn còn nguyên -> provider không bị gọi để hoàn")
                .isInstanceOf(ProviderUnavailableException.class);
    }

    // ------------------------------------------------------------------
    // Lệnh hỏng -> DLT, không để lại gì
    // ------------------------------------------------------------------
    @ParameterizedTest(name = "đổi {0}")
    @ValueSource(strings = {"số tiền ít hơn", "số tiền nhiều hơn", "ví", "loại tiền"})
    @DisplayName("⭐ Lệnh hoàn lệch lần thu (ví/số tiền/loại tiền): vào DLT nguyên văn với BANK_REFUND_MISMATCH, không tạo hàng, provider không bị chạm; lệnh đúng sau đó vẫn hoàn được")
    void lenh_hoan_lech_so_tien_vao_dlt(String khacBiet) {
        BankChargeCommand charge = succeededCharge(60_000);
        BankRefundCommand mismatched = switch (khacBiet) {
            case "số tiền ít hơn" -> new BankRefundCommand(charge.transactionId(), charge.walletId(), 30_000,
                    ECurrency.VND, "ACCOUNT_CLOSED");
            case "số tiền nhiều hơn" -> new BankRefundCommand(charge.transactionId(), charge.walletId(), 61_000,
                    ECurrency.VND, "ACCOUNT_CLOSED");
            case "ví" -> new BankRefundCommand(charge.transactionId(), UUID.randomUUID(), 60_000,
                    ECurrency.VND, "ACCOUNT_CLOSED");
            default -> new BankRefundCommand(charge.transactionId(), charge.walletId(), 60_000,
                    ECurrency.USD, "ACCOUNT_CLOSED");
        };
        String json = BankFixtures.refundEnvelopeJson(objectMapper, tenant, mismatched);

        bus.send(CMD_TOPIC, charge.walletId().toString(), json);

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, charge.transactionId().toString());
        assertThat(dead.value()).as("DLT giữ nguyên văn để replay được").isEqualTo(json);
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).contains("PoisonMessageException");
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("BANK_REFUND_MISMATCH");

        assertThat(db.refundRowCountAnyTenant(charge.transactionId()))
                .as("lệnh sai không được để lại hàng REFUND nào")
                .isZero();
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND")).isEmpty();
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_FAILED, "REFUND")).isEmpty();
        assertThat(db.status(tenant, charge.transactionId(), "CHARGE")).isEqualTo("SUCCEEDED");

        // Lệnh sai không "đốt" giao dịch: lệnh đúng đến sau vẫn hoàn được, và provider còn nguyên số để hoàn
        // — nếu lệnh sai đã hoàn một phần, lần hoàn trọn này sẽ bị provider từ chối (NOT_REFUNDABLE) và hàng thành FAILED.
        sendRefund(tenant, BankFixtures.refundOf(charge));
        awaitRefundStatus(charge, "SUCCEEDED");
        assertFullyRefundedAtProvider(db.row(tenant, charge.transactionId(), "CHARGE"), 60_000);
    }

    @Test
    @DisplayName("Hoàn cho giao dịch chưa từng thu: vào DLT với BANK_REFUND_NO_CHARGE, không tạo hàng nào, provider không bị gọi")
    void lenh_hoan_khong_co_lan_thu_vao_dlt() {
        BankRefundCommand command = BankFixtures.refundOf(BankFixtures.freshCharge(20_000));
        String json = BankFixtures.refundEnvelopeJson(objectMapper, tenant, command);

        bus.send(CMD_TOPIC, command.walletId().toString(), json);

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, command.transactionId().toString());
        assertThat(dead.value()).isEqualTo(json);
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("BANK_REFUND_NO_CHARGE");
        assertThat(db.rowCountAnyTenant(command.transactionId())).isZero();
        assertThat(db.outboxCountAnyTopic(command.transactionId())).isZero();
        assertThat(mock.sessionCount()).isZero();
    }

    @Test
    @DisplayName("Lần thu thuộc provider khác: hoàn vào DLT với BANK_PROVIDER_MISMATCH — mã tham chiếu vô nghĩa ở provider đang chạy")
    void lan_thu_cua_provider_khac_khong_hoan_duoc() {
        BankChargeCommand charge = BankFixtures.freshCharge(35_000);
        db.insertRow(tenant, charge.transactionId(), charge.walletId(), 35_000, "vnpay", "SUCCEEDED",
                "VNP-" + UUID.randomUUID(), null);

        sendRefund(tenant, BankFixtures.refundOf(charge));

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, charge.transactionId().toString());
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("BANK_PROVIDER_MISMATCH");
        assertThat(db.refundRowCountAnyTenant(charge.transactionId())).isZero();
        assertThat(db.outboxCountAnyTopic(charge.transactionId())).isZero();
    }

    @ParameterizedTest(name = "số tiền {0}")
    @ValueSource(longs = {0, -1, -60_000})
    @DisplayName("Số tiền hoàn không dương: BANK_REFUND_INVALID, không tạo hàng, không gọi provider")
    void so_tien_hoan_khong_duong_bi_tu_choi(long amountMinor) {
        BankChargeCommand charge = succeededCharge(60_000);
        BankRefundCommand command = new BankRefundCommand(charge.transactionId(), charge.walletId(), amountMinor,
                ECurrency.VND, "ACCOUNT_CLOSED");

        assertThatThrownBy(() -> TenantContext.callWith(tenant, () -> refundService.refund(command)))
                .isInstanceOfSatisfying(BadRequestException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo("BANK_REFUND_INVALID"));

        assertThat(db.refundRowCountAnyTenant(charge.transactionId())).isZero();
        assertNothingRefundedAtProvider(db.row(tenant, charge.transactionId(), "CHARGE"), 60_000);
    }

    @Test
    @DisplayName("Tenant khác biết transactionId cũng không hoàn được: DLT với BANK_REFUND_NO_CHARGE, không có hàng ở tenant nào, lần thu không bị chạm")
    void tenant_khac_khong_hoan_duoc() {
        BankChargeCommand charge = succeededCharge(70_000);
        UUID attacker = UUID.randomUUID();
        String json = BankFixtures.refundEnvelopeJson(objectMapper, attacker, BankFixtures.refundOf(charge));

        bus.send(CMD_TOPIC, charge.walletId().toString(), json);

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, charge.transactionId().toString());
        assertThat(dead.value()).isEqualTo(json);
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("BANK_REFUND_NO_CHARGE");

        assertThat(db.rowCount(attacker, charge.transactionId(), "REFUND")).isZero();
        assertThat(db.rowCount(tenant, charge.transactionId(), "REFUND"))
                .as("lần thu của chủ thật cũng không bị hoàn hộ")
                .isZero();
        assertThat(db.status(tenant, charge.transactionId(), "CHARGE")).isEqualTo("SUCCEEDED");
        assertNothingRefundedAtProvider(db.row(tenant, charge.transactionId(), "CHARGE"), 70_000);
    }

    // ------------------------------------------------------------------
    // Lỗi kỹ thuật -> không kết luận
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Provider lỗi kỹ thuật khi hoàn: retry rồi DLT, hàng REFUND giữ ở NEW, KHÔNG có completed/failed; hồi lại rồi gửi lại → hoàn đúng một lần")
    void provider_loi_ky_thuat_thi_khong_ket_luan() {
        BankChargeCommand charge = succeededCharge(55_000);
        mock.failNextCalls(1_000);

        sendRefund(tenant, BankFixtures.refundOf(charge));

        ConsumerRecord<String, String> dead = bus.await(DLT_TOPIC, charge.transactionId().toString());
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .as("lỗi kỹ thuật chứ không phải PoisonMessageException")
                .contains("ProviderUnavailableException");
        assertThat(db.row(tenant, charge.transactionId(), "REFUND").get("status"))
                .as("lỗi/timeout ≠ chưa hoàn: không biết provider đã hoàn chưa nên không được kết luận")
                .isEqualTo("NEW");
        assertThat(db.row(tenant, charge.transactionId(), "REFUND").get("provider_ref")).isNull();
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_FAILED, "REFUND"))
                .as("nhất là không được phát 'failed' — saga sẽ báo người dùng tiền không về trong khi chưa biết")
                .isEmpty();
        assertThat(db.outboxOfOperation(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND")).isEmpty();

        // Provider hồi lại, người vận hành replay từ DLT: dùng lại đúng hàng NEW, hoàn một lần
        mock.failNextCalls(0);
        resilientProvider.refundCircuitBreaker().reset();
        sendRefund(tenant, BankFixtures.refundOf(charge));

        awaitRefundStatus(charge, "SUCCEEDED");
        assertThat(db.rowCount(tenant, charge.transactionId(), "REFUND")).isEqualTo(1);
        assertThat(db.outboxEventIds(charge.transactionId(), WalletTopics.EVT_BANK_COMPLETED, "REFUND")).hasSize(1);
        assertFullyRefundedAtProvider(db.row(tenant, charge.transactionId(), "CHARGE"), 55_000);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Lần thu đã đi trọn tới SUCCEEDED (người dùng trả tiền + job đối chiếu), do {@link #tenant} sở hữu. */
    private BankChargeCommand succeededCharge(long amountMinor) {
        BankChargeCommand command = BankFixtures.freshCharge(amountMinor);
        ProviderTransaction pending = TenantContext.callWith(tenant, () -> chargeService.charge(command));
        mock.settle(pending.getProviderRef(), EProviderState.SUCCEEDED);
        assertThat(TenantContext.callWith(tenant, () -> chargeStatusService.refresh(pending)))
                .as("điều kiện đầu vào của mọi ca hoàn: lần thu đã SUCCEEDED")
                .isEqualTo(EProviderTxnStatus.SUCCEEDED);
        return command;
    }

    private void sendRefund(UUID tenantId, BankRefundCommand command) {
        bus.send(CMD_TOPIC, command.walletId().toString(),
                BankFixtures.refundEnvelopeJson(objectMapper, tenantId, command));
    }

    private void awaitRefundStatus(BankChargeCommand charge, String expected) {
        Await.until(() -> db.rowCount(tenant, charge.transactionId(), "REFUND") == 1
                        && expected.equals(db.row(tenant, charge.transactionId(), "REFUND").get("status")),
                "hàng REFUND của " + charge.transactionId() + " sang " + expected);
    }

    /** Message (envelope JSON) của đúng giao dịch này và đúng thao tác — topic completed/failed dùng chung thu + hoàn. */
    private Predicate<String> isEventOf(UUID transactionId, String operation) {
        return json -> {
            try {
                JsonNode payload = objectMapper.readTree(json).path("payload");
                return transactionId.toString().equals(payload.path("transactionId").asText())
                        && operation.equals(payload.path("operation").asText());
            } catch (Exception notJson) {
                return false;
            }
        };
    }

    /**
     * Provider đã hoàn HẾT số đã thu: một lần hoàn mới (khoá khác) dù chỉ 1 đồng cũng bị từ chối. Nếu adapter
     * chưa hoàn, phép thử này sẽ thành công và test đỏ; nếu hoàn vượt, provider đã từ chối từ trước.
     */
    private void assertFullyRefundedAtProvider(Map<String, Object> chargeRow, long chargedMinor) {
        RefundResult probe = mock.refund(new RefundRequest(UUID.randomUUID().toString(),
                (String) chargeRow.get("provider_ref"), 1, ECurrency.VND));
        assertThat(probe)
                .as("provider phải đã hoàn trọn %d — hoàn thêm 1 đồng phải bị từ chối", chargedMinor)
                .isInstanceOfSatisfying(RefundResult.Declined.class,
                        declined -> assertThat(declined.failureCode()).isEqualTo("NOT_REFUNDABLE"));
    }

    /**
     * Provider CHƯA hoàn gì: hoàn trọn bằng khoá mới thành công. Phép thử này tự nó hoàn tiền ở mock nên chỉ gọi
     * ở cuối test, khi không còn gì khác cần kiểm về provider.
     */
    private void assertNothingRefundedAtProvider(Map<String, Object> chargeRow, long chargedMinor) {
        RefundResult probe = mock.refund(new RefundRequest(UUID.randomUUID().toString(),
                (String) chargeRow.get("provider_ref"), chargedMinor, ECurrency.VND));
        assertThat(probe)
                .as("adapter chưa hoàn gì ở provider nên hoàn trọn %d bằng khoá mới phải thành công", chargedMinor)
                .isInstanceOf(RefundResult.Refunded.class);
    }
}
