package com.walletflow.bank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.services.ChargeService;
import com.walletflow.bank.services.PendingChargeReconciler;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.BankFixtures;
import com.walletflow.bank.support.BankKafka;
import com.walletflow.bank.support.FakeVnPayServer;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.support.KafkaHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Luồng thu tiền qua VNPay đi trọn: lệnh Kafka → URL thanh toán ký sẵn → (người dùng trả tiền ở VNPay giả) → job đối
 * soát hỏi {@code querydr} → event {@code bank.event.*} (ADR-017). VNPay giả là một HTTP server thật ({@link
 * FakeVnPayServer}) tự cài chữ ký ĐỘC LẬP, nên nó cũng là phép kiểm chéo chữ ký của adapter.
 *
 * <p>Nguyên tắc xuyên suốt: câu trả lời của VNPay chỉ được tin khi chữ ký đúng; "không biết" (sai chữ ký, mã lạ,
 * timeout, trả lời rác) KHÔNG BAO GIỜ thành "đã thu" hay "thất bại" — hàng giữ PENDING và lỗi được đếm.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "outbox.relay.delay-ms=300",
        "walletflow.bank.provider=vnpay",
        "walletflow.bank.vnpay.tmn-code=" + VnPayChargeIT.TMN_CODE,
        "walletflow.bank.vnpay.hash-secret=" + VnPayChargeIT.HASH_SECRET,
        "walletflow.bank.vnpay.return-url=http://localhost/return",
        "walletflow.bank.vnpay.server-ip=10.0.0.1",
        // HTTP client phải timeout NGẮN hơn resilience.timeout (PaymentProviderConfig kiểm lúc khởi động)
        "walletflow.bank.vnpay.connect-timeout=1s",
        "walletflow.bank.vnpay.read-timeout=500ms",
        "walletflow.bank.resilience.timeout=2s",
        "walletflow.bank.session.ttl=PT15M",
        "walletflow.bank.session.expiry-grace=PT15M",
        // Sau EXPIRED hỏi xác nhận một lần sau chừng này (ADR-017) — phát hiện tiền về muộn
        "walletflow.bank.session.expired-confirm-after=PT1H",
        // Gọi reconcile() trực tiếp; recheck PT0S để hàng PENDING tới hạn ngay ở mọi lượt
        "walletflow.bank.status-check.interval-ms=3600000",
        "walletflow.bank.status-check.recheck-after=PT0S",
        "walletflow.kafka.retry.max-attempts=2",
        "walletflow.kafka.retry.initial-interval-ms=200",
        "walletflow.kafka.retry.max-interval-ms=400"
})
@Testcontainers
@ExtendWith(OutputCaptureExtension.class)
class VnPayChargeIT {

    static final String TMN_CODE = "WFTEST01";
    /** Giá trị giả, chỉ dùng trong test này — không phải bí mật của môi trường nào. */
    static final String HASH_SECRET = "it-only-fake-hash-secret-0123456789abcdef";
    /** Phải khớp walletflow.bank.session.expiry-grace ở trên. */
    private static final long GRACE_SECONDS = 900;
    /** Phải khớp walletflow.bank.session.expired-confirm-after ở trên. */
    private static final long CONFIRM_SECONDS = 3600;
    private static final String NEEDS_REVIEW_METRIC = "walletflow.bank.expired_needs_review";
    private static final DateTimeFormatter VNPAY_DATE = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private static final String CMD_TOPIC = WalletTopics.CMD_BANK_CHARGE;
    private static final String FAILURE_METRIC = "walletflow.bank.status_check.failures";
    private static final String AMOUNT_MISMATCH_METRIC = "walletflow.bank.amount_mismatch";
    private static final String OVERDUE_GAUGE = "walletflow.bank.pending_overdue";

    private static final Logger log = LoggerFactory.getLogger(VnPayChargeIT.class);

    static final FakeVnPayServer fake = new FakeVnPayServer(TMN_CODE, HASH_SECRET);

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
        registry.add("walletflow.bank.vnpay.pay-url", fake::payUrl);
        registry.add("walletflow.bank.vnpay.api-url", fake::apiUrl);
    }

    @AfterAll
    static void stopFake() {
        fake.close();
    }

    @Autowired
    PendingChargeReconciler reconciler;
    @Autowired
    ChargeService chargeService;
    @Autowired
    ResilientPaymentProvider resilientProvider;
    @Autowired
    MeterRegistry meterRegistry;
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
        fake.reset();
        resilientProvider.circuitBreaker().reset();
        tenant = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Tạo phiên: URL thanh toán
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Lệnh thu có clientIp qua Kafka → event charge-pending mang URL VNPay; VNPay giả (ký độc lập) chấp nhận URL: "
            + "số tiền ×100, IP người dùng, hạn 15 phút, vnp_TxnRef = providerRef; hàng DB có mốc tạo và hạn khớp URL")
    void lenh_thu_co_client_ip_ra_url_vnpay_ky_dung() throws Exception {
        BankChargeCommand command = new BankChargeCommand(UUID.randomUUID(), UUID.randomUUID(), 123_456,
                ECurrency.VND, "203.0.113.7");

        sendCommand(command);

        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_CHARGE_PENDING,
                command.transactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_CHARGE_PENDING, record.value());
        JsonNode payload = objectMapper.readTree(record.value()).path("payload");
        String providerRef = payload.path("providerRef").asText();
        String paymentUrl = payload.path("paymentUrl").asText();
        assertThat(payload.path("provider").asText()).isEqualTo("vnpay");
        assertThat(paymentUrl).startsWith(fake.payUrl() + "?");

        // Người dùng mở URL: AssertionError nếu chữ ký/dạng chuẩn của URL sai
        Map<String, String> params = fake.pay(paymentUrl);

        assertThat(params.get("vnp_Amount")).as("123.456 đồng × 100").isEqualTo("12345600");
        assertThat(params.get("vnp_IpAddr")).isEqualTo("203.0.113.7");
        assertThat(params.get("vnp_TmnCode")).isEqualTo(TMN_CODE);
        assertThat(params.get("vnp_ReturnUrl")).isEqualTo("http://localhost/return");
        assertThat(params.get("vnp_TxnRef")).isEqualTo(providerRef);
        String idempotencyKey = (String) db.row(tenant, command.transactionId()).get("idempotency_key");
        assertThat(providerRef)
                .as("mã tham chiếu tất định theo khoá idempotency: gửi lại lệnh không bao giờ sinh giao dịch thứ hai ở VNPay")
                .isEqualTo(idempotencyKey.replace("-", ""));
        Duration validity = Duration.between(LocalDateTime.parse(params.get("vnp_CreateDate"), VNPAY_DATE),
                LocalDateTime.parse(params.get("vnp_ExpireDate"), VNPAY_DATE));
        assertThat(validity).isEqualTo(Duration.ofMinutes(15));

        assertThat(db.status(tenant, command.transactionId())).isEqualTo("PENDING");
        assertThat(db.providerAccount(tenant, command.transactionId()))
                .as("hàng ghi tài khoản merchant đã tạo phiên (vnp_TmnCode): phiên chỉ hỏi được dưới đúng tài khoản này")
                .isEqualTo(TMN_CODE);
        assertThat(db.providerRef(tenant, command.transactionId())).isEqualTo(providerRef);
        assertThat(db.row(tenant, command.transactionId()).get("payment_url")).isEqualTo(paymentUrl);
        assertThat(db.providerCreatedAtAsVnpayDate(command.transactionId()))
                .as("mốc tạo lưu ở DB (giờ VN, do Postgres đổi) = vnp_CreateDate đã ký — querydr sẽ gửi lại đúng mốc này")
                .isEqualTo(params.get("vnp_CreateDate"));
        assertThat(db.sessionLifetimeSeconds(command.transactionId())).isEqualTo(15 * 60);
        // expires_at do đồng hồ ỨNG DỤNG đặt, now() là đồng hồ DB (container): hai đồng hồ lệch nhau vài giây là
        // chuyện thường, nên cận trên nới thêm 5s để test không đỏ vì lệch đồng hồ (không phải vì lỗi logic)
        assertThat(db.secondsUntilExpiry(command.transactionId())).isBetween(14 * 60.0, 15 * 60.0 + 5);
        assertThat(db.checkAttempts(command.transactionId())).isZero();
        assertThat(fake.requests()).as("tạo phiên không gọi mạng tới VNPay").isEmpty();
    }

    @Test
    @DisplayName("Lệnh thu không có clientIp (message v1 cũ) → vnp_IpAddr là IP máy chủ")
    void lenh_thu_khong_co_client_ip_dung_ip_may_chu() throws Exception {
        BankChargeCommand command = BankFixtures.freshCharge(80_000);

        sendCommand(command);

        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_CHARGE_PENDING,
                command.transactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_CHARGE_PENDING, record.value());
        String paymentUrl = objectMapper.readTree(record.value()).path("payload").path("paymentUrl").asText();

        Map<String, String> params = fake.pay(paymentUrl);

        assertThat(params.get("vnp_IpAddr")).isEqualTo("10.0.0.1");
        assertThat(params.get("vnp_Amount")).isEqualTo("8000000");
    }

    @Test
    @DisplayName("USD qua Kafka: event failed CURRENCY_NOT_SUPPORTED với providerRef null; hàng FAILED, không phiên nào ở VNPay")
    void usd_ra_event_failed_khong_co_phien() throws Exception {
        BankChargeCommand command = new BankChargeCommand(UUID.randomUUID(), UUID.randomUUID(), 50, ECurrency.USD, null);

        sendCommand(command);

        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_FAILED,
                command.transactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_FAILED, record.value());
        JsonNode payload = objectMapper.readTree(record.value()).path("payload");
        assertThat(payload.path("failureCode").asText()).isEqualTo("CURRENCY_NOT_SUPPORTED");
        assertThat(payload.hasNonNull("providerRef")).as("chưa từng có phiên").isFalse();
        assertThat(payload.path("provider").asText()).isEqualTo("vnpay");

        assertThat(db.status(tenant, command.transactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, command.transactionId())).isEqualTo("CURRENCY_NOT_SUPPORTED");
        assertThat(db.providerRef(tenant, command.transactionId())).isNull();
        assertThat(db.row(tenant, command.transactionId()).get("payment_url")).isNull();
        assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING)).isEmpty();
        assertThat(fake.requests()).isEmpty();
    }

    @Test
    @DisplayName("Số tiền vượt trần vnp_Amount (9.999.999.999): FAILED AMOUNT_LIMIT_EXCEEDED, một event failed, không URL")
    void vuot_tran_so_tien_bi_tu_choi() {
        BankChargeCommand command = BankFixtures.freshCharge(10_000_000_000L);

        ProviderTransaction row = TenantContext.callWith(tenant, () -> chargeService.charge(command));

        assertThat(row.getPaymentUrl()).isNull();
        assertThat(db.status(tenant, command.transactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, command.transactionId())).isEqualTo("AMOUNT_LIMIT_EXCEEDED");
        assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
        assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING)).isEmpty();
    }

    // ------------------------------------------------------------------
    // Đối soát: kết quả thật
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Người dùng trả tiền ở VNPay + job đối soát → SUCCEEDED, event completed đúng schema mang đủ số tiền (VNPay ×100 được chia lại)")
    void da_tra_tien_doi_soat_ra_completed() throws Exception {
        ProviderTransaction row = createPending(250_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_COMPLETED,
                row.getTransactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_COMPLETED, record.value());
        JsonNode payload = objectMapper.readTree(record.value()).path("payload");
        assertThat(payload.path("amountMinor").asLong()).isEqualTo(250_000L);
        assertThat(payload.path("currency").asText()).isEqualTo("VND");
        assertThat(payload.path("providerRef").asText()).isEqualTo(row.getProviderRef());
        assertThat(payload.path("operation").asText()).isEqualTo("CHARGE");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(db.row(tenant, row.getTransactionId()).get("raw_response").toString())
                .as("câu trả lời VNPay được lưu nguyên văn để đối soát")
                .contains("vnp_TransactionStatus");
        assertThat(db.row(tenant, row.getTransactionId()).get("next_check_at")).isNull();
    }

    @Test
    @DisplayName("querydr gửi đúng vnp_TransactionDate = vnp_CreateDate trong URL (sai một giây VNPay nói 'không tìm thấy'), "
            + "đúng mã merchant/tham chiếu/IP máy chủ, JSON, checksum hợp lệ")
    void querydr_gui_dung_ngay_tao_cua_url() {
        ProviderTransaction row = createPending(80_000, "203.0.113.7");
        Map<String, String> params = fake.pay(row.getPaymentUrl());

        reconciler.reconcile();

        List<FakeVnPayServer.QueryRequest> requests = fake.requests();
        assertThat(requests).hasSize(1);
        FakeVnPayServer.QueryRequest query = requests.get(0);
        assertThat(query.get("vnp_TransactionDate")).isEqualTo(params.get("vnp_CreateDate"));
        assertThat(query.get("vnp_TxnRef")).isEqualTo(row.getProviderRef());
        assertThat(query.get("vnp_TmnCode")).isEqualTo(TMN_CODE);
        assertThat(query.get("vnp_Command")).isEqualTo("querydr");
        assertThat(query.get("vnp_Version")).isEqualTo("2.1.0");
        assertThat(query.get("vnp_IpAddr")).as("querydr là máy chủ gọi: IP máy chủ, không phải IP người dùng")
                .isEqualTo("10.0.0.1");
        assertThat(query.get("vnp_RequestId")).isNotBlank();
        assertThat(query.checksumValid()).as("checksum của request, do VNPay giả kiểm độc lập").isTrue();
        assertThat(query.contentType()).startsWith("application/json");
        long secondsFromNow = Math.abs(Duration.between(LocalDateTime.parse(query.get("vnp_CreateDate"), VNPAY_DATE),
                LocalDateTime.now(java.time.ZoneOffset.ofHours(7))).toSeconds());
        assertThat(secondsFromNow).as("vnp_CreateDate của querydr là giờ hiện tại (GMT+7)").isLessThan(60);

        assertThat(db.status(tenant, row.getTransactionId()))
                .as("hậu quả của hỏi đúng ngày: VNPay tìm thấy giao dịch đã trả -> SUCCEEDED (hỏi sai ngày sẽ ra 91 = 'chưa thu')")
                .isEqualTo("SUCCEEDED");
    }

    // ------------------------------------------------------------------
    // Đối soát: câu trả lời không đáng tin
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Người dùng đã trả tiền nhưng chữ ký câu trả lời SAI: hàng giữ PENDING, KHÔNG completed, KHÔNG failed, "
            + "đếm status_check.failures +1; có chữ ký đúng thì chốt SUCCEEDED")
    void chu_ky_tra_loi_sai_thi_khong_chot_thanh_cong() {
        ProviderTransaction row = createPending(500_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());
        fake.tamperSignature(true);
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId()))
                .as("câu trả lời giả mạo nói 'đã thu' là cách tạo tiền từ không khí")
                .isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);

        // Đối chứng: cùng giao dịch, bỏ chữ ký sai -> chốt được. Test không đỏ/xanh vì lý do khác
        fake.tamperSignature(false);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
    }

    @Test
    @DisplayName("⭐ Giao dịch CHƯA trả (01) nhưng kẻ tấn công sửa câu trả lời thành 00/00 sau khi VNPay ký: chữ ký không khớp "
            + "-> PENDING, không completed")
    void noi_dung_bi_sua_sau_khi_ky_khong_chot_thanh_cong() {
        ProviderTransaction row = createPending(500_000, "203.0.113.7");
        fake.open(row.getPaymentUrl());   // mở URL nhưng chưa trả: trạng thái thật là 01
        fake.forgeSuccessAfterSigning(true);
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Câu trả lời ký đúng nhưng mang mã merchant khác, hoặc nói về giao dịch khác: PENDING + đếm lỗi; bình thường lại thì chốt")
    void tra_loi_ve_merchant_hoac_giao_dich_khac_khong_duoc_tin() {
        ProviderTransaction row = createPending(120_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());
        double failuresBefore = failureCount();

        fake.answerWithTmnCode("OTHER001");
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).as("mã merchant khác").isEqualTo("PENDING");
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);

        fake.answerWithTmnCode(null);
        fake.answerWithTxnRef("SOMEOTHERREF123");
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).as("giao dịch khác").isEqualTo("PENDING");
        assertThat(failureCount() - failuresBefore).isEqualTo(2.0);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();

        fake.answerWithTxnRef(null);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
    }

    @ParameterizedTest(name = "thân trả lời: [{0}]")
    @ValueSource(strings = {"<html>Bad gateway</html>", "", "[]", "null", "{\"vnp_ResponseCode\":\"00\",\"vnp_TransactionStatus\":\"00\"}"})
    @DisplayName("Câu trả lời rác (không phải JSON, rỗng, không phải object, không có chữ ký): PENDING + đếm lỗi, không completed")
    void tra_loi_rac_thi_khong_biet(String rawBody) {
        ProviderTransaction row = createPending(120_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());
        fake.respondWithRawBody(rawBody);
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
    }

    @Test
    @DisplayName("VNPay trả HTTP 500: PENDING + đếm lỗi (lỗi kỹ thuật, không phải câu trả lời)")
    void http_500_thi_khong_biet() {
        ProviderTransaction row = createPending(120_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());
        fake.respondWithHttpStatus(500);
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
    }

    @Test
    @DisplayName("Mã trả lời lạ (99) từ VNPay: hàng giữ PENDING, đếm lỗi, không event nào")
    void ma_tra_loi_99_thi_khong_biet() {
        ProviderTransaction row = createPending(120_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());
        fake.respondWithCode("99");
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
    }

    @Test
    @DisplayName("querydr chậm hơn read-timeout: HTTP client tự ngắt (không đợi hết resilience.timeout), PENDING + đếm lỗi; hết chậm thì chốt")
    void querydr_timeout_thi_khong_biet() {
        ProviderTransaction row = createPending(120_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());
        fake.delay(Duration.ofMillis(1_500));
        double failuresBefore = failureCount();

        long startedAt = System.nanoTime();
        assertThat(reconciler.reconcile()).isEqualTo(1);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(elapsed)
                .as("read-timeout 500ms của HTTP client phải nổ trước độ trễ 1,5s của VNPay và resilience.timeout 2s")
                .isLessThan(Duration.ofMillis(1_400));

        fake.delay(Duration.ZERO);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("Hàng không có mốc tạo phiên (tạo trước 2.2b): không hỏi VNPay (sẽ ra 'không tìm thấy' giả), PENDING + đếm lỗi")
    void hang_thieu_moc_tao_phien_khong_hoi_vnpay() {
        ProviderTransaction row = createPending(120_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());
        jdbcTemplate.update("UPDATE provider_transactions SET provider_created_at = NULL WHERE id = ?", row.getId());
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(fake.requests()).as("không có ngày thì không hỏi — hỏi bừa sẽ nhận 91").isEmpty();
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
    }

    // ------------------------------------------------------------------
    // Số tiền
    // ------------------------------------------------------------------
    @ParameterizedTest(name = "VNPay báo vnp_Amount = {0} cho lần thu 10.000 đồng (đúng là 1000000)")
    @ValueSource(longs = {1_000_100, 999_900, 100, 0, 100_000_000})
    @DisplayName("⭐ VNPay báo đã thu NHƯNG số tiền khác số yêu cầu: PENDING, KHÔNG completed, walletflow.bank.amount_mismatch +1")
    void thu_lech_so_tien_khong_chot(long scaledAmount) {
        ProviderTransaction row = createPending(10_000, "203.0.113.7");
        Map<String, String> params = fake.pay(row.getPaymentUrl());
        fake.setAmount(params.get("vnp_TxnRef"), scaledAmount);
        double mismatchBefore = amountMismatchCount();
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED))
                .as("ledger sẽ cộng đúng số trong event: số đó phải là số VNPay thực thu").isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(amountMismatchCount() - mismatchBefore).isEqualTo(1.0);
        assertThat(failureCount()).as("lệch số không phải lỗi kỹ thuật của lần hỏi").isEqualTo(failuresBefore);
        assertThat(db.row(tenant, row.getTransactionId()).get("amount_minor")).isEqualTo(10_000L);
    }

    @Test
    @DisplayName("vnp_Amount lẻ trăm (VND không có phần lẻ): câu trả lời vô nghĩa -> PENDING + đếm lỗi, không đoán số, không completed")
    void so_tien_le_tram_thi_khong_biet() {
        ProviderTransaction row = createPending(10_000, "203.0.113.7");
        Map<String, String> params = fake.pay(row.getPaymentUrl());
        fake.setAmount(params.get("vnp_TxnRef"), 1_000_050);
        double mismatchBefore = amountMismatchCount();
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
        assertThat(amountMismatchCount()).isEqualTo(mismatchBefore);
    }

    // ------------------------------------------------------------------
    // Vòng đời: hết hạn
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Chưa ai mở URL (VNPay nói 91): trong hạn PENDING, quá hạn nhưng còn trong grace vẫn PENDING, "
            + "quá hạn + grace thì FAILED(EXPIRED) với đúng MỘT event failed mang providerRef")
    void chua_ai_mo_url_het_han_sau_grace() throws Exception {
        ProviderTransaction row = createPending(60_000, "203.0.113.7");
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(fake.requests()).as("đã hỏi VNPay, và nó nói 91").hasSize(1);
        assertThat(db.status(tenant, row.getTransactionId())).as("91 trong hạn = chưa thu, chưa kết luận").isEqualTo("PENDING");
        assertThat(failureCount()).as("91 là câu trả lời bình thường, không phải lỗi").isEqualTo(failuresBefore);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();

        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, -60);   // quá hạn phiên 14 phút, còn 1 phút grace
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).as("còn trong grace").isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();

        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 60);    // quá hạn + grace 1 phút
        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, row.getTransactionId())).isEqualTo("EXPIRED");
        // Hợp đồng ĐÃ ĐỔI có chủ ý (ADR-017): hết hạn không còn xoá next_check_at mà đặt lịch MỘT lần hỏi xác nhận
        // sau expired-confirm-after (1 giờ), để phát hiện tiền về muộn
        assertThat(db.secondsUntilNextCheck(row.getTransactionId()))
                .as("hết hạn đặt lịch hỏi xác nhận sau expired-confirm-after")
                .isBetween(CONFIRM_SECONDS - 5.0, CONFIRM_SECONDS + 0.5);
        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_FAILED,
                row.getTransactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_FAILED, record.value());
        JsonNode payload = objectMapper.readTree(record.value()).path("payload");
        assertThat(payload.path("failureCode").asText()).isEqualTo("EXPIRED");
        assertThat(payload.path("providerRef").asText()).isEqualTo(row.getProviderRef());
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(reconciler.reconcile()).as("hàng đã kết thúc không còn được hỏi").isZero();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
    }

    @ParameterizedTest(name = "trạng thái giao dịch {0}")
    @ValueSource(strings = {"01", "02"})
    @DisplayName("Đã mở URL nhưng chưa trả xong (01) hoặc lần trả hỏng (02): PENDING trong hạn (còn trả lại được), quá hạn + grace thì FAILED(EXPIRED)")
    void chua_tra_xong_het_han_sau_grace(String transactionStatus) {
        ProviderTransaction row = createPending(60_000, "203.0.113.7");
        Map<String, String> params = fake.open(row.getPaymentUrl());
        fake.setStatus(params.get("vnp_TxnRef"), transactionStatus);

        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();

        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 60);
        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, row.getTransactionId())).isEqualTo("EXPIRED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
    }

    @ParameterizedTest(name = "trạng thái giao dịch \"{0}\"")
    @ValueSource(strings = {"04", "05", "06", "07", "09", "99", ""})
    @DisplayName("⭐ Trạng thái lơ lửng (04 đảo, 05/06/09 hoàn, 07 nghi gian lận, lạ, thiếu) QUÁ HẠN + grace: KHÔNG BAO GIỜ hết hạn, "
            + "gauge pending_overdue = 1; sau đó VNPay chốt 00 thì SUCCEEDED")
    void trang_thai_lo_lung_qua_han_khong_bao_gio_het_han(String transactionStatus) {
        ProviderTransaction row = createPending(60_000, "203.0.113.7");
        Map<String, String> params = fake.pay(row.getPaymentUrl());
        fake.setStatus(params.get("vnp_TxnRef"), transactionStatus);
        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 3_600);
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId()))
                .as("tiền có thể đã rời tài khoản người dùng: 'không biết' không được thành 'thất bại'")
                .isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(failureCount()).as("lơ lửng là câu trả lời, không phải lỗi kỹ thuật").isEqualTo(failuresBefore);
        assertThat(overdueGauge()).as("hàng quá hạn + grace mà chưa kết luận được = một ca cần người xem").isEqualTo(1.0);

        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).as("hỏi lại vẫn thế, vẫn không hết hạn").isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();

        fake.setStatus(params.get("vnp_TxnRef"), "00");
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
        assertThat(overdueGauge()).as("hết lơ lửng thì gauge về 0").isZero();
    }

    // ------------------------------------------------------------------
    // Loại giao dịch: "thành công" của một lần HOÀN TIỀN không phải tiền thu
    // ------------------------------------------------------------------
    @ParameterizedTest(name = "vnp_TransactionType = \"{0}\"")
    @ValueSource(strings = {"02", "03", ""})
    @DisplayName("⭐ Giao dịch 00 nhưng KHÔNG phải loại thanh toán (02/03 hoàn tiền, thiếu loại): không cộng ví — PENDING, KHÔNG completed, "
            + "quá hạn cũng không EXPIRED; đổi về loại 01 thì chốt")
    void giao_dich_khong_phai_thanh_toan_khong_chot_thanh_cong(String transactionType) {
        ProviderTransaction row = createPending(500_000, "203.0.113.7");
        String txnRef = fake.pay(row.getPaymentUrl()).get("vnp_TxnRef");
        // VNPay báo 'thành công' cho chính lần hoàn, kèm số tiền gốc — cộng ví theo nó là cộng tiền đã trả lại
        fake.setTransactionType(txnRef, transactionType);
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(failureCount()).as("lơ lửng là câu trả lời, không phải lỗi kỹ thuật").isEqualTo(failuresBefore);

        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 3_600);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).as("tiền có thể đã được hoàn: không bao giờ tự hết hạn").isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(overdueGauge()).isEqualTo(1.0);

        fake.setTransactionType(txnRef, "01");
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
    }

    // ------------------------------------------------------------------
    // Circuit breaker: tạo URL không gọi mạng nên không bị breaker chặn
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Breaker đang MỞ (querydr hỏng): lệnh thu mới VẪN ra event charge-pending với URL ký đúng (ký tại chỗ, không qua breaker), "
            + "còn hỏi trạng thái thì bị chặn — PENDING + đếm lỗi, VNPay không nhận yêu cầu nào")
    void breaker_mo_van_tao_duoc_url_nhung_van_chan_hoi_trang_thai() throws Exception {
        resilientProvider.circuitBreaker().transitionToOpenState();
        BankChargeCommand command = new BankChargeCommand(UUID.randomUUID(), UUID.randomUUID(), 90_000,
                ECurrency.VND, "203.0.113.7");
        try {
            sendCommand(command);

            ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_CHARGE_PENDING,
                    command.transactionId().toString());
            EventSchemas.assertValid(WalletTopics.EVT_BANK_CHARGE_PENDING, record.value());
            String paymentUrl = objectMapper.readTree(record.value()).path("payload").path("paymentUrl").asText();
            Map<String, String> params = fake.pay(paymentUrl);
            assertThat(params.get("vnp_Amount")).isEqualTo("9000000");
            assertThat(db.status(tenant, command.transactionId())).isEqualTo("PENDING");

            double failuresBefore = failureCount();
            assertThat(reconciler.reconcile()).isEqualTo(1);

            assertThat(resilientProvider.circuitBreaker().getState())
                    .isEqualTo(io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN);
            assertThat(fake.requests()).as("mạch mở: lời gọi querydr không ra khỏi process").isEmpty();
            assertThat(db.status(tenant, command.transactionId())).isEqualTo("PENDING");
            assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
            assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
            assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        } finally {
            resilientProvider.circuitBreaker().reset();
        }
    }

    // ------------------------------------------------------------------
    // Tài khoản merchant (vnp_TmnCode): phiên chỉ tra được dưới đúng tài khoản đã tạo ra nó
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đổi VNPAY_TMN_CODE khi còn lần thu PENDING quá hạn của tmn cũ: job KHÔNG hỏi (dưới tmn mới VNPay nói 91 = 'chưa thu'), "
            + "hàng nguyên vẹn, không bao giờ EXPIRED, gauge đếm nó; đúng tmn thì chốt SUCCEEDED")
    void doi_tmn_code_hang_cu_khong_bi_hoi_va_khong_het_han() {
        ProviderTransaction row = createPending(300_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());   // người dùng ĐÃ trả tiền ở VNPay
        db.setProviderAccount(row.getTransactionId(), "OLDTMN01");
        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 3_600);
        Map<String, Object> before = db.rowById(row.getId());
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).as("hàng của tài khoản khác không được nhặt").isZero();

        assertThat(fake.requests()).as("không hỏi VNPay bằng tmn mới về phiên của tmn cũ").isEmpty();
        assertThat(db.rowById(row.getId())).isEqualTo(before);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(failureCount()).isEqualTo(failuresBefore);
        assertThat(overdueGauge()).as("bị bỏ lại thì hiện ở gauge — một ca cần người xem").isEqualTo(1.0);

        db.setProviderAccount(row.getTransactionId(), TMN_CODE);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).as("đúng tài khoản: thành công vẫn thắng dù quá hạn").isEqualTo("SUCCEEDED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
    }

    @Test
    @DisplayName("Hàng legacy (provider_account NULL, tạo trước 2.2b) vẫn được hỏi và chốt")
    void hang_legacy_khong_co_tai_khoan_van_duoc_hoi() {
        ProviderTransaction row = createPending(120_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());
        db.setProviderAccount(row.getTransactionId(), null);

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("Lệnh thu gửi lại cho hàng NEW của tmn khác: vào DLT với BANK_PROVIDER_MISMATCH nguyên văn, hàng NEW nguyên vẹn, không event")
    void lenh_thu_gui_lai_cho_hang_new_cua_tmn_khac_vao_dlt() {
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.randomUUID();
        db.insertNewRowOfProvider(tenant, transactionId, walletId, 30_000, "vnpay");
        db.setProviderAccount(transactionId, "OLDTMN01");
        Map<String, Object> before = db.row(tenant, transactionId);
        String json = BankFixtures.envelopeJson(objectMapper, tenant, BankFixtures.charge(transactionId, walletId, 30_000));

        bus.send(CMD_TOPIC, walletId.toString(), json);

        ConsumerRecord<String, String> dead = bus.await(WalletTopics.CMD_BANK_CHARGE + WalletTopics.DLT_SUFFIX,
                transactionId.toString());
        assertThat(dead.value()).isEqualTo(json);
        assertThat(BankKafka.header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("BANK_PROVIDER_MISMATCH");
        assertThat(db.row(tenant, transactionId)).isEqualTo(before);
        assertThat(db.outboxCountAnyTopic(transactionId)).isZero();
        assertThat(fake.requests()).isEmpty();
    }

    // ------------------------------------------------------------------
    // Hỏi xác nhận sau EXPIRED: tiền về muộn thì kêu, không lật trạng thái
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Hết hạn (VNPay nói 91) rồi người dùng trả tiền MUỘN: lần hỏi xác nhận tới hạn → expired_needs_review +1, hàng VẪN FAILED(EXPIRED), "
            + "không completed; hàng EXPIRED không ai trả thì không kêu")
    void tien_ve_muon_sau_het_han_vnpay_duoc_phat_hien() {
        ProviderTransaction paidLate = createPending(200_000, "203.0.113.7");
        ProviderTransaction neverPaid = createPending(210_000, "203.0.113.8");
        for (ProviderTransaction row : new ProviderTransaction[]{paidLate, neverPaid}) {
            db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 60);
        }
        assertThat(reconciler.reconcile()).isEqualTo(2);
        for (ProviderTransaction row : new ProviderTransaction[]{paidLate, neverPaid}) {
            assertThat(db.status(tenant, row.getTransactionId())).as("tiền điều kiện: 91 + quá hạn + grace").isEqualTo("FAILED");
            assertThat(db.failureCode(tenant, row.getTransactionId())).isEqualTo("EXPIRED");
        }

        Map<String, String> params = fake.pay(paidLate.getPaymentUrl());   // trả muộn, cùng CreateDate
        db.makeExpiredConfirmationDue(paidLate.getTransactionId());
        db.makeExpiredConfirmationDue(neverPaid.getTransactionId());
        double reviewBefore = reviewCount();
        int queriesBefore = fake.requests().size();

        assertThat(reconciler.reconcile()).isZero();

        assertThat(fake.requests()).as("mỗi hàng EXPIRED được hỏi xác nhận đúng một lần").hasSize(queriesBefore + 2);
        assertThat(reviewCount() - reviewBefore).as("chỉ hàng đã trả muộn").isEqualTo(1.0);
        assertThat(db.status(tenant, paidLate.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, paidLate.getTransactionId())).isEqualTo("EXPIRED");
        assertThat(db.outboxEventIds(paidLate.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(paidLate.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
        assertThat(db.nextCheckIsNull(paidLate.getTransactionId())).isTrue();
        assertThat(db.nextCheckIsNull(neverPaid.getTransactionId())).isTrue();
        assertThat(params.get("vnp_CreateDate")).isEqualTo(db.providerCreatedAtAsVnpayDate(paidLate.getTransactionId()));
        assertThat(fake.requests().stream().skip(queriesBefore).map(q -> q.get("vnp_TransactionDate")))
                .as("hỏi xác nhận cũng dùng đúng mốc tạo phiên của từng hàng")
                .contains(params.get("vnp_CreateDate"));

        assertThat(reconciler.reconcile()).isZero();
        assertThat(reviewCount() - reviewBefore).as("không hỏi lại, không đếm lại").isEqualTo(1.0);
    }

    // ------------------------------------------------------------------
    // Log: che dữ liệu nhạy cảm đầu-cuối (logback-spring.xml + converter của common-lib)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Log ra console đã che: vnp_SecureHash=*** và số thẻ hợp lệ Luhn (giữ 4 số cuối), giá trị gốc không xuất hiện")
    void log_che_chu_ky_va_so_the(CapturedOutput output) {
        log.info("Thử log: vnp_SecureHash=abcdef0123 thẻ 4111111111111111 hết");

        assertThat(output.getAll())
                .contains("vnp_SecureHash=***")
                .contains("***1111")
                .doesNotContain("abcdef0123")
                .doesNotContain("4111111111111111");
    }

    @Test
    @DisplayName("Exception có URL thanh toán đã ký trong message: cả dòng message lẫn stack trace đều che chữ ký thật")
    void log_exception_chua_url_da_ky_duoc_che(CapturedOutput output) {
        ProviderTransaction row = createPending(60_000, "203.0.113.7");
        String signature = fake.pay(row.getPaymentUrl()).get("vnp_SecureHash");
        assertThat(signature).as("tiền điều kiện: chữ ký thật dài 128 ký tự hex").hasSize(128);

        log.warn("Không mở được {}", row.getPaymentUrl());
        log.error("Lỗi gọi VNPay", new IllegalStateException("GET " + row.getPaymentUrl() + " thất bại"));

        assertThat(output.getAll())
                .doesNotContain(signature)
                .contains("vnp_SecureHash=***")
                .as("stack trace vẫn được in (chỉ bị che chứ không bị nuốt)")
                .contains("IllegalStateException")
                .contains("VnPayChargeIT");
    }

    @Test
    @DisplayName("Chạy cả luồng thu + đối soát: hash secret không xuất hiện trong log")
    void luong_thu_khong_lo_hash_secret_ra_log(CapturedOutput output) {
        ProviderTransaction row = createPending(60_000, "203.0.113.7");
        fake.pay(row.getPaymentUrl());
        fake.tamperSignature(true);
        reconciler.reconcile();
        fake.tamperSignature(false);
        reconciler.reconcile();

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(output.getAll()).doesNotContain(HASH_SECRET);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private void sendCommand(BankChargeCommand command) {
        bus.send(CMD_TOPIC, command.walletId().toString(), BankFixtures.envelopeJson(objectMapper, tenant, command));
    }

    /** Tạo một lần thu PENDING qua service (URL thanh toán đã ký, chưa ai mở). */
    private ProviderTransaction createPending(long amountMinor, String clientIp) {
        BankChargeCommand command = new BankChargeCommand(UUID.randomUUID(), UUID.randomUUID(), amountMinor,
                ECurrency.VND, clientIp);
        ProviderTransaction row = TenantContext.callWith(tenant, () -> chargeService.charge(command));
        assertThat(row.getProviderRef()).as("tiền điều kiện: lần thu phải PENDING").isNotNull();
        assertThat(row.getPaymentUrl()).startsWith(fake.payUrl());
        return row;
    }

    private double reviewCount() {
        return meterRegistry.get(NEEDS_REVIEW_METRIC).counter().count();
    }

    private double failureCount() {
        return meterRegistry.get(FAILURE_METRIC).counter().count();
    }

    private double amountMismatchCount() {
        return meterRegistry.get(AMOUNT_MISMATCH_METRIC).counter().count();
    }

    private double overdueGauge() {
        return meterRegistry.get(OVERDUE_GAUGE).gauge().value();
    }
}
