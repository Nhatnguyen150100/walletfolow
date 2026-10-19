package com.walletflow.bank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.bank.services.ChargeService;
import com.walletflow.bank.services.ChargeStatusService;
import com.walletflow.bank.services.PendingChargeReconciler;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.BankFixtures;
import com.walletflow.bank.support.BankKafka;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.ConflictException;
import com.walletflow.common.tenant.TenantContext;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Vòng đời hết hạn của lần thu PENDING (ADR-017): lần thu chỉ kết thúc {@code FAILED(EXPIRED)} khi CẢ HAI cùng đúng —
 * đã quá hạn phiên + grace (đồng hồ DB) VÀ provider vừa khẳng định "chưa thu". Provider không trả lời, hay trả lời
 * lơ lửng, thì quá hạn bao lâu cũng KHÔNG hết hạn: biến "không biết" thành "thất bại" là cách người đã trả tiền nhận
 * báo thất bại (roadmap §5.7). Thành công sau hạn vẫn thắng.
 *
 * <p>Dùng MockProvider (điều khiển được trạng thái phiên); hàng được làm "quá hạn" bằng SQL trên {@code expires_at},
 * không chờ giờ thật. Có thêm backoff cấp số của nhịp hỏi lại và gauge {@code walletflow.bank.pending_overdue}.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "outbox.relay.delay-ms=300",
        "walletflow.bank.provider=mock",
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        "walletflow.bank.status-check.interval-ms=3600000",
        // 30s -> 60s -> 120s rồi chạm trần 2 phút
        "walletflow.bank.status-check.recheck-after=PT30S",
        "walletflow.bank.status-check.max-recheck-after=PT2M",
        "walletflow.bank.session.ttl=PT15M",
        "walletflow.bank.session.expiry-grace=PT15M",
        // Sau EXPIRED hỏi xác nhận một lần sau chừng này (ADR-017) — phát hiện tiền về muộn
        "walletflow.bank.session.expired-confirm-after=PT1H",
        "walletflow.bank.status-check.max-confirm-attempts=3",
        // Nhịp hỏi xác nhận: 1h rồi chạm trần 2h — test đẩy next_check_at về quá khứ nên không chờ giờ thật
        "walletflow.bank.status-check.max-confirm-recheck-after=PT2H",
        "walletflow.bank.resilience.timeout=1s"
})
@Testcontainers
class PendingExpiryIT {

    /** Phải khớp walletflow.bank.session.expiry-grace ở trên. */
    private static final long GRACE_SECONDS = 900;
    /** Phải khớp walletflow.bank.session.expired-confirm-after ở trên. */
    private static final long CONFIRM_SECONDS = 3600;
    /** Phải khớp walletflow.bank.status-check.max-confirm-attempts ở trên. */
    private static final int MAX_CONFIRM_ATTEMPTS = 3;
    private static final String NEEDS_REVIEW_METRIC = "walletflow.bank.expired_needs_review";
    private static final String FAILURE_METRIC = "walletflow.bank.status_check.failures";
    private static final String OVERDUE_GAUGE = "walletflow.bank.pending_overdue";
    private static final String UNCONFIRMED_METRIC = "walletflow.bank.expired_unconfirmed";

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
    ChargeService chargeService;
    @Autowired
    ChargeStatusService chargeStatusService;
    @Autowired
    ProviderTransactionRepository repository;
    @Autowired
    ResilientPaymentProvider resilientProvider;
    @Autowired
    MockProvider mock;
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
        mock.reset();
        resilientProvider.circuitBreaker().reset();
        tenant = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Hết hạn: cả hai điều kiện
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Phiên bỏ dở quá hạn + grace: FAILED(EXPIRED) với đúng MỘT event failed; trong hạn, hoặc quá hạn nhưng còn trong grace thì vẫn PENDING")
    void phien_bo_do_qua_han_cong_grace_het_han_dung_mot_event() throws Exception {
        ProviderTransaction abandoned = createPending(tenant, 11_000);
        ProviderTransaction withinValidity = createPending(tenant, 22_000);
        ProviderTransaction withinGrace = createPending(tenant, 33_000);
        db.makeOverdue(abandoned.getTransactionId(), GRACE_SECONDS, 60);    // quá hạn + grace 1 phút
        db.makeOverdue(withinGrace.getTransactionId(), GRACE_SECONDS, -60); // quá hạn nhưng còn 1 phút grace
        for (ProviderTransaction row : new ProviderTransaction[]{abandoned, withinValidity, withinGrace}) {
            db.makeDue(row.getTransactionId(), 5);
        }
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(3);

        assertThat(db.status(tenant, abandoned.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, abandoned.getTransactionId())).isEqualTo("EXPIRED");
        // Hợp đồng ĐÃ ĐỔI có chủ ý (ADR-017): hết hạn không còn xoá next_check_at mà đặt lịch MỘT lần hỏi xác nhận
        // sau expired-confirm-after (1 giờ), để phát hiện tiền về muộn
        assertThat(db.secondsUntilNextCheck(abandoned.getTransactionId()))
                .as("hết hạn đặt lịch hỏi xác nhận sau expired-confirm-after")
                .isBetween(CONFIRM_SECONDS - 5.0, CONFIRM_SECONDS + 0.5);
        ConsumerRecord<String, String> record = bus.await(WalletTopics.EVT_BANK_FAILED,
                abandoned.getTransactionId().toString());
        EventSchemas.assertValid(WalletTopics.EVT_BANK_FAILED, record.value());
        JsonNode payload = objectMapper.readTree(record.value()).path("payload");
        assertThat(payload.path("failureCode").asText()).isEqualTo("EXPIRED");
        assertThat(payload.path("providerRef").asText()).isEqualTo(abandoned.getProviderRef());
        assertThat(payload.path("operation").asText()).isEqualTo("CHARGE");
        assertThat(db.outboxEventIds(abandoned.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
        assertThat(db.outboxEventIds(abandoned.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();

        assertThat(db.status(tenant, withinValidity.getTransactionId()))
                .as("còn trong hạn phiên: người dùng vẫn có thể trả").isEqualTo("PENDING");
        assertThat(db.status(tenant, withinGrace.getTransactionId()))
                .as("đã quá hạn phiên nhưng grace còn: tiền ngân hàng đã trừ có thể chưa kịp về provider").isEqualTo("PENDING");
        for (ProviderTransaction alive : new ProviderTransaction[]{withinValidity, withinGrace}) {
            assertThat(db.outboxEventIds(alive.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
            assertThat(db.outboxEventIds(alive.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        }
        assertThat(failureCount()).as("hết hạn là kết luận, không phải lỗi kỹ thuật").isEqualTo(failuresBefore);

        // Hàng đã kết thúc không bị nhặt lại, nên không phát event thứ hai
        db.makeDue(withinValidity.getTransactionId(), 5);
        db.makeDue(withinGrace.getTransactionId(), 5);
        assertThat(reconciler.reconcile()).isEqualTo(2);
        assertThat(db.outboxEventIds(abandoned.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
    }

    @Test
    @DisplayName("Hàng không có hạn phiên (expires_at NULL, tạo trước 2.2b): không bao giờ hết hạn và không tính vào gauge pending_overdue")
    void hang_khong_co_han_phien_khong_bao_gio_het_han() {
        ProviderTransaction legacy = createPending(tenant, 9_000);
        db.setExpiresInSeconds(legacy.getTransactionId(), null);
        db.makeDue(legacy.getTransactionId(), 5);

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, legacy.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(legacy.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(overdueGauge()).isZero();
    }

    @Test
    @DisplayName("⭐ Provider không trả lời được khi hàng đã quá hạn + grace: vẫn PENDING, đếm lỗi, không event, gauge = 1; "
            + "provider hồi và khẳng định chưa thu thì mới EXPIRED")
    void provider_khong_tra_loi_thi_khong_bao_gio_het_han() {
        ProviderTransaction row = createPending(tenant, 15_000);
        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 3_600);
        db.makeDue(row.getTransactionId(), 5);
        mock.failNextCalls(1);
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId()))
                .as("'không biết' không bao giờ được biến thành 'thất bại', quá hạn bao lâu cũng vậy")
                .isEqualTo("PENDING");
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(overdueGauge()).as("quá hạn + grace mà chưa kết luận được = một ca cần người xem").isEqualTo(1.0);

        // Provider hồi lại, khẳng định phiên chưa được trả: bây giờ mới được kết thúc
        db.makeDue(row.getTransactionId(), 5);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, row.getTransactionId())).isEqualTo("EXPIRED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
        assertThat(overdueGauge()).as("đã kết thúc thì không còn quá hạn").isZero();
    }

    @Test
    @DisplayName("⭐ Provider báo UNRESOLVED (lơ lửng) khi đã quá hạn + grace: không hết hạn; sau đó provider chốt SUCCEEDED thì SUCCEEDED + completed")
    void unresolved_qua_han_khong_het_han_roi_chot_thanh_cong() {
        ProviderTransaction row = createPending(tenant, 18_000);
        mock.settle(row.getProviderRef(), EProviderState.UNRESOLVED);
        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 3_600);
        db.makeDue(row.getTransactionId(), 5);
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(failureCount()).as("lơ lửng là câu trả lời, không phải lỗi kỹ thuật").isEqualTo(failuresBefore);
        assertThat(overdueGauge()).isEqualTo(1.0);

        db.makeDue(row.getTransactionId(), 5);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).as("hỏi lại vẫn lơ lửng, vẫn không hết hạn").isEqualTo("PENDING");

        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        db.makeDue(row.getTransactionId(), 5);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(overdueGauge()).isZero();
    }

    @Test
    @DisplayName("⭐ Người dùng trả tiền SAU hạn phiên (hàng đã quá hạn + grace): thành công vẫn thắng -> SUCCEEDED + completed, không EXPIRED")
    void tra_tien_sau_han_thanh_cong_van_thang() {
        ProviderTransaction row = createPending(tenant, 25_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 3_600);
        db.makeDue(row.getTransactionId(), 5);

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(db.failureCode(tenant, row.getTransactionId())).isNull();
        assertThat(overdueGauge()).isZero();
    }

    @Test
    @DisplayName("Provider KHẲNG ĐỊNH phiên hỏng (FAILED) khi hàng đã quá hạn: giữ mã PAYMENT_FAILED của provider, không bị ghi đè thành EXPIRED")
    void provider_bao_hong_giu_ma_cua_provider_khong_thanh_expired() {
        ProviderTransaction row = createPending(tenant, 25_000);
        mock.settle(row.getProviderRef(), EProviderState.FAILED);
        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 3_600);
        db.makeDue(row.getTransactionId(), 5);

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, row.getTransactionId())).isEqualTo("PAYMENT_FAILED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
    }

    // ------------------------------------------------------------------
    // Backoff
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Mỗi lần hỏi lùi hạn theo cấp số: 30s, 60s, 120s rồi chạm trần 120s; check_attempts tăng đúng 1 mỗi lần")
    void backoff_cap_so_nhan_doi_roi_chan_tran() {
        ProviderTransaction row = createPending(tenant, 12_000);
        assertThat(db.checkAttempts(row.getTransactionId())).as("hàng mới sang PENDING bắt đầu từ 0").isZero();
        double[] expectedSeconds = {30, 60, 120, 120, 120};

        for (int round = 0; round < expectedSeconds.length; round++) {
            db.makeDue(row.getTransactionId(), 1);
            assertThat(reconciler.reconcile()).as("lượt %d", round + 1).isEqualTo(1);

            assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
            assertThat(db.checkAttempts(row.getTransactionId())).as("lượt %d", round + 1).isEqualTo(round + 1);
            // Đọc sau lúc job chạy vài chục ms: giá trị thật nhỏ hơn mong đợi một chút, không bao giờ lớn hơn
            assertThat(db.secondsUntilNextCheck(row.getTransactionId()))
                    .as("lượt %d: hạn hỏi lại kế tiếp", round + 1)
                    .isBetween(expectedSeconds[round] - 5, expectedSeconds[round] + 0.5);
        }
    }

    @Test
    @DisplayName("Hàng NEW có check_attempts cũ được tạo phiên (markPending): check_attempts về 0 — backoff bắt đầu lại")
    void sang_pending_dat_lai_bo_dem_backoff() {
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.randomUUID();
        db.insertNewRowOfProvider(tenant, transactionId, walletId, 30_000, "mock");
        db.setCheckAttempts(transactionId, 5);
        assertThat(db.checkAttempts(transactionId)).isEqualTo(5);

        ProviderTransaction row = TenantContext.callWith(tenant,
                () -> chargeService.charge(BankFixtures.charge(transactionId, walletId, 30_000)));

        assertThat(row.getProviderRef()).isNotNull();
        assertThat(db.status(tenant, transactionId)).isEqualTo("PENDING");
        assertThat(db.checkAttempts(transactionId)).isZero();
        assertThat(db.secondsUntilNextCheck(transactionId)).isBetween(25.0, 30.5);
        // expires_at do đồng hồ ỨNG DỤNG đặt, now() là đồng hồ DB (container): lệch vài giây là chuyện thường, nên
        // cận trên nới thêm 5s để test không đỏ vì lệch đồng hồ (không phải vì lỗi logic)
        assertThat(db.secondsUntilExpiry(transactionId)).as("hạn phiên = ttl 15 phút").isBetween(14 * 60.0, 15 * 60.0 + 5);
    }

    // ------------------------------------------------------------------
    // Gauge pending_overdue + tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Gauge pending_overdue đếm hàng PENDING quá hạn + grace XUYÊN tenant (kể cả hàng chưa tới hạn hỏi lại), "
            + "và giảm khi chúng được kết thúc")
    void gauge_dem_hang_qua_han_xuyen_tenant_va_giam_khi_het_han() {
        UUID tenantX = UUID.randomUUID();
        UUID tenantY = UUID.randomUUID();
        ProviderTransaction overdueX = createPending(tenantX, 10_000);
        ProviderTransaction overdueY = createPending(tenantY, 20_000);
        ProviderTransaction fresh = createPending(tenantX, 30_000);
        ProviderTransaction withinGrace = createPending(tenantY, 40_000);
        db.makeOverdue(overdueX.getTransactionId(), GRACE_SECONDS, 10);
        db.makeOverdue(overdueY.getTransactionId(), GRACE_SECONDS, 10);
        db.makeOverdue(withinGrace.getTransactionId(), GRACE_SECONDS, -10);

        // Chưa hàng nào tới hạn hỏi lại (next_check_at = +30s): lượt này không hỏi ai nhưng vẫn cập nhật gauge
        assertThat(reconciler.reconcile()).isZero();
        assertThat(overdueGauge()).as("hai hàng quá hạn + grace, ở hai tenant").isEqualTo(2.0);

        db.makeDue(overdueX.getTransactionId(), 5);
        db.makeDue(overdueY.getTransactionId(), 5);
        assertThat(reconciler.reconcile()).isEqualTo(2);

        assertThat(db.status(tenantX, overdueX.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.status(tenantY, overdueY.getTransactionId())).isEqualTo("FAILED");
        assertThat(overdueGauge()).as("hàng vừa hết hạn trong lượt này không còn bị tính").isZero();
        assertThat(db.status(tenantX, fresh.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.status(tenantY, withinGrace.getTransactionId())).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("Hai tenant cùng có hàng hết hạn: mỗi event failed EXPIRED nằm trong ĐÚNG tenant của hàng (tenant_id outbox), không rò chéo")
    void het_han_dung_tenant() {
        UUID tenantX = UUID.randomUUID();
        UUID tenantY = UUID.randomUUID();
        ProviderTransaction x = createPending(tenantX, 10_000);
        ProviderTransaction y = createPending(tenantY, 20_000);
        for (ProviderTransaction row : new ProviderTransaction[]{x, y}) {
            db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 60);
            db.makeDue(row.getTransactionId(), 5);
        }

        assertThat(reconciler.reconcile()).isEqualTo(2);

        assertThat(db.status(tenantX, x.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.status(tenantY, y.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenantX, x.getTransactionId())).isEqualTo("EXPIRED");
        assertThat(db.failureCode(tenantY, y.getTransactionId())).isEqualTo("EXPIRED");
        assertThat(db.outbox(x.getTransactionId(), WalletTopics.EVT_BANK_FAILED))
                .extracting(row -> row.get("tenant_id")).containsExactly(tenantX);
        assertThat(db.outbox(y.getTransactionId(), WalletTopics.EVT_BANK_FAILED))
                .extracting(row -> row.get("tenant_id")).containsExactly(tenantY);
        assertThat(db.outboxEventIds(x.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(y.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(TenantContext.current()).as("job trả lại tenant, không để dính vào luồng").isEmpty();
    }

    // ------------------------------------------------------------------
    // Tài khoản merchant: phiên chỉ hỏi được dưới ĐÚNG tài khoản đã tạo ra nó (ADR-017)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Hàng PENDING quá hạn của tài khoản merchant KHÁC: job không nhặt (không hỏi provider, hàng nguyên vẹn kể cả next_check_at), "
            + "không bao giờ hết hạn, gauge pending_overdue đếm nó; hàng đúng tài khoản và hàng legacy (NULL) vẫn được xử lý")
    void hang_cua_tai_khoan_khac_khong_bi_nhat_khong_het_han_va_duoc_dem() {
        ProviderTransaction own = createPending(tenant, 10_000);
        ProviderTransaction legacy = createPending(tenant, 11_000);
        // Mock BIẾT phiên này và nói PENDING: nếu job nhặt nhầm nó, hàng quá hạn sẽ bị kết thúc EXPIRED
        ProviderTransaction foreign = createPending(tenant, 12_000);
        assertThat(db.providerAccount(tenant, own.getTransactionId()))
                .as("lúc tạo phiên hàng được ghi tài khoản của provider").isEqualTo("mock");
        db.setProviderAccount(legacy.getTransactionId(), null);
        db.setProviderAccount(foreign.getTransactionId(), "other-merchant");
        mock.settle(own.getProviderRef(), EProviderState.SUCCEEDED);
        mock.settle(legacy.getProviderRef(), EProviderState.SUCCEEDED);
        for (ProviderTransaction row : new ProviderTransaction[]{own, legacy, foreign}) {
            db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 3_600);
            db.makeDue(row.getTransactionId(), 5);
        }
        Map<String, Object> foreignBefore = db.rowById(foreign.getId());
        int providerCallsBefore = resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls();
        double failuresBefore = failureCount();

        int asked = reconciler.reconcile();

        assertThat(asked).as("chỉ hàng đúng tài khoản và hàng legacy được hỏi").isEqualTo(2);
        assertThat(db.status(tenant, own.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.status(tenant, legacy.getTransactionId())).as("NULL = hàng trước 2.2b: coi là hỏi được").isEqualTo("SUCCEEDED");
        assertThat(db.rowById(foreign.getId())).as("hàng của tài khoản khác nguyên vẹn đến từng cột").isEqualTo(foreignBefore);
        assertThat(db.status(tenant, foreign.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(foreign.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(db.outboxEventIds(foreign.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls() - providerCallsBefore)
                .as("provider chỉ bị hỏi cho hai hàng hợp lệ").isEqualTo(2);
        assertThat(failureCount()).isEqualTo(failuresBefore);
        assertThat(overdueGauge()).as("hàng bị bỏ lại hiện ở gauge — một ca cần người xem").isEqualTo(1.0);

        assertThat(reconciler.reconcile()).isZero();
        assertThat(db.rowById(foreign.getId())).isEqualTo(foreignBefore);
        assertThat(overdueGauge()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("refresh/confirmExpired một hàng của tài khoản merchant khác: IllegalStateException, KHÔNG hỏi provider, hàng không đổi, không event")
    void refresh_hang_cua_tai_khoan_khac_bi_tu_choi_khong_hoi_provider() {
        ProviderTransaction created = createPending(tenant, 7_000);
        mock.settle(created.getProviderRef(), EProviderState.SUCCEEDED);   // nếu bị hỏi nhầm sẽ ra 'đã thu'
        db.setProviderAccount(created.getTransactionId(), "other-merchant");
        ProviderTransaction foreign = repository.findByTenantIdAndId(tenant, created.getId()).orElseThrow();
        Map<String, Object> before = db.rowById(created.getId());
        int providerCallsBefore = resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls();

        assertThatThrownBy(() -> TenantContext.callWith(tenant, () -> chargeStatusService.refresh(foreign)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tài khoản")
                .hasMessageContaining("other-merchant");

        assertThat(db.rowById(created.getId())).isEqualTo(before);
        assertThat(db.outboxEventIds(created.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();

        // Cùng chốt chặn cho lần hỏi xác nhận sau hết hạn
        db.forceFailed(created.getTransactionId(), "EXPIRED", true);
        ProviderTransaction expiredForeign = repository.findByTenantIdAndId(tenant, created.getId()).orElseThrow();
        Map<String, Object> expiredBefore = db.rowById(created.getId());
        double reviewBefore = reviewCount();

        assertThatThrownBy(() -> TenantContext.runWith(tenant, () -> chargeStatusService.confirmExpired(expiredForeign)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("other-merchant");

        assertThat(db.rowById(created.getId())).isEqualTo(expiredBefore);
        assertThat(reviewCount()).isEqualTo(reviewBefore);
        assertThat(resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls())
                .as("provider không bị gọi lần nào").isEqualTo(providerCallsBefore);
    }

    @Test
    @DisplayName("Gửi lại lệnh thu cho hàng NEW của tài khoản merchant khác: ConflictException BANK_PROVIDER_MISMATCH, hàng NEW nguyên vẹn, provider không bị gọi")
    void gui_lai_lenh_cho_hang_new_cua_tai_khoan_khac_bi_tu_choi() {
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.randomUUID();
        db.insertNewRowOfProvider(tenant, transactionId, walletId, 30_000, "mock");
        db.setProviderAccount(transactionId, "other-merchant");
        Map<String, Object> before = db.row(tenant, transactionId);

        assertThatThrownBy(() -> TenantContext.callWith(tenant,
                () -> chargeService.charge(BankFixtures.charge(transactionId, walletId, 30_000))))
                .isInstanceOfSatisfying(ConflictException.class,
                        conflict -> assertThat(conflict.getErrorCode()).isEqualTo("BANK_PROVIDER_MISMATCH"));

        assertThat(db.row(tenant, transactionId)).isEqualTo(before);
        assertThat(db.status(tenant, transactionId)).isEqualTo("NEW");
        assertThat(mock.chargeCalls()).as("không tạo phiên ở provider").isZero();
        assertThat(db.outboxCountAnyTopic(transactionId)).isZero();
    }

    // ------------------------------------------------------------------
    // Hỏi xác nhận sau khi hết hạn: tiền về muộn thì kêu, KHÔNG tự đổi trạng thái (ADR-017)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Sau EXPIRED provider báo ĐÃ THU: expired_needs_review +1, hàng VẪN FAILED(EXPIRED), KHÔNG bank.event.completed, "
            + "next_check_at về NULL và không bị hỏi lại")
    void tien_ve_muon_sau_het_han_duoc_phat_hien_khong_lat_trang_thai() {
        ProviderTransaction row = expire(tenant, 40_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);   // người dùng đã trả, ngân hàng báo chậm
        db.makeExpiredConfirmationDue(row.getTransactionId());
        double reviewBefore = reviewCount();

        assertThat(reconciler.reconcile()).as("hỏi xác nhận không tính vào số hàng PENDING").isZero();

        assertThat(reviewCount() - reviewBefore).isEqualTo(1.0);
        assertThat(db.status(tenant, row.getTransactionId())).as("event failed đã phát, saga đã kết thúc: không lật lại").isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, row.getTransactionId())).isEqualTo("EXPIRED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
        assertThat(db.nextCheckIsNull(row.getTransactionId())).as("đã có câu trả lời xác nhận: thôi hỏi").isTrue();

        assertThat(reconciler.reconcile()).isZero();
        assertThat(reviewCount() - reviewBefore).as("không hỏi lại, không đếm lại").isEqualTo(1.0);
    }

    @Test
    @DisplayName("Sau EXPIRED provider vẫn nói CHƯA THU: không kêu (counter không đổi), next_check_at về NULL — đã hỏi đúng một lần")
    void xac_nhan_van_chua_thu_khong_keu() {
        ProviderTransaction row = expire(tenant, 40_000);
        db.makeExpiredConfirmationDue(row.getTransactionId());
        double reviewBefore = reviewCount();
        int providerCallsBefore = resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls();

        assertThat(reconciler.reconcile()).isZero();

        assertThat(resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls() - providerCallsBefore)
                .as("provider được hỏi đúng một lần").isEqualTo(1);
        assertThat(reviewCount()).isEqualTo(reviewBefore);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.nextCheckIsNull(row.getTransactionId())).isTrue();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
    }

    @Test
    @DisplayName("Hỏi xác nhận mà provider không trả lời: counter không đổi, đếm lỗi +1, lùi hạn theo nhịp XÁC NHẬN "
            + "(expired-confirm-after, không phải nhịp PENDING) chứ không bỏ; hồi lại thì phát hiện")
    void xac_nhan_provider_khong_tra_loi_thi_hoi_lai_sau() {
        ProviderTransaction row = expire(tenant, 40_000);
        db.makeExpiredConfirmationDue(row.getTransactionId());
        mock.failNextCalls(1);
        double reviewBefore = reviewCount();
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isZero();

        assertThat(reviewCount()).isEqualTo(reviewBefore);
        assertThat(failureCount() - failuresBefore).isEqualTo(1.0);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.nextCheckIsNull(row.getTransactionId())).as("chưa có câu trả lời: phải còn lịch hỏi lại").isFalse();
        assertThat(db.secondsUntilNextCheck(row.getTransactionId()))
                .as("lượt xác nhận đầu lùi expired-confirm-after = 1 giờ (nhịp này rộng hơn nhịp PENDING rất "
                        + "nhiều vì tiền về muộn tính bằng giờ), và không dội mỗi lượt")
                .isBetween(CONFIRM_SECONDS - 5.0, CONFIRM_SECONDS + 0.5);
        assertThat(reconciler.reconcile()).isZero();
        assertThat(failureCount() - failuresBefore).as("chưa tới hạn: không hỏi lại").isEqualTo(1.0);

        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        db.makeExpiredConfirmationDue(row.getTransactionId());
        assertThat(reconciler.reconcile()).isZero();

        assertThat(reviewCount() - reviewBefore).isEqualTo(1.0);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.nextCheckIsNull(row.getTransactionId())).isTrue();
    }

    @Test
    @DisplayName("⭐ Provider không bao giờ xác nhận được: vòng hỏi dừng đúng sau max-confirm-attempts lượt, counter "
            + "expired_unconfirmed +1 — không dội provider mãi mãi trong im lặng")
    void vong_hoi_xac_nhan_co_tran_va_keu_khi_het_luot() {
        ProviderTransaction row = expire(tenant, 45_000);
        double failuresBefore = failureCount();
        double unconfirmedBefore = unconfirmedCount();
        mock.failNextCalls(100);   // provider hỏng hẳn với hàng này

        for (int round = 1; round < MAX_CONFIRM_ATTEMPTS; round++) {
            db.makeExpiredConfirmationDue(row.getTransactionId());
            assertThat(reconciler.reconcile()).as("lượt %d", round).isZero();
            assertThat(db.nextCheckIsNull(row.getTransactionId()))
                    .as("lượt %d: còn hạn mức thì phải còn lịch hỏi lại", round).isFalse();
            assertThat(unconfirmedCount()).as("lượt %d: chưa hết lượt thì chưa kêu", round).isEqualTo(unconfirmedBefore);
        }

        db.makeExpiredConfirmationDue(row.getTransactionId());
        assertThat(reconciler.reconcile()).as("lượt cuối").isZero();

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, row.getTransactionId())).isEqualTo("EXPIRED");
        assertThat(failureCount() - failuresBefore)
                .as("mỗi lượt hỏi hỏng đếm đúng một lỗi").isEqualTo((double) MAX_CONFIRM_ATTEMPTS);
        assertThat(db.nextCheckIsNull(row.getTransactionId()))
                .as("hết lượt thì thôi hỏi — không còn được lấy vào lô").isTrue();
        assertThat(unconfirmedCount() - unconfirmedBefore)
                .as("hàng không xác nhận được phải kêu đúng một lần, không biến mất im lặng").isEqualTo(1.0);

        // Không makeExpiredConfirmationDue nữa: hàng phải nằm im kể cả khi job chạy tiếp
        double failuresAfterCap = failureCount();
        assertThat(reconciler.reconcile()).isZero();
        assertThat(failureCount()).as("không hỏi provider nữa").isEqualTo(failuresAfterCap);
        assertThat(unconfirmedCount() - unconfirmedBefore).as("không kêu lại").isEqualTo(1.0);
    }

    @Test
    @DisplayName("⭐ Câu trả lời tới đúng LƯỢT CUỐI (ca biên): hàng coi như đã xác nhận, counter expired_unconfirmed KHÔNG tăng")
    void xac_nhan_duoc_o_luot_cuoi_thi_khong_keu() {
        ProviderTransaction row = expire(tenant, 46_000);
        double unconfirmedBefore = unconfirmedCount();
        mock.failNextCalls(MAX_CONFIRM_ATTEMPTS - 1);

        for (int round = 1; round < MAX_CONFIRM_ATTEMPTS; round++) {
            db.makeExpiredConfirmationDue(row.getTransactionId());
            assertThat(reconciler.reconcile()).isZero();
        }

        // Lượt cuối: provider trả lời được — đây đúng lúc capExpiredConfirmations cũng chạm trần
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        db.makeExpiredConfirmationDue(row.getTransactionId());
        double reviewBefore = reviewCount();
        assertThat(reconciler.reconcile()).isZero();

        assertThat(reviewCount() - reviewBefore).as("tiền về muộn vẫn được phát hiện").isEqualTo(1.0);
        assertThat(db.nextCheckIsNull(row.getTransactionId())).isTrue();
        assertThat(unconfirmedCount() - unconfirmedBefore)
                .as("đã có câu trả lời, chỉ là trả lời ở lượt chót — không phải ca 'không biết gì'").isZero();
    }

    @Test
    @DisplayName("⭐ Hàng EXPIRED có sẵn từ trước V3 (check_attempts lớn của giai đoạn PENDING): vẫn được hỏi đủ hạn mức, "
            + "không bị kết luận hết lượt ngay và không kêu oan")
    void hang_expired_cu_khong_bi_coi_la_het_luot() {
        ProviderTransaction row = expire(tenant, 47_000);
        // Dựng lại hàng kiểu cũ: bộ đếm giai đoạn PENDING còn nguyên, chưa từng có confirm_attempts
        db.setCheckAttempts(row.getTransactionId(), 9);
        double unconfirmedBefore = unconfirmedCount();
        mock.failNextCalls(100);

        db.makeExpiredConfirmationDue(row.getTransactionId());
        assertThat(reconciler.reconcile()).isZero();

        assertThat(db.nextCheckIsNull(row.getTransactionId()))
                .as("check_attempts của giai đoạn PENDING không được tiêu hạn mức hỏi xác nhận").isFalse();
        assertThat(unconfirmedCount()).isEqualTo(unconfirmedBefore);
        assertThat(db.checkAttempts(row.getTransactionId())).as("cột cũ không bị đụng tới").isEqualTo(9);
    }

    @Test
    @DisplayName("Sau EXPIRED provider báo UNRESOLVED (lơ lửng): expired_needs_review +1, hàng vẫn FAILED(EXPIRED), không event completed")
    void xac_nhan_unresolved_thi_keu() {
        ProviderTransaction row = expire(tenant, 40_000);
        mock.settle(row.getProviderRef(), EProviderState.UNRESOLVED);
        db.makeExpiredConfirmationDue(row.getTransactionId());
        double reviewBefore = reviewCount();

        assertThat(reconciler.reconcile()).isZero();

        assertThat(reviewCount() - reviewBefore).isEqualTo(1.0);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.nextCheckIsNull(row.getTransactionId())).isTrue();
    }

    @Test
    @DisplayName("⭐ Hỏi xác nhận chỉ đụng hàng FAILED(EXPIRED): hàng FAILED mã khác (PAYMENT_FAILED dù tới hạn, DECLINED NULL) nguyên vẹn; "
            + "hàng EXPIRED của hai tenant được hỏi trong ĐÚNG tenant của chúng")
    void xac_nhan_chi_dung_hang_expired_va_dung_tenant() {
        UUID tenantX = UUID.randomUUID();
        UUID tenantY = UUID.randomUUID();
        ProviderTransaction expiredX = expire(tenantX, 10_000);
        ProviderTransaction expiredY = expire(tenantY, 20_000);
        // Hai hàng FAILED mã khác; mock nói 'đã thu' cho cả hai — nếu bị hỏi nhầm, counter sẽ tăng
        ProviderTransaction paymentFailed = createPending(tenantX, 30_000);
        ProviderTransaction declined = createPending(tenantY, 40_000);
        for (ProviderTransaction row : new ProviderTransaction[]{expiredX, expiredY, paymentFailed, declined}) {
            mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        }
        db.forceFailed(paymentFailed.getTransactionId(), "PAYMENT_FAILED", true);
        db.forceFailed(declined.getTransactionId(), "DECLINED", false);
        db.makeExpiredConfirmationDue(expiredX.getTransactionId());
        db.makeExpiredConfirmationDue(expiredY.getTransactionId());
        Map<String, Object> paymentFailedBefore = db.rowById(paymentFailed.getId());
        Map<String, Object> declinedBefore = db.rowById(declined.getId());
        double reviewBefore = reviewCount();
        int providerCallsBefore = resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls();

        assertThat(reconciler.reconcile()).isZero();

        assertThat(reviewCount() - reviewBefore).as("đúng hai hàng EXPIRED").isEqualTo(2.0);
        assertThat(resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls() - providerCallsBefore)
                .as("provider chỉ bị hỏi cho hai hàng EXPIRED").isEqualTo(2);
        assertThat(db.rowById(paymentFailed.getId())).isEqualTo(paymentFailedBefore);
        assertThat(db.rowById(declined.getId())).isEqualTo(declinedBefore);
        assertThat(db.status(tenantX, expiredX.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.status(tenantY, expiredY.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.nextCheckIsNull(expiredX.getTransactionId())).isTrue();
        assertThat(db.nextCheckIsNull(expiredY.getTransactionId())).isTrue();
        assertThat(db.outbox(expiredX.getTransactionId(), WalletTopics.EVT_BANK_FAILED))
                .extracting(r -> r.get("tenant_id")).containsExactly(tenantX);
        assertThat(db.outbox(expiredY.getTransactionId(), WalletTopics.EVT_BANK_FAILED))
                .extracting(r -> r.get("tenant_id")).containsExactly(tenantY);
        assertThat(TenantContext.current()).as("job trả lại tenant, không để dính vào luồng").isEmpty();
    }

    @Test
    @DisplayName("⭐ Provider báo ĐÃ THU đúng lúc hàng vừa bị kết thúc EXPIRED (luồng đọc cũ còn PENDING): không lật lại, không completed, "
            + "nhưng expired_needs_review +1")
    void thanh_toan_toi_dung_luc_het_han_khong_lat_lai_nhung_keu() {
        ProviderTransaction stale = createPending(tenant, 55_000);   // bản đọc cũ: còn PENDING
        db.makeOverdue(stale.getTransactionId(), GRACE_SECONDS, 60);
        db.makeDue(stale.getTransactionId(), 5);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, stale.getTransactionId())).isEqualTo("FAILED");
        mock.settle(stale.getProviderRef(), EProviderState.SUCCEEDED);
        double reviewBefore = reviewCount();

        EProviderTxnStatus status = TenantContext.callWith(tenant, () -> chargeStatusService.refresh(stale));

        assertThat(status).as("luồng thua thấy trạng thái thật trong DB").isEqualTo(EProviderTxnStatus.FAILED);
        assertThat(reviewCount() - reviewBefore).isEqualTo(1.0);
        assertThat(db.status(tenant, stale.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.outboxEventIds(stale.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(stale.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    /** Đưa một lần thu bỏ dở tới FAILED(EXPIRED) bằng đường thật: quá hạn + grace, provider nói chưa thu. */
    private ProviderTransaction expire(UUID tenantId, long amountMinor) {
        ProviderTransaction row = createPending(tenantId, amountMinor);
        db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 60);
        db.makeDue(row.getTransactionId(), 5);
        assertThat(reconciler.reconcile()).as("tiền điều kiện: hàng quá hạn được hỏi").isGreaterThanOrEqualTo(1);
        assertThat(db.status(tenantId, row.getTransactionId())).as("tiền điều kiện").isEqualTo("FAILED");
        assertThat(db.failureCode(tenantId, row.getTransactionId())).isEqualTo("EXPIRED");
        return row;
    }

    private double reviewCount() {
        return meterRegistry.get(NEEDS_REVIEW_METRIC).counter().count();
    }

    /** Tạo một lần thu ở trạng thái PENDING (phiên đã tạo ở mock, chưa ai trả tiền). */
    private ProviderTransaction createPending(UUID tenantId, long amountMinor) {
        BankChargeCommand command = BankFixtures.freshCharge(amountMinor);
        ProviderTransaction row = TenantContext.callWith(tenantId, () -> chargeService.charge(command));
        assertThat(row.getProviderRef()).as("tiền điều kiện: lần thu phải PENDING").isNotNull();
        return row;
    }

    private double failureCount() {
        return meterRegistry.get(FAILURE_METRIC).counter().count();
    }

    private double overdueGauge() {
        return meterRegistry.get(OVERDUE_GAUGE).gauge().value();
    }

    private double unconfirmedCount() {
        return meterRegistry.get(UNCONFIRMED_METRIC).counter().count();
    }
}
