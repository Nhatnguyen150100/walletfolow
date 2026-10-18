package com.walletflow.bank;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.RefundRequest;
import com.walletflow.bank.provider.RefundResult;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.services.ChargeService;
import com.walletflow.bank.services.ChargeStatusService;
import com.walletflow.bank.services.PendingChargeReconciler;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.BankFixtures;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
 * Job đối chiếu là lưới an toàn của luồng thu: webhook có thể không bao giờ tới, nên một lần thu chỉ chắc
 * chắn kết thúc nhờ job này hỏi lại provider (ADR-016). Test gọi {@code reconcile()} trực tiếp (scheduler bị
 * đẩy ra xa) và làm hàng "tới hạn" bằng cách lùi {@code next_check_at} trong DB, không chờ giờ thật.
 *
 * <p>Cuối file có thêm các tính chất hoàn tiền của {@link PaymentProvider} (đường bù trừ của saga ở 2.4):
 * không hoàn trước khi thu xong, hoàn idempotent theo khoá, không bao giờ hoàn quá số đã thu.
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
        "walletflow.bank.status-check.interval-ms=3600000",
        // 30s thật: hàng vừa tạo CHƯA tới hạn, và "lùi hạn" sau lần hỏi nhìn thấy được (next_check_at > now())
        "walletflow.bank.status-check.recheck-after=PT30S",
        "walletflow.bank.resilience.timeout=1s"
})
@Testcontainers
class PendingChargeReconcilerIT {

    private static final String FAILURE_METRIC = "walletflow.bank.status_check.failures";
    private static final String AMOUNT_MISMATCH_METRIC = "walletflow.bank.amount_mismatch";

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
    PaymentProvider paymentProvider;
    @Autowired
    ResilientPaymentProvider resilientProvider;
    @Autowired
    MockProvider mock;
    @Autowired
    ProviderTransactionRepository repository;
    @Autowired
    MeterRegistry meterRegistry;
    @Autowired
    JdbcTemplate jdbcTemplate;

    BankDb db;
    UUID tenant;

    @BeforeEach
    void reset() {
        db = new BankDb(jdbcTemplate);
        db.clean();
        mock.reset();
        resilientProvider.circuitBreaker().reset();
        tenant = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Provider chết lúc đối soát
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Provider chết khi hỏi một hàng: hàng đó vẫn PENDING, lùi hạn, đếm lỗi; các hàng khác vẫn được xử lý; hồi lại thì xong")
    void provider_chet_khi_doi_soat_hang_van_pending_va_hang_khac_van_chay() {
        ProviderTransaction a = createPending(tenant, 11_000);
        ProviderTransaction b = createPending(tenant, 22_000);
        ProviderTransaction c = createPending(tenant, 33_000);
        mock.settle(b.getProviderRef(), EProviderState.SUCCEEDED);
        // a tới hạn sớm nhất nên được hỏi ĐẦU TIÊN (ORDER BY next_check_at) — đúng hàng gặp lỗi
        db.makeDue(a.getTransactionId(), 30);
        db.makeDue(b.getTransactionId(), 20);
        db.makeDue(c.getTransactionId(), 10);
        mock.failNextCalls(1);
        double failuresBefore = failureCount();

        int asked = reconciler.reconcile();

        assertThat(asked).isEqualTo(3);
        assertThat(db.status(tenant, a.getTransactionId())).as("provider không trả lời -> không kết luận gì").isEqualTo("PENDING");
        assertThat(db.nextCheckIsInFuture(a.getTransactionId()))
                .as("đã lùi hạn hỏi lại, không bị hỏi dội mỗi lượt quét")
                .isTrue();
        assertThat(failureCount() - failuresBefore).as("đúng một lần hỏi lỗi").isEqualTo(1.0);
        assertThat(db.outboxEventIds(a.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(a.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();

        assertThat(db.status(tenant, b.getTransactionId())).as("hàng sau hàng lỗi vẫn được xử lý").isEqualTo("SUCCEEDED");
        assertThat(db.outboxEventIds(b.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
        assertThat(db.status(tenant, c.getTransactionId())).isEqualTo("PENDING");

        assertThat(reconciler.reconcile())
                .as("a, c đã được lùi hạn; b đã xong: lượt kế tiếp không còn gì tới hạn")
                .isZero();

        // Provider hồi lại, a được thanh toán, tới hạn -> được chốt ở lượt sau
        mock.settle(a.getProviderRef(), EProviderState.SUCCEEDED);
        db.makeDue(a.getTransactionId(), 1);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, a.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.outboxEventIds(a.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
    }

    @Test
    @DisplayName("Provider nói vẫn PENDING: không phát event nào, hàng giữ PENDING và được lùi hạn")
    void provider_noi_van_pending_thi_khong_phat_event() {
        ProviderTransaction row = createPending(tenant, 9_000);
        db.makeDue(row.getTransactionId(), 5);
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING))
                .as("chỉ còn event pending lúc tạo phiên")
                .hasSize(1);
        assertThat(db.nextCheckIsInFuture(row.getTransactionId())).isTrue();
        assertThat(failureCount()).as("provider trả lời PENDING không phải lỗi").isEqualTo(failuresBefore);
    }

    @Test
    @DisplayName("Hàng chưa tới hạn hỏi lại thì không bị hỏi")
    void hang_chua_toi_han_khong_bi_hoi() {
        ProviderTransaction row = createPending(tenant, 9_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);

        assertThat(reconciler.reconcile()).isZero();

        assertThat(db.status(tenant, row.getTransactionId()))
                .as("provider đã báo thành công nhưng chưa tới hạn -> chưa ai hỏi")
                .isEqualTo("PENDING");
    }

    // ------------------------------------------------------------------
    // Tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Hàng của hai tenant được chốt, mỗi hàng trong ĐÚNG tenant của nó (tenant_id của outbox khớp), không rò tenant")
    void hai_tenant_duoc_doi_soat_dung_tenant() {
        UUID tenantX = UUID.randomUUID();
        UUID tenantY = UUID.randomUUID();
        ProviderTransaction x = createPending(tenantX, 10_000);
        ProviderTransaction y = createPending(tenantY, 20_000);
        mock.settle(x.getProviderRef(), EProviderState.SUCCEEDED);
        mock.settle(y.getProviderRef(), EProviderState.FAILED);
        db.makeDue(x.getTransactionId(), 5);
        db.makeDue(y.getTransactionId(), 5);

        assertThat(reconciler.reconcile()).isEqualTo(2);

        assertThat(db.status(tenantX, x.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.status(tenantY, y.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.outbox(x.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED))
                .extracting(row -> row.get("tenant_id")).containsExactly(tenantX);
        assertThat(db.outbox(y.getTransactionId(), WalletTopics.EVT_BANK_FAILED))
                .extracting(row -> row.get("tenant_id")).containsExactly(tenantY);
        assertThat(db.outboxEventIds(x.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(db.outboxEventIds(y.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(TenantContext.current())
                .as("job đặt tenant theo từng hàng rồi trả lại, không để tenant dính vào luồng")
                .isEmpty();
    }

    // ------------------------------------------------------------------
    // ChargeStatusService.refresh — nơi duy nhất chốt kết quả
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Hai luồng cùng chốt một hàng (bản đọc cũ vẫn PENDING): chỉ MỘT event completed, luồng thua im lặng")
    void chot_hai_lan_voi_ban_doc_cu_chi_phat_mot_event() {
        ProviderTransaction stale = createPending(tenant, 7_000);   // bản đọc còn ở PENDING
        mock.settle(stale.getProviderRef(), EProviderState.SUCCEEDED);

        EProviderTxnStatus first = TenantContext.callWith(tenant, () -> chargeStatusService.refresh(stale));
        EProviderTxnStatus second = TenantContext.callWith(tenant, () -> chargeStatusService.refresh(stale));

        assertThat(first).isEqualTo(EProviderTxnStatus.SUCCEEDED);
        assertThat(second).as("luồng thua thấy trạng thái thật trong DB").isEqualTo(EProviderTxnStatus.SUCCEEDED);
        assertThat(db.outboxEventIds(stale.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED))
                .as("UPDATE có điều kiện trả 0 cho luồng thua -> không phát event thứ hai")
                .hasSize(1);
    }

    @Test
    @DisplayName("Hàng đã có kết quả cuối: refresh không hỏi provider (provider đang chết cũng không sao)")
    void refresh_hang_da_ket_thuc_khong_hoi_provider() {
        ProviderTransaction row = createPending(tenant, 7_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        TenantContext.runWith(tenant, () -> chargeStatusService.refresh(row));
        ProviderTransaction done = TenantContext.callWith(tenant, () -> chargeService.charge(
                new BankChargeCommand(row.getTransactionId(), row.getWalletId(), row.getAmountMinor(), row.getCurrency())));
        assertThat(done.getStatus()).isEqualTo(EProviderTxnStatus.SUCCEEDED);

        mock.failNextCalls(1_000);
        EProviderTxnStatus status = TenantContext.callWith(tenant, () -> chargeStatusService.refresh(done));

        assertThat(status).isEqualTo(EProviderTxnStatus.SUCCEEDED);
    }

    @Test
    @DisplayName("Chốt một lần thu trong TenantContext của tenant khác bị từ chối, hàng không đổi")
    void refresh_sai_tenant_bi_tu_choi() {
        ProviderTransaction row = createPending(tenant, 7_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);

        assertThatThrownBy(() -> TenantContext.callWith(UUID.randomUUID(), () -> chargeStatusService.refresh(row)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("không thuộc tenant hiện tại");

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
    }

    // ------------------------------------------------------------------
    // Đúng provider
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Hàng PENDING tới hạn của provider KHÁC không bị job nhặt: không lùi hạn, không đếm lỗi; hàng của provider đang chạy vẫn được chốt")
    void hang_cua_provider_khac_khong_bi_job_nhat() {
        UUID otherTxn = UUID.randomUUID();
        UUID otherRowId = db.insertRow(tenant, otherTxn, UUID.randomUUID(), 5_000, "other", "PENDING",
                "OTHER-REF-1", null);
        // Đối chứng: một hàng của mock cũng tới hạn trong CÙNG lượt — job không phải "không nhặt gì cả"
        ProviderTransaction own = createPending(tenant, 6_000);
        mock.settle(own.getProviderRef(), EProviderState.SUCCEEDED);
        db.makeDue(own.getTransactionId(), 5);
        Map<String, Object> otherBefore = db.rowById(otherRowId);
        int providerCallsBefore = resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls();
        double failuresBefore = failureCount();

        int asked = reconciler.reconcile();

        assertThat(asked).as("chỉ hàng của mock được nhặt").isEqualTo(1);
        assertThat(db.rowById(otherRowId))
                .as("hàng của provider 'other' nguyên vẹn đến từng cột, kể cả next_check_at (không bị lùi hạn)")
                .isEqualTo(otherBefore);
        assertThat(db.status(tenant, otherTxn)).isEqualTo("PENDING");
        assertThat(db.outboxCountAnyTopic(otherTxn)).isZero();
        assertThat(failureCount()).as("không nhặt thì không có lần hỏi lỗi nào").isEqualTo(failuresBefore);
        assertThat(resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls() - providerCallsBefore)
                .as("provider chỉ bị hỏi MỘT lần — cho hàng của chính nó")
                .isEqualTo(1);
        assertThat(db.status(tenant, own.getTransactionId())).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("refresh một hàng của provider khác: IllegalStateException, KHÔNG hỏi provider, hàng không đổi, không phát event")
    void refresh_hang_cua_provider_khac_bi_tu_choi() {
        UUID otherTxn = UUID.randomUUID();
        UUID otherRowId = db.insertRow(tenant, otherTxn, UUID.randomUUID(), 5_000, "other", "PENDING",
                "OTHER-REF-2", null);
        ProviderTransaction row = repository.findByTenantIdAndId(tenant, otherRowId).orElseThrow();
        Map<String, Object> before = db.rowById(otherRowId);
        int providerCallsBefore = resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls();

        assertThatThrownBy(() -> TenantContext.callWith(tenant, () -> chargeStatusService.refresh(row)))
                .as("hỏi provider khác về phiên này có thể bị hiểu 'không tìm thấy' thành thất bại")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("thuộc provider other")
                .hasMessageContaining("mock");

        assertThat(db.rowById(otherRowId)).isEqualTo(before);
        assertThat(db.outboxCountAnyTopic(otherTxn)).isZero();
        assertThat(resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls())
                .as("provider không bị gọi").isEqualTo(providerCallsBefore);
    }

    // ------------------------------------------------------------------
    // Đúng số tiền
    // ------------------------------------------------------------------
    @ParameterizedTest(name = "provider thu {0} so với số yêu cầu 10.000")
    @ValueSource(longs = {9_999, 10_001, 1})
    @DisplayName("⭐ Provider báo SUCCEEDED nhưng thu SỐ KHÁC số yêu cầu: hàng giữ PENDING, KHÔNG có bank.event.completed, "
            + "đếm amount_mismatch; hàng đúng số cùng lượt vẫn được chốt")
    void thu_lech_so_tien_khong_chot_thanh_cong(long capturedMinor) {
        ProviderTransaction mismatched = createPending(tenant, 10_000);
        ProviderTransaction exact = createPending(tenant, 20_000);   // đối chứng: guard không chặn nhầm
        mock.settleWithCapturedAmount(mismatched.getProviderRef(), capturedMinor);
        mock.settle(exact.getProviderRef(), EProviderState.SUCCEEDED);
        db.makeDue(mismatched.getTransactionId(), 20);
        db.makeDue(exact.getTransactionId(), 10);
        double mismatchBefore = amountMismatchCount();
        double failuresBefore = failureCount();

        assertThat(reconciler.reconcile()).isEqualTo(2);

        assertThat(db.status(tenant, mismatched.getTransactionId()))
                .as("lệch số tiền thì không kết luận gì: ledger sẽ cộng đúng số trong event")
                .isEqualTo("PENDING");
        assertThat(db.outboxEventIds(mismatched.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED))
                .as("không event completed nào cho lần thu lệch số").isEmpty();
        assertThat(db.outboxEventIds(mismatched.getTransactionId(), WalletTopics.EVT_BANK_FAILED))
                .as("cũng không báo thất bại: người dùng đã trả tiền thật").isEmpty();
        assertThat(db.row(tenant, mismatched.getTransactionId()).get("amount_minor"))
                .as("số tiền yêu cầu của hàng không bị đổi theo số provider thu").isEqualTo(10_000L);
        assertThat(amountMismatchCount() - mismatchBefore).as("metric walletflow.bank.amount_mismatch +1").isEqualTo(1.0);
        assertThat(failureCount()).as("lệch số không phải lỗi kỹ thuật của lần hỏi").isEqualTo(failuresBefore);
        assertThat(db.nextCheckIsInFuture(mismatched.getTransactionId()))
                .as("đã lùi hạn, không bị hỏi dội mỗi lượt quét").isTrue();

        assertThat(db.status(tenant, exact.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.outboxEventIds(exact.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);

        // Hỏi lại lần nữa vẫn thế: không tự "hết lệch", vẫn kêu, vẫn chờ người đối chiếu
        db.makeDue(mismatched.getTransactionId(), 5);
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(db.status(tenant, mismatched.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(mismatched.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(amountMismatchCount() - mismatchBefore).isEqualTo(2.0);
    }

    @Test
    @DisplayName("⭐ Provider thu ĐÚNG số nhưng khác LOẠI TIỀN so với hàng: giữ PENDING, không phát completed, đếm amount_mismatch")
    void thu_lech_loai_tien_khong_chot_thanh_cong() {
        ProviderTransaction row = createPending(tenant, 10_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);   // mock thu VND, đúng số
        // Hàng ghi nhận yêu cầu bằng USD: số giống hệt, loại tiền khác -> không được coi là thu đúng
        int updated = jdbcTemplate.update("UPDATE provider_transactions SET currency = 'USD' WHERE id = ?", row.getId());
        assertThat(updated).isEqualTo(1);
        db.makeDue(row.getTransactionId(), 5);
        double mismatchBefore = amountMismatchCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(amountMismatchCount() - mismatchBefore).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Provider báo FAILED: vẫn chốt FAILED bình thường (không có tiền nào để lệch) và không đếm amount_mismatch")
    void that_bai_khong_bi_guard_so_tien_chan() {
        ProviderTransaction row = createPending(tenant, 10_000);
        mock.settle(row.getProviderRef(), EProviderState.FAILED);
        db.makeDue(row.getTransactionId(), 5);
        double mismatchBefore = amountMismatchCount();

        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
        assertThat(amountMismatchCount()).isEqualTo(mismatchBefore);
    }

    // ------------------------------------------------------------------
    // Hoàn tiền (PaymentProvider, đường bù trừ của saga)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Hoàn tiền: chưa thu xong -> NOT_REFUNDABLE; thu xong -> Refunded; cùng khoá hai lần -> cùng refundRef; không hoàn quá số đã thu")
    void hoan_tien_idempotent_va_khong_vuot_so_da_thu() {
        ProviderTransaction charged = createPending(tenant, 5_000);
        String ref = charged.getProviderRef();

        assertThat(paymentProvider.refund(refund(UUID.randomUUID().toString(), ref, 5_000)))
                .as("phiên còn PENDING: chưa có tiền để hoàn")
                .isInstanceOfSatisfying(RefundResult.Declined.class,
                        declined -> assertThat(declined.failureCode()).isEqualTo("NOT_REFUNDABLE"));

        mock.settle(ref, EProviderState.SUCCEEDED);
        RefundRequest request = refund(UUID.randomUUID().toString(), ref, 2_000);
        RefundResult.Refunded first = (RefundResult.Refunded) paymentProvider.refund(request);
        RefundResult.Refunded again = (RefundResult.Refunded) paymentProvider.refund(request);

        assertThat(first.refundRef()).isNotBlank();
        assertThat(again.refundRef()).as("cùng khoá: cùng lần hoàn, không hoàn hai lần").isEqualTo(first.refundRef());

        // Còn lại 3.000: khoá khác đòi 4.000 phải bị từ chối. Nếu lần hoàn trùng khoá ở trên bị trừ hai lần thì
        // còn lại chỉ 1.000 và đòi 3.000 bên dưới sẽ hỏng.
        assertThat(paymentProvider.refund(refund(UUID.randomUUID().toString(), ref, 4_000)))
                .isInstanceOfSatisfying(RefundResult.Declined.class,
                        declined -> assertThat(declined.failureCode()).isEqualTo("NOT_REFUNDABLE"));
        assertThat(paymentProvider.refund(refund(UUID.randomUUID().toString(), ref, 3_000)))
                .isInstanceOf(RefundResult.Refunded.class);
        assertThat(paymentProvider.refund(refund(UUID.randomUUID().toString(), ref, 1)))
                .as("đã hoàn đủ 5.000 — không còn gì để hoàn")
                .isInstanceOf(RefundResult.Declined.class);
    }

    @ParameterizedTest(name = "số tiền hoàn {0}")
    @ValueSource(longs = {0, -1, -5_000})
    @DisplayName("Hoàn tiền với số không dương: INVALID_AMOUNT, và không làm thay đổi số còn hoàn được (hoàn âm không được 'nới' trần)")
    void hoan_tien_so_khong_duong_bi_tu_choi_va_khong_dong_vao_tran(long amountMinor) {
        ProviderTransaction charged = createPending(tenant, 5_000);
        String ref = charged.getProviderRef();
        mock.settle(ref, EProviderState.SUCCEEDED);

        assertThat(paymentProvider.refund(refund(UUID.randomUUID().toString(), ref, amountMinor)))
                .isInstanceOfSatisfying(RefundResult.Declined.class,
                        declined -> assertThat(declined.failureCode()).isEqualTo("INVALID_AMOUNT"));

        // Trần vẫn đúng 5.000: nếu hoàn âm làm 'số đã hoàn' âm thì 5.001 sẽ lọt qua
        assertThat(paymentProvider.refund(refund(UUID.randomUUID().toString(), ref, 5_001)))
                .isInstanceOfSatisfying(RefundResult.Declined.class,
                        declined -> assertThat(declined.failureCode()).isEqualTo("NOT_REFUNDABLE"));
        assertThat(paymentProvider.refund(refund(UUID.randomUUID().toString(), ref, 5_000)))
                .isInstanceOf(RefundResult.Refunded.class);
        assertThat(paymentProvider.refund(refund(UUID.randomUUID().toString(), ref, 1)))
                .isInstanceOfSatisfying(RefundResult.Declined.class,
                        declined -> assertThat(declined.failureCode()).isEqualTo("NOT_REFUNDABLE"));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    /** Tạo một lần thu ở trạng thái PENDING (phiên đã tạo ở mock, chưa ai trả tiền). */
    private ProviderTransaction createPending(UUID tenantId, long amountMinor) {
        BankChargeCommand command = BankFixtures.freshCharge(amountMinor);
        ProviderTransaction row = TenantContext.callWith(tenantId, () -> chargeService.charge(command));
        assertThat(row.getProviderRef()).as("tiền điều kiện: lần thu phải PENDING").isNotNull();
        return row;
    }

    private static RefundRequest refund(String key, String providerRef, long amountMinor) {
        return new RefundRequest(key, providerRef, amountMinor, ECurrency.VND);
    }

    private double failureCount() {
        return meterRegistry.get(FAILURE_METRIC).counter().count();
    }

    private double amountMismatchCount() {
        return meterRegistry.get(AMOUNT_MISMATCH_METRIC).counter().count();
    }
}
