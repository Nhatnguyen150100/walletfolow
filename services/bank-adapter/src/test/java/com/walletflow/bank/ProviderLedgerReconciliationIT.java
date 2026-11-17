package com.walletflow.bank;

import com.walletflow.bank.services.ProviderLedgerReconciler;
import com.walletflow.bank.support.FakeLedger;
import com.walletflow.bank.support.FakeLedgerConfig;
import com.walletflow.bank.support.ProviderRows;
import com.walletflow.bank.support.ReconciliationDb;
import com.walletflow.common.money.ECurrency;
import io.micrometer.core.instrument.MeterRegistry;
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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Đối soát bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024): tiền RÒNG qua cửa ngoài theo
 * {@code provider_transactions} phải khớp tiền sổ cái ghi qua {@code EXTERNAL_BANK}, từng lệnh một.
 *
 * <p>Test chứng minh ba nhóm tính chất:
 * <ol>
 *   <li><b>Bắt được lệch</b> theo cả hai chiều quét — đã thu mà chưa ghi sổ, ghi sổ mà lần thu thất bại, lệch
 *       số tiền, và bút toán nạp mà adapter không có hàng nào (chỉ chiều ngược thấy).</li>
 *   <li><b>KHÔNG kêu oan</b> — hoàn tiền (kể cả hoàn xong rồi ledger từ chối/đảo), lệnh chưa yên, thao tác
 *       đang chạy, và ca saga vừa ghi sổ giữa hai lần đọc (ADR-023 §4).</li>
 *   <li><b>Không tự sửa, không ghi trùng</b> — mỗi chỗ lệch đúng MỘT hàng OPEN, lượt sau chỉ tăng
 *       {@code times_seen}; hai chiều quét không cùng ghi một lệnh; tenant không lẫn.</li>
 * </ol>
 *
 * <p>Lỗi ledger / chạm trần nằm ở {@code ProviderLedgerReconciliationFailuresIT} (cần cấu hình khác và một
 * gauge {@code last_success} chưa từng được đặt).
 *
 * <p>{@code interval} rất dài để mọi lần gọi trong một test rơi vào cùng cửa sổ giành lượt;
 * {@code settle-window} một giờ là hàng rào THẬT của production — hàng "đã yên" là hàng có
 * {@code updated_at} cách đây 2 giờ ({@link ProviderRows}).
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "outbox.relay.delay-ms=3600000",
        "walletflow.bank.provider=mock",
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        // Không luồng nền nào được đụng vào hàng test dựng sẵn
        "walletflow.bank.status-check.enabled=false",
        "walletflow.bank.webhook.enabled=false",
        // @Scheduled không được chen vào: test gọi reconcile() trực tiếp
        "walletflow.bank.reconciliation.initial-delay=PT24H",
        "walletflow.bank.reconciliation.interval=PT8760H",
        "walletflow.bank.reconciliation.settle-window=PT1H",
        // Lô nhỏ để vài hàng là đã chạy qua nhiều trang, ở cả hai chiều
        "walletflow.bank.reconciliation.batch-size=2"
})
@Import(FakeLedgerConfig.class)
@Testcontainers
class ProviderLedgerReconciliationIT {

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("railsdb");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    private static final String NOT_POSTED = "PROVIDER_MONEY_NOT_POSTED";
    private static final String POSTED_WITHOUT_MONEY = "POSTED_WITHOUT_PROVIDER_MONEY";

    @Autowired
    ProviderLedgerReconciler reconciler;
    @Autowired
    FakeLedger ledger;
    @Autowired
    MeterRegistry meterRegistry;
    @Autowired
    JdbcTemplate jdbcTemplate;

    ProviderRows rows;
    ReconciliationDb db;
    UUID tenant;
    UUID txn;

    @BeforeEach
    void reset() {
        rows = new ProviderRows(jdbcTemplate);
        db = new ReconciliationDb(jdbcTemplate);
        db.clean();
        ledger.reset();
        tenant = UUID.randomUUID();
        txn = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // 1 — Chiều xuôi: bắt lệch
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Provider đã thu 100k mà sổ cái không có gì: đúng MỘT break OPEN PROVIDER_MONEY_NOT_POSTED, không tự sửa")
    void da_thu_ma_chua_ghi_so_thi_bao_thieu_tien_vao_so() {
        rows.charge(tenant, txn, "SUCCEEDED", 100_000);

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        Map<String, Object> theBreak = db.onlyBreak();
        assertThat(theBreak.get("type")).isEqualTo(NOT_POSTED);
        assertThat(theBreak.get("subject")).isEqualTo(txn + ":VND");
        assertThat(theBreak.get("tenant_id")).isEqualTo(tenant);
        assertThat(theBreak.get("currency")).isEqualTo("VND");
        assertThat(theBreak.get("expected_minor")).as("provider giữ ròng 100k").isEqualTo(100_000L);
        assertThat(theBreak.get("actual_minor")).as("sổ cái chưa ghi đồng nào").isEqualTo(0L);
        assertThat(theBreak.get("delta_minor")).isEqualTo(-100_000L);
        assertThat(theBreak.get("status")).isEqualTo("OPEN");
        assertThat(theBreak.get("auto_repaired")).as("không bao giờ tự sửa sổ").isEqualTo(false);
        assertThat(theBreak.get("times_seen")).isEqualTo(1);
        Map<String, Object> run = db.onlyRun();
        assertThat(run.get("kind")).isEqualTo(ProviderLedgerReconciler.KIND);
        assertThat(run.get("status")).isEqualTo("SUCCEEDED");
        assertThat(run.get("break_count")).isEqualTo(1);
        assertThat(run.get("checked_count")).as("đã thật sự soi lệnh").isEqualTo(1L);
        assertThat(run.get("failure_message")).isNull();
        assertThat(ledger.movementCalls()).as("lần đọc đầu + lần đọc lại, cả hai đúng tenant")
                .hasSize(2)
                .allSatisfy(call -> {
                    assertThat(call.tenantId()).isEqualTo(tenant);
                    assertThat(call.transactionIds()).containsExactly(txn);
                });
    }

    @Test
    @DisplayName("Provider thu 100k và sổ cái ghi +100k: khớp, không break, lượt SUCCEEDED")
    void thu_va_ghi_so_khop_thi_khong_bao_lech() {
        rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        ledger.declareTopup(tenant, txn, 100_000, settledAt());

        int breaks = reconciler.reconcile();

        assertThat(breaks).isZero();
        assertThat(db.count("reconciliation_breaks")).isZero();
        Map<String, Object> run = db.onlyRun();
        assertThat(run.get("status")).isEqualTo("SUCCEEDED");
        assertThat(run.get("break_count")).isEqualTo(0);
        assertThat(run.get("checked_count"))
                .as("mỗi lệnh đếm MỘT lần: chiều xuôi soi nó, chiều ngược bỏ qua vì adapter đã biết")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("Không có dữ liệu nào: lượt vẫn SUCCEEDED với 0 lệnh đã soi (không phải lỗi)")
    void khong_co_du_lieu_thi_luot_van_thanh_cong() {
        int breaks = reconciler.reconcile();

        assertThat(breaks).isZero();
        Map<String, Object> run = db.onlyRun();
        assertThat(run.get("status")).isEqualTo("SUCCEEDED");
        assertThat(run.get("checked_count")).isEqualTo(0L);
    }

    @Test
    @DisplayName("⭐ Lần thu FAILED mà sổ cái vẫn có +100k: break POSTED_WITHOUT_PROVIDER_MONEY (tiền vào ví mà không ai trả)")
    void thu_that_bai_ma_so_cai_van_ghi_thi_bao_tien_ao() {
        rows.charge(tenant, txn, "FAILED", 100_000);
        ledger.declareTopup(tenant, txn, 100_000, settledAt());

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        Map<String, Object> theBreak = db.onlyBreak();
        assertThat(theBreak.get("type")).isEqualTo(POSTED_WITHOUT_MONEY);
        assertThat(theBreak.get("subject")).isEqualTo(txn + ":VND");
        assertThat(theBreak.get("expected_minor")).as("lần thu FAILED góp 0").isEqualTo(0L);
        assertThat(theBreak.get("actual_minor")).isEqualTo(100_000L);
        assertThat(theBreak.get("delta_minor")).isEqualTo(100_000L);
        assertThat(theBreak.get("auto_repaired")).isEqualTo(false);
    }

    @Test
    @DisplayName("⭐ Provider thu 100k, sổ cái chỉ ghi 90k: break PROVIDER_MONEY_NOT_POSTED với delta -10k")
    void lech_so_tien_thi_bao_dung_chenh_lech() {
        rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        ledger.declareTopup(tenant, txn, 90_000, settledAt());

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        Map<String, Object> theBreak = db.onlyBreak();
        assertThat(theBreak.get("type")).isEqualTo(NOT_POSTED);
        assertThat(theBreak.get("expected_minor")).isEqualTo(100_000L);
        assertThat(theBreak.get("actual_minor")).isEqualTo(90_000L);
        assertThat(theBreak.get("delta_minor")).isEqualTo(-10_000L);
    }

    @Test
    @DisplayName("Sổ cái ghi thêm một loại tiền khác (USD) mà provider không thu: so RIÊNG từng loại, break cho USD")
    void so_cai_ghi_them_loai_tien_khac_thi_bao_rieng_loai_do() {
        rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        ledger.declareNet(tenant, txn, ECurrency.VND, 100_000);
        ledger.declareNet(tenant, txn, ECurrency.USD, 5_000);

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        Map<String, Object> theBreak = db.onlyBreak();
        assertThat(theBreak.get("subject")).isEqualTo(txn + ":USD");
        assertThat(theBreak.get("currency")).isEqualTo("USD");
        assertThat(theBreak.get("type")).isEqualTo(POSTED_WITHOUT_MONEY);
        assertThat(theBreak.get("expected_minor")).isEqualTo(0L);
        assertThat(theBreak.get("actual_minor")).isEqualTo(5_000L);
    }

    // ------------------------------------------------------------------
    // 2 — Không kêu oan: hoàn tiền
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đã thu rồi HOÀN (ledger từ chối nạp nên không có bút toán): ròng 0 = 0, KHÔNG break")
    void thu_roi_hoan_ma_so_cai_khong_co_gi_thi_khong_bao_lech() {
        UUID charge = rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        rows.refund(tenant, txn, charge, "SUCCEEDED", 100_000);

        int breaks = reconciler.reconcile();

        assertThat(breaks).as("nếu hoàn không bị trừ, mọi lần nạp bị từ chối rồi hoàn đều thành break giả")
                .isZero();
        assertThat(db.count("reconciliation_breaks")).isZero();
        assertThat(db.onlyRun().get("checked_count")).as("lệnh đã thật sự được soi").isEqualTo(1L);
    }

    @Test
    @DisplayName("⭐ Đã thu rồi hoàn, sổ cái có nạp rồi ĐẢO (ròng 0, vẫn hiện): khớp, KHÔNG break")
    void thu_roi_hoan_ma_so_cai_nap_roi_dao_thi_khong_bao_lech() {
        UUID charge = rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        rows.refund(tenant, txn, charge, "SUCCEEDED", 100_000);
        ledger.declareTopup(tenant, txn, 0, settledAt());

        int breaks = reconciler.reconcile();

        assertThat(breaks).isZero();
        assertThat(db.count("reconciliation_breaks")).isZero();
        assertThat(db.onlyRun().get("checked_count"))
                .as("mỗi lệnh đếm MỘT lần, dù có ở cả hai chiều quét").isEqualTo(1L);
    }

    @Test
    @DisplayName("Lần hoàn FAILED không trừ tiền: thu 100k + hoàn FAILED, sổ cái +100k thì khớp")
    void hoan_that_bai_khong_duoc_tru_tien() {
        UUID charge = rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        rows.refund(tenant, txn, charge, "FAILED", 100_000);
        ledger.declareNet(tenant, txn, 100_000);

        int breaks = reconciler.reconcile();

        assertThat(breaks).as("tiền chưa hoàn thì provider vẫn giữ 100k, đúng bằng sổ cái").isZero();
    }

    // ------------------------------------------------------------------
    // 3 — Không kêu oan: chưa yên / đang chạy / saga vừa ghi sổ
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Lần thu vừa chốt (updated_at = bây giờ) mà sổ cái chưa có gì: CHƯA soi, không break")
    void lan_thu_chua_yen_thi_chua_so() {
        rows.charge(UUID.randomUUID(), tenant, txn, "SUCCEEDED", 100_000, 0);

        int breaks = reconciler.reconcile();

        assertThat(breaks).as("saga ghi sổ bất đồng bộ SAU khi lần thu chốt: khoảng 'đã thu, chưa ghi' là bình thường")
                .isZero();
        assertThat(db.count("reconciliation_breaks")).isZero();
        Map<String, Object> run = db.onlyRun();
        assertThat(run.get("status")).isEqualTo("SUCCEEDED");
        assertThat(run.get("checked_count")).as("không tính lệnh chưa yên là đã soi").isEqualTo(0L);
        assertThat(ledger.movementCalls()).as("không đụng tới ledger vì chưa có gì đủ yên để hỏi").isEmpty();
    }

    @Test
    @DisplayName("⭐ Thu SUCCEEDED (đã yên) nhưng lần hoàn còn NEW (đang chạy): chưa chốt nên KHÔNG so, không break")
    void dang_hoan_do_thi_khong_so() {
        UUID charge = rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        rows.refund(tenant, txn, charge, "NEW", 100_000);

        int breaks = reconciler.reconcile();

        assertThat(breaks).as("hoàn chưa chốt thì ròng của provider chưa biết — so bây giờ sẽ kêu oan").isZero();
        assertThat(db.count("reconciliation_breaks")).isZero();
        assertThat(ledger.movementCalls()).isEmpty();
    }

    @Test
    @DisplayName("Lần thu còn PENDING (chưa chốt): không so, không break")
    void dang_cho_provider_thi_khong_so() {
        rows.charge(tenant, txn, "PENDING", 100_000);

        int breaks = reconciler.reconcile();

        assertThat(breaks).isZero();
        assertThat(ledger.movementCalls()).isEmpty();
    }

    @Test
    @DisplayName("⭐ Double read: lần đọc đầu sổ cái chưa có gì, nhưng saga vừa ghi +100k trước lần đọc lại: KHÔNG break")
    void saga_ghi_so_giua_hai_lan_doc_thi_khong_bao_lech() {
        rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        // Lời gọi 1 = đọc theo lô (thấy "chưa có gì" -> nghi ngờ); ngay sau đó saga ghi sổ; lời gọi 2 = đọc lại.
        ledger.afterMovementCall(1, () -> ledger.declareNet(tenant, txn, 100_000));

        int breaks = reconciler.reconcile();

        assertThat(ledger.movementCalls()).as("phải có lần ĐỌC LẠI trước khi kết luận").hasSize(2);
        assertThat(breaks).isZero();
        assertThat(db.count("reconciliation_breaks"))
                .as("kết luận phải mô tả cái thấy lúc kết luận, không phải lần đọc đầu đã cũ")
                .isZero();
        assertThat(db.onlyRun().get("status")).isEqualTo("SUCCEEDED");
    }

    // ------------------------------------------------------------------
    // 4 — Chiều ngược
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Sổ cái có bút toán nạp 50k mà bank-adapter KHÔNG có hàng nào: break POSTED_WITHOUT_PROVIDER_MONEY (chỉ chiều ngược thấy)")
    void but_toan_nap_khong_co_cong_dung_sau_thi_bao_tien_ao() {
        ledger.declareTopup(tenant, txn, 50_000, settledAt());

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        Map<String, Object> theBreak = db.onlyBreak();
        assertThat(theBreak.get("type")).isEqualTo(POSTED_WITHOUT_MONEY);
        assertThat(theBreak.get("subject")).isEqualTo(txn + ":VND");
        assertThat(theBreak.get("tenant_id")).isEqualTo(tenant);
        assertThat(theBreak.get("expected_minor")).isEqualTo(0L);
        assertThat(theBreak.get("actual_minor")).isEqualTo(50_000L);
        assertThat(theBreak.get("delta_minor")).isEqualTo(50_000L);
        assertThat((String) theBreak.get("detail")).contains("bank-adapter KHÔNG có hàng nào");
        assertThat(theBreak.get("auto_repaired")).isEqualTo(false);
        assertThat(db.onlyRun().get("break_count")).isEqualTo(1);
    }

    @Test
    @DisplayName("Bút toán nạp ròng 0 (nạp rồi đảo) mà bank-adapter không có hàng: không có tiền nào lệch, không break")
    void but_toan_nap_ron_bang_0_khong_co_hang_thi_khong_bao_lech() {
        ledger.declareTopup(tenant, txn, 0, settledAt());

        int breaks = reconciler.reconcile();

        assertThat(breaks).isZero();
        assertThat(db.count("reconciliation_breaks")).isZero();
    }

    @Test
    @DisplayName("Bút toán nạp quá mới (sau mốc settle-window): chiều ngược chưa soi, không break")
    void but_toan_nap_qua_moi_thi_chua_soi() {
        ledger.declareTopup(tenant, txn, 50_000, Instant.now().minus(1, ChronoUnit.MINUTES));

        int breaks = reconciler.reconcile();

        assertThat(breaks).isZero();
        assertThat(db.count("reconciliation_breaks")).isZero();
        assertThat(ledger.topupCalls()).as("tham số before = mốc đã yên, không phải bây giờ")
                .allSatisfy(call -> assertThat(call.before()).isBefore(Instant.now().minus(59, ChronoUnit.MINUTES)));
    }

    @Test
    @DisplayName("⭐ Hai chiều không ghi trùng: lệnh adapter biết và khớp thì 0 break; lệnh adapter biết và lệch thì đúng MỘT break (times_seen = 1)")
    void hai_chieu_khong_ghi_trung_mot_lenh() {
        UUID matching = UUID.randomUUID();
        UUID mismatching = UUID.randomUUID();
        rows.charge(tenant, matching, "SUCCEEDED", 70_000);
        rows.charge(tenant, mismatching, "SUCCEEDED", 80_000);
        ledger.declareTopup(tenant, matching, 70_000, settledAt());
        ledger.declareTopup(tenant, mismatching, 30_000, settledAt());

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        Map<String, Object> theBreak = db.onlyBreak();
        assertThat(theBreak.get("subject")).isEqualTo(mismatching + ":VND");
        assertThat(theBreak.get("type")).isEqualTo(NOT_POSTED);
        assertThat(theBreak.get("delta_minor")).isEqualTo(-50_000L);
        assertThat(theBreak.get("times_seen"))
                .as("chiều ngược cũng thấy lệnh này trong danh sách TOPUP; nếu nó không loại lệnh adapter đã biết "
                        + "thì break bị ghi lần hai và times_seen = 2 ngay trong một lượt")
                .isEqualTo(1);
        assertThat(ledger.topupCalls()).as("chiều ngược thật sự đã chạy và thấy các bút toán đó").isNotEmpty();
    }

    @Test
    @DisplayName("Nhiều lệnh qua nhiều lô (batch-size = 2) ở cả hai chiều: không bỏ sót, mỗi lệnh lệch đúng một break")
    void nhieu_lo_o_ca_hai_chieu_khong_bo_sot() {
        // 3 lệnh adapter biết và lệch (2 lô chiều xuôi), 3 lệnh adapter không biết (2 trang chiều ngược)
        List<UUID> known = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        List<UUID> unknown = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        Instant base = settledAt();
        for (int i = 0; i < known.size(); i++) {
            rows.charge(tenant, known.get(i), "SUCCEEDED", 10_000);
            ledger.declareTopup(tenant, known.get(i), 9_000, base.plusSeconds(i));
        }
        for (int i = 0; i < unknown.size(); i++) {
            ledger.declareTopup(tenant, unknown.get(i), 7_000, base.plusSeconds(10 + i));
        }

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(6);
        Set<String> subjects = db.breaks().stream().map(b -> (String) b.get("subject")).collect(Collectors.toSet());
        Set<String> expected = new java.util.HashSet<>();
        known.forEach(id -> expected.add(id + ":VND"));
        unknown.forEach(id -> expected.add(id + ":VND"));
        assertThat(subjects).isEqualTo(expected);
        assertThat(db.breaks()).allSatisfy(b -> assertThat(b.get("times_seen")).isEqualTo(1));
        assertThat(ledger.topupCalls().size()).as("3 bút toán / trang 2 = ít nhất 2 trang").isGreaterThanOrEqualTo(2);
    }

    // ------------------------------------------------------------------
    // 5 — Cô lập tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Cô lập tenant: cùng transactionId, A thu 100k chưa ghi sổ, B có bút toán nạp mà không có hàng: mỗi tenant một break ĐÚNG loại")
    void cung_transaction_id_o_hai_tenant_khong_lan_nhau() {
        UUID tenantA = UUID.randomUUID();
        UUID tenantB = UUID.randomUUID();
        rows.charge(tenantA, txn, "SUCCEEDED", 100_000);
        ledger.declareTopup(tenantB, txn, 100_000, settledAt());

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(2);
        List<Map<String, Object>> ofA = db.breaksOf(tenantA);
        assertThat(ofA).singleElement().satisfies(b -> {
            assertThat(b.get("type")).as("A: ledger của A không có gì dù ledger của B có").isEqualTo(NOT_POSTED);
            assertThat(b.get("subject")).isEqualTo(txn + ":VND");
            assertThat(b.get("actual_minor")).isEqualTo(0L);
        });
        List<Map<String, Object>> ofB = db.breaksOf(tenantB);
        assertThat(ofB).singleElement().satisfies(b -> {
            assertThat(b.get("type")).as("B: adapter không có hàng NÀO của B dù có hàng của A")
                    .isEqualTo(POSTED_WITHOUT_MONEY);
            assertThat(b.get("actual_minor")).isEqualTo(100_000L);
        });
        assertThat(ledger.movementCalls()).extracting(FakeLedger.MovementCall::tenantId)
                .as("mọi lời gọi ledger mang đúng tenant, không lời gọi nào không-tenant")
                .containsOnly(tenantA, tenantB);
        assertThat(ledger.movementCalls()).allSatisfy(call ->
                assertThat(call.transactionIds()).containsExactly(txn));
    }

    // ------------------------------------------------------------------
    // 6 — Giành lượt, lặp lại, gauge
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ reconcile() lần hai trong CÙNG cửa sổ trả -1 và không ghi gì; lượt SAU thấy lại chỗ lệch thì tăng times_seen, giữ first_seen_at, không thêm hàng OPEN")
    void lap_lai_thi_tang_times_seen_va_khong_nhan_doi() {
        rows.charge(tenant, txn, "SUCCEEDED", 100_000);

        assertThat(reconciler.reconcile()).isEqualTo(1);
        Map<String, Object> first = db.onlyBreak();
        int callsAfterFirst = ledger.movementCalls().size();

        assertThat(reconciler.reconcile()).as("instance/lần gọi khác trong cùng cửa sổ: bỏ qua").isEqualTo(-1);
        assertThat(db.runs()).as("uk_recon_run_window: đúng một lượt").hasSize(1);
        assertThat(db.onlyBreak().get("times_seen")).as("lần gọi bị từ chối không ghi gì").isEqualTo(1);
        assertThat(ledger.movementCalls()).as("và không hỏi ledger").hasSize(callsAfterFirst);

        // Lùi cửa sổ của lượt cũ như đã là lượt hôm qua, rồi chạy lượt mới
        jdbcTemplate.update("UPDATE reconciliation_runs SET window_start = window_start - interval '2 years'");
        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.runs()).hasSize(2);
        Map<String, Object> again = db.onlyBreak();
        assertThat(again.get("id")).as("cùng MỘT hàng, không thêm hàng OPEN thứ hai").isEqualTo(first.get("id"));
        assertThat(again.get("times_seen")).isEqualTo(2);
        assertThat(again.get("first_seen_at")).as("giữ lần đầu thấy").isEqualTo(first.get("first_seen_at"));
        assertThat(again.get("status")).isEqualTo("OPEN");
        assertThat(again.get("run_id")).as("trỏ về lượt mới nhất").isNotEqualTo(first.get("run_id"));
        assertThat(db.count("reconciliation_breaks")).isEqualTo(1);
        assertThat(gauge("walletflow.reconciliation.breaks", "tier", "bank"))
                .as("gauge đếm hàng OPEN, không đếm số lần thấy").isEqualTo(1.0);
    }

    @Test
    @DisplayName("⭐ Gauge sau một lượt có đủ hai loại lệch: open{type} = 1 mỗi loại, breaks{tier=bank} = 2, last_success = đầu cửa sổ")
    void gauge_phan_anh_so_break_dang_mo_theo_loai_va_moc_thanh_cong() {
        UUID notPosted = UUID.randomUUID();
        UUID phantom = UUID.randomUUID();
        rows.charge(tenant, notPosted, "SUCCEEDED", 100_000);
        rows.charge(tenant, phantom, "FAILED", 40_000);
        ledger.declareTopup(tenant, phantom, 40_000, settledAt());

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(2);
        assertThat(db.breaks()).extracting(b -> (String) b.get("type"))
                .containsExactlyInAnyOrder(NOT_POSTED, POSTED_WITHOUT_MONEY);
        assertThat(gauge("walletflow.reconciliation.provider_ledger.open", "type", NOT_POSTED)).isEqualTo(1.0);
        assertThat(gauge("walletflow.reconciliation.provider_ledger.open", "type", POSTED_WITHOUT_MONEY))
                .isEqualTo(1.0);
        assertThat(gauge("walletflow.reconciliation.breaks", "tier", "bank")).isEqualTo(2.0);
        assertThat(db.onlyRun().get("status")).as("lệch tiền không làm hỏng lượt: lượt chạy trọn").isEqualTo("SUCCEEDED");
        assertThat(gauge("walletflow.reconciliation.last_success_timestamp", "tier", "bank"))
                .as("mốc lượt thành công gần nhất = đầu cửa sổ của lượt này")
                .isEqualTo((double) db.onlyRunWindowStart().getEpochSecond());
    }

    @Test
    @DisplayName("Đóng break rồi chạy lượt sạch: gauge open về 0 (gauge đọc từ DB, không cộng dồn trong bộ nhớ)")
    void dong_break_thi_gauge_ve_0_o_luot_sau() {
        rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        reconciler.reconcile();
        assertThat(gauge("walletflow.reconciliation.provider_ledger.open", "type", NOT_POSTED)).isEqualTo(1.0);
        // Người trực đóng break (đã hoàn tay) và ledger sau đó khớp
        jdbcTemplate.update("UPDATE reconciliation_breaks SET status = 'CLOSED', closed_at = now(), "
                + "closed_by = gen_random_uuid(), closed_note = 'đã xử lý tay'");
        jdbcTemplate.update("UPDATE reconciliation_runs SET window_start = window_start - interval '2 years'");
        ledger.declareNet(tenant, txn, 100_000);

        int breaks = reconciler.reconcile();

        assertThat(breaks).isZero();
        assertThat(gauge("walletflow.reconciliation.provider_ledger.open", "type", NOT_POSTED)).isEqualTo(0.0);
        assertThat(gauge("walletflow.reconciliation.breaks", "tier", "bank")).isEqualTo(0.0);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Bút toán ghi cách đây 2 giờ — đã qua settle-window nên chiều ngược chịu soi. */
    private static Instant settledAt() {
        return Instant.now().minus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MICROS);
    }

    private double gauge(String name, String tagKey, String tagValue) {
        return meterRegistry.get(name).tag(tagKey, tagValue).gauge().value();
    }
}
