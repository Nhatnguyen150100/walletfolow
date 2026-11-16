package com.walletflow.wallet;

import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.services.WalletProjector;
import com.walletflow.wallet.services.WalletReconciler;
import com.walletflow.wallet.services.WalletService;
import com.walletflow.wallet.support.Events;
import com.walletflow.wallet.support.FakeLedger;
import com.walletflow.wallet.support.FakeLedgerConfig;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Đối soát TẦNG 2 (roadmap 3.1, ADR-023): read model của ví so với sổ cái.
 *
 * <p>Trước job này, một {@code UPDATE wallets SET balance_minor = ...} gõ tay hay một lỗi của
 * projector làm người dùng thấy số dư sai mà không có gì phát hiện (GAP-A3). Test chứng minh:
 * <ol>
 *   <li>Bắt được lệch, tự dựng lại số dư đúng từ sổ cái, nhưng break VẪN MỞ (§5): rebuild chép lại
 *       con số đúng, không sửa cái bug đã làm lệch.</li>
 *   <li>KHÔNG kêu oan: ví vừa đổi thì chưa soi, lệch biến mất ở lần đọc thứ hai thì không ghi, ví
 *       PENDING chưa có tài khoản ledger là bình thường (§4).</li>
 *   <li>Ví ACTIVE giữ tiền mà ledger không có tài khoản thì gọi người, KHÔNG quét số dư về 0 — "sửa"
 *       bằng cách xoá tiền của khách là tệ hơn cả cái lệch.</li>
 * </ol>
 *
 * <p>{@code settle-window} đặt một giờ cho cả lớp (đúng hàng rào của production, không nới lỏng): ca
 * nào cần ví "đã yên" thì lùi {@code updated_at} của nó bằng {@link #settle()}; ca kiểm chính hàng
 * rào thì để nguyên {@code updated_at = now()}. {@code interval} rất dài để hai lần gọi trong cùng
 * một test chắc chắn rơi vào cùng cửa sổ giành lượt (ca "chạy trùng").
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "walletflow.wallet.pending-resend.enabled=false",
        "management.tracing.enabled=false",
        // @Scheduled không được chen vào: test gọi reconcile() trực tiếp
        "walletflow.wallet.reconciliation.initial-delay=PT24H",
        "walletflow.wallet.reconciliation.interval=PT8760H",
        "walletflow.wallet.reconciliation.settle-window=PT1H",
        // Lô nhỏ để một test có vài ví là đã chạy qua nhiều trang
        "walletflow.wallet.reconciliation.batch-size=2"
})
@Import(FakeLedgerConfig.class)
@Testcontainers
class WalletReconciliationIT {

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("walletdb");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    WalletReconciler reconciler;
    @Autowired
    WalletService walletService;
    @Autowired
    WalletProjector projector;
    @Autowired
    FakeLedger ledger;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE reconciliation_breaks, reconciliation_runs");
        jdbcTemplate.execute("TRUNCATE wallet_entries, wallets, outbox_event");
        ledger.reset();
    }

    // ------------------------------------------------------------------
    // 1 — Khớp thì không kêu oan
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Read model khớp sổ cái: reconcile() trả 0, run SUCCEEDED, đã soi ví, không break")
    void read_model_khop_so_cai_thi_khong_bao_lech() {
        UUID tenant = UUID.randomUUID();
        UUID wallet = fundedWallet(tenant, 100_000);
        settle();

        int breaks = reconciler.reconcile();

        assertThat(breaks).isZero();
        Map<String, Object> run = onlyRun();
        assertThat(run.get("status")).isEqualTo("SUCCEEDED");
        assertThat(run.get("kind")).isEqualTo(WalletReconciler.KIND);
        assertThat(run.get("checked_count")).as("đã thật sự soi ví — '0 break' vì không soi gì là rỗng")
                .isEqualTo(1L);
        assertThat(run.get("break_count")).isEqualTo(0);
        assertThat(count("reconciliation_breaks")).isZero();
        assertThat(balanceOf(wallet)).isEqualTo(100_000);
    }

    // ------------------------------------------------------------------
    // 2 — ⭐ Số dư bị sửa tay: bắt được, dựng lại, break vẫn mở
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Số dư bị sửa tay: ghi break PROJECTION_SELF_MISMATCH, tự dựng lại số đúng từ sổ cái, break VẪN MỞ")
    void so_du_bi_sua_tay_thi_bat_duoc_va_dung_lai_tu_so_cai() {
        UUID tenant = UUID.randomUUID();
        UUID wallet = fundedWallet(tenant, 100_000);
        jdbcTemplate.update("UPDATE wallets SET balance_minor = 999999 WHERE id = ?", wallet);
        settle();

        int breaks = reconciler.reconcile();

        // Số dư sai lệch CẢ với lịch sử của chính ví (2a) LẪN với ledger (2b). Break 2a ghi trước và
        // gọi rebuild; đến lượt ứng viên 2b thì lần đọc thứ hai đã thấy khớp (rebuild vừa sửa) nên
        // không ghi break thứ hai. Một chỗ lệch ra đúng MỘT break, không đếm đôi cùng một nguyên nhân.
        assertThat(breaks).isEqualTo(1);
        List<Map<String, Object>> rows = breaks();
        assertThat(rows).hasSize(1);
        Map<String, Object> row = rows.getFirst();
        assertThat(row.get("type")).isEqualTo("PROJECTION_SELF_MISMATCH");
        assertThat(row.get("tenant_id")).isEqualTo(tenant);
        assertThat(row.get("subject")).as("subject = id ví").isEqualTo(wallet.toString());
        assertThat(row.get("currency")).isEqualTo("VND");
        assertThat(row.get("expected_minor")).isEqualTo(100_000L);
        assertThat(row.get("actual_minor")).as("con số sai người dùng đã thấy").isEqualTo(999_999L);
        assertThat(row.get("delta_minor")).isEqualTo(899_999L);
        assertThat(row.get("auto_repaired")).as("máy đã dựng lại read model").isEqualTo(true);
        assertThat(row.get("status"))
                .as("KHÔNG tự đóng (ADR-023 §5): rebuild chép lại số đúng, không sửa bug đã làm lệch")
                .isEqualTo("OPEN");
        assertThat(row.get("closed_at")).isNull();

        assertThat(balanceOf(wallet)).as("số dư đã đúng bằng sổ cái").isEqualTo(100_000);
        assertThat(entriesSum(wallet)).isEqualTo(100_000);
        assertThat(onlyRun().get("break_count")).isEqualTo(1);
    }

    @Test
    @DisplayName("Ví tự khớp lịch sử nhưng lệch sổ cái (projector bỏ sót): chỉ phép so với ledger bắt được, LEDGER_MISMATCH + dựng lại")
    void vi_nhat_quan_noi_bo_nhung_lech_so_cai_thi_van_bi_bat() {
        UUID tenant = UUID.randomUUID();
        UUID wallet = fundedWallet(tenant, 100_000);
        addPhantomEntry(tenant, wallet, 50_000);   // cả dòng lịch sử LẪN số dư cùng lệch 50 000
        settle();
        assertThat(balanceOf(wallet)).isEqualTo(entriesSum(wallet));   // 2a vẫn khớp hoàn hảo

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        Map<String, Object> row = breaks().getFirst();
        assertThat(row.get("type"))
                .as("2a không bắt được ca này — đó là lý do phải có 2b so với ledger")
                .isEqualTo("LEDGER_MISMATCH");
        assertThat(row.get("expected_minor")).isEqualTo(100_000L);
        assertThat(row.get("actual_minor")).isEqualTo(150_000L);
        assertThat(row.get("auto_repaired")).isEqualTo(true);
        assertThat(row.get("status")).isEqualTo("OPEN");
        assertThat(balanceOf(wallet)).isEqualTo(100_000);
        assertThat(entriesSum(wallet)).as("dòng lịch sử ma bị gỡ").isEqualTo(100_000);
    }

    // ------------------------------------------------------------------
    // 3 — Hàng rào ví vừa đổi
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Ví vừa đổi số dư (trong settle-window): chưa bị đối soát, không break, số dư không bị đụng tới")
    void vi_vua_doi_so_du_thi_chua_bi_doi_soat() {
        UUID tenant = UUID.randomUUID();
        UUID wallet = fundedWallet(tenant, 100_000);
        jdbcTemplate.update("UPDATE wallets SET balance_minor = 999999, updated_at = now() WHERE id = ?", wallet);

        int breaks = reconciler.reconcile();

        assertThat(breaks).as("event đang bay trông y hệt một ví lệch — kêu oan thì alert bị tắt").isZero();
        assertThat(onlyRun().get("checked_count")).isEqualTo(0L);
        assertThat(count("reconciliation_breaks")).isZero();
        assertThat(balanceOf(wallet)).as("chưa soi thì cũng chưa dựng lại").isEqualTo(999_999);

        // Đối chứng: cùng ví đó, một khi đã yên thì lượt sau bắt được — hàng rào là LÝ DO DUY NHẤT
        // lượt trước không thấy. Dời hàng run cũ ra xa để lượt này rơi vào cửa sổ khác.
        settle();
        jdbcTemplate.update("UPDATE reconciliation_runs SET window_start = window_start - interval '2 years'");
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(breaks()).hasSize(1);
        assertThat(balanceOf(wallet)).isEqualTo(100_000);
    }

    // ------------------------------------------------------------------
    // 4 — Lệch biến mất ở lần đọc thứ hai
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Lệch biến mất ở lần đọc thứ hai (event đang bay vừa áp xong): không ghi break")
    void lech_bien_mat_o_lan_doc_thu_hai_thi_khong_ghi_break() {
        UUID tenant = UUID.randomUUID();
        UUID wallet = fundedWallet(tenant, 100_000);
        addPhantomEntry(tenant, wallet, 50_000);   // lần đọc đầu: read model 150 000 vs ledger 100 000
        settle();

        // duringRead chạy NGAY SAU mỗi lần job hỏi ledger. Lần đầu (đọc cả lô): giả lập projector vừa
        // áp xong nốt phần đang bay — read model về đúng 100 000 trước khi job kịp đọc lại ví này.
        AtomicInteger ledgerReads = new AtomicInteger();
        ledger.duringRead(() -> {
            if (ledgerReads.incrementAndGet() == 1) {
                jdbcTemplate.update("DELETE FROM wallet_entries WHERE wallet_id = ? AND amount_minor = 50000", wallet);
                jdbcTemplate.update("UPDATE wallets SET balance_minor = 100000 WHERE id = ?", wallet);
            }
        });

        int breaks = reconciler.reconcile();

        assertThat(ledgerReads.get())
                .as("job phải đọc ledger HAI lần (cả lô, rồi đọc lại ví nghi lệch); nếu chỉ một thì "
                        + "hàng rào lần đọc thứ hai không tồn tại và ca này vô nghĩa")
                .isEqualTo(2);
        assertThat(breaks).as("lệch chỉ tồn tại ở lần đọc đầu — không phải sự cố").isZero();
        assertThat(count("reconciliation_breaks")).isZero();
        assertThat(onlyRun().get("status")).isEqualTo("SUCCEEDED");
        assertThat(balanceOf(wallet)).isEqualTo(100_000);
    }

    // ------------------------------------------------------------------
    // 5 — Ví ACTIVE có tiền mà ledger không có tài khoản
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Ví ACTIVE có tiền mà ledger không có tài khoản: break LEDGER_ACCOUNT_MISSING, không tự sửa, số dư KHÔNG bị quét về 0")
    void vi_active_co_tien_ma_ledger_khong_co_tai_khoan_thi_keu_nguoi() {
        UUID tenant = UUID.randomUUID();
        UUID wallet = activeWallet(tenant);
        apply(tenant, Events.topUp(wallet, 80_000));   // read model có tiền; FakeLedger KHÔNG khai gì
        settle();

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        Map<String, Object> row = breaks().getFirst();
        assertThat(row.get("type")).isEqualTo("LEDGER_ACCOUNT_MISSING");
        assertThat(row.get("tenant_id")).isEqualTo(tenant);
        assertThat(row.get("subject")).isEqualTo(wallet.toString());
        assertThat(row.get("expected_minor")).isEqualTo(0L);
        assertThat(row.get("actual_minor")).isEqualTo(80_000L);
        assertThat(row.get("auto_repaired"))
                .as("không có sổ nào để chép về — đây là ca phải gọi người")
                .isEqualTo(false);
        assertThat(row.get("status")).isEqualTo("OPEN");
        assertThat(balanceOf(wallet))
                .as("rebuild lúc này sẽ quét 80 000 của khách về 0 — tệ hơn cả cái lệch")
                .isEqualTo(80_000);
        assertThat(entriesSum(wallet)).isEqualTo(80_000);
    }

    // ------------------------------------------------------------------
    // 5b — auto_repaired phải là SỰ THẬT, không phải ý định (ADR-023 Amendment A1)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Rebuild không sửa được gì thì break KHÔNG được mang cờ 'đã tự sửa'")
    void rebuild_khong_lam_gi_thi_khong_duoc_danh_dau_da_sua() {
        UUID tenant = UUID.randomUUID();
        UUID wallet = activeWallet(tenant);
        apply(tenant, Events.topUp(wallet, 80_000));
        // Ví vừa lệch 2a (số dư khác tổng lịch sử) VỪA không có tài khoản bên ledger. Ứng viên 2a
        // chạy trước và gọi rebuild — nhưng WalletRebuildService thoát sớm khi ledger chưa có tài
        // khoản, nên nó KHÔNG sửa gì cả.
        jdbcTemplate.update("UPDATE wallets SET balance_minor = ? WHERE id = ?", 12_345, wallet);
        settle();

        int breaks = reconciler.reconcile();

        assertThat(breaks).isGreaterThanOrEqualTo(1);
        Map<String, Object> selfMismatch = breaks().stream()
                .filter(row -> "PROJECTION_SELF_MISMATCH".equals(row.get("type")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("thiếu break PROJECTION_SELF_MISMATCH"));
        assertThat(selfMismatch.get("auto_repaired"))
                .as("rebuild không đụng được gì — nói với người trực rằng 'số dư đã đúng lại' là nói dối")
                .isEqualTo(false);
        assertThat(balanceOf(wallet))
                .as("và quả thật số dư vẫn sai")
                .isEqualTo(12_345);
    }

    // ------------------------------------------------------------------
    // 6 — PENDING chưa có tài khoản là bình thường
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Ví PENDING chưa có tài khoản ledger: KHÔNG báo thiếu tài khoản (đó là trạng thái bình thường)")
    void vi_con_pending_khong_bi_bao_thieu_tai_khoan() {
        UUID tenant = UUID.randomUUID();
        UUID pending = TenantContext.callWith(tenant,
                () -> walletService.open(UUID.randomUUID(), ECurrency.VND).wallet().id());
        settle();

        int breaks = reconciler.reconcile();

        assertThat(breaks).isZero();
        assertThat(onlyRun().get("checked_count")).as("ví PENDING vẫn được soi, chỉ là không có gì sai")
                .isEqualTo(1L);
        assertThat(count("reconciliation_breaks")).isZero();
        assertThat(balanceOf(pending)).isZero();
    }

    // ------------------------------------------------------------------
    // 7 — Hai instance cùng cửa sổ
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Hai lần reconcile() cùng cửa sổ (mô phỏng hai instance): lần hai trả -1, chỉ MỘT hàng run, không ghi break đôi")
    void hai_instance_cung_cua_so_thi_chi_mot_luot_chay() {
        UUID tenant = UUID.randomUUID();
        UUID wallet = fundedWallet(tenant, 100_000);
        jdbcTemplate.update("UPDATE wallets SET balance_minor = 5 WHERE id = ?", wallet);
        settle();

        int first = reconciler.reconcile();
        int second = reconciler.reconcile();

        assertThat(first).isEqualTo(1);
        assertThat(second).as("-1 = instance khác đã giành lượt của cửa sổ này").isEqualTo(-1);
        assertThat(count("reconciliation_runs")).as("uk_recon_run_window: đúng MỘT run").isEqualTo(1);
        assertThat(breaks()).hasSize(1);
        assertThat(breaks().getFirst().get("times_seen")).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Ngoài danh sách yêu cầu: hai tính chất của vòng quét
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Quét XUYÊN tenant: break mang đúng tenant của ví lệch, rebuild chạy trong tenant đó, ví tenant khác không bị đụng")
    void quet_xuyen_tenant_break_mang_dung_tenant_cua_vi() {
        UUID tenantA = UUID.randomUUID();
        UUID tenantB = UUID.randomUUID();
        UUID walletA = fundedWallet(tenantA, 10_000);
        UUID walletB = fundedWallet(tenantB, 20_000);
        jdbcTemplate.update("UPDATE wallets SET balance_minor = 123 WHERE id = ?", walletB);
        settle();

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        assertThat(breaks().getFirst().get("tenant_id")).isEqualTo(tenantB);
        assertThat(breaks().getFirst().get("subject")).isEqualTo(walletB.toString());
        assertThat(balanceOf(walletB)).as("rebuild trong đúng tenant B (FakeLedger phân theo tenant)")
                .isEqualTo(20_000);
        assertThat(balanceOf(walletA)).isEqualTo(10_000);
        assertThat(onlyRun().get("checked_count")).isEqualTo(2L);
    }

    @Test
    @DisplayName("Nhiều lô (batch-size=2, 5 ví): quét hết, không bỏ sót ví ở trang cuối")
    void quet_nhieu_lo_khong_bo_sot_vi_nao() {
        UUID tenant = UUID.randomUUID();
        List<UUID> wallets = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            wallets.add(fundedWallet(tenant, 1_000 * (i + 1)));
        }
        // Ví có id lớn nhất nằm ở trang cuối — con trỏ phân trang sai sẽ bỏ sót đúng nó
        UUID last = wallets.stream().max(Comparator.naturalOrder()).orElseThrow();
        jdbcTemplate.update("UPDATE wallets SET balance_minor = balance_minor + 7 WHERE id = ?", last);
        settle();

        int breaks = reconciler.reconcile();

        assertThat(onlyRun().get("checked_count")).as("cả năm ví đều được soi").isEqualTo(5L);
        assertThat(breaks).isEqualTo(1);
        assertThat(breaks().getFirst().get("subject")).isEqualTo(last.toString());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Ví ACTIVE có {@code amount} đã áp vào read model và được ledger khai đúng như vậy. */
    private UUID fundedWallet(UUID tenant, long amount) {
        UUID wallet = activeWallet(tenant);
        LedgerPostedEvent topUp = Events.topUp(wallet, amount);
        apply(tenant, topUp);
        ledger.declare(tenant, wallet, EAccountStatus.ACTIVE, 0, List.of(Events.lineFor(wallet, topUp)));
        return wallet;
    }

    private UUID activeWallet(UUID tenant) {
        return TenantContext.callWith(tenant, () -> {
            UUID wallet = walletService.open(UUID.randomUUID(), ECurrency.VND).wallet().id();
            projector.applyAccountOpened(Events.opened(wallet, EAccountStatus.ACTIVE, 0));
            return wallet;
        });
    }

    private void apply(UUID tenant, LedgerPostedEvent event) {
        TenantContext.runWith(tenant, () -> projector.applyPosted(event, Instant.now()));
    }

    /** Thêm một dòng lịch sử ma VÀ cộng số dư tương ứng: ví vẫn tự khớp (2a) nhưng lệch ledger (2b). */
    private void addPhantomEntry(UUID tenant, UUID wallet, long amount) {
        jdbcTemplate.update("""
                INSERT INTO wallet_entries (tenant_id, wallet_id, entry_id, transaction_id, type, amount_minor)
                VALUES (?, ?, ?, ?, 'TOPUP', ?)
                """, tenant, wallet, UUID.randomUUID(), UUID.randomUUID(), amount);
        jdbcTemplate.update("UPDATE wallets SET balance_minor = balance_minor + ? WHERE id = ?", amount, wallet);
    }

    /** Coi mọi ví là đã yên: lùi {@code updated_at} qua khỏi settle-window (một giờ). */
    private void settle() {
        jdbcTemplate.update("UPDATE wallets SET updated_at = now() - interval '2 hours'");
    }

    private long balanceOf(UUID wallet) {
        return jdbcTemplate.queryForObject("SELECT balance_minor FROM wallets WHERE id = ?", Long.class, wallet);
    }

    private long entriesSum(UUID wallet) {
        return jdbcTemplate.queryForObject(
                "SELECT COALESCE(SUM(amount_minor), 0) FROM wallet_entries WHERE wallet_id = ?", Long.class, wallet);
    }

    private Map<String, Object> onlyRun() {
        List<Map<String, Object>> runs = jdbcTemplate.queryForList("SELECT * FROM reconciliation_runs");
        assertThat(runs).hasSize(1);
        return runs.getFirst();
    }

    private List<Map<String, Object>> breaks() {
        return jdbcTemplate.queryForList("SELECT * FROM reconciliation_breaks ORDER BY first_seen_at");
    }

    private long count(String table) {
        Long n = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
        return n == null ? 0 : n;
    }
}
