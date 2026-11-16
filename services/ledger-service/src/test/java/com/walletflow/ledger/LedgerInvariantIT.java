package com.walletflow.ledger;

import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.reconciliation.ReconciliationScope;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.entity.JournalEntry;
import com.walletflow.ledger.services.AccountService;
import com.walletflow.ledger.services.LedgerInvariantReconciler;
import com.walletflow.ledger.services.LedgerService;
import com.walletflow.ledger.services.SystemAccountProvisioner;
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

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Đối soát TẦNG 1 (roadmap 3.1, ADR-023): bất biến vàng {@code SUM(postings) = 0} được kiểm KHI CHẠY
 * THẬT, không chỉ trong test.
 *
 * <p>Trước job này, "tiền tự sinh / bốc hơi" không có gì phát hiện (GAP-A3). Test này chứng minh job
 * bắt được nó, không kêu oan khi sổ cân, và không tự nhân bản báo động.
 *
 * <p>Cách dựng lệch: trigger append-only ({@code V2}) chỉ chặn UPDATE/DELETE, còn INSERT thẳng bằng
 * SQL vẫn được — đúng kịch bản "một script vận hành lúc 2 giờ sáng ghi nửa bút toán". Posting chèn
 * tay là THỨ DUY NHẤT làm sổ lệch; mọi bút toán đi qua {@link LedgerService} đều cân.
 *
 * <p>Cửa sổ giành lượt làm tròn theo {@code interval}. Test đặt {@code interval} rất dài (một năm)
 * để hai lần gọi trong cùng một test CHẮC CHẮN rơi vào cùng một cửa sổ (ca "chạy trùng") mà không
 * phụ thuộc việc có vắt qua ranh giới giờ hay không. Ca cần cửa sổ KHÁC thì dời {@code window_start}
 * của hàng run cũ ra xa — bảng run không có trigger, và đó đúng là cách một chu kỳ sau sẽ nhìn thấy
 * nó.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "management.tracing.enabled=false",
        // @Scheduled không được chen vào giữa: test gọi reconcile() trực tiếp để kiểm soát thời điểm
        "walletflow.ledger.reconciliation.initial-delay=PT24H",
        "walletflow.ledger.reconciliation.interval=PT8760H"
})
@Testcontainers
class LedgerInvariantIT {

    private static final String BANK = "EXTERNAL_BANK";

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("ledgerdb");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    LedgerInvariantReconciler reconciler;
    @Autowired
    AccountService accountService;
    @Autowired
    LedgerService ledgerService;
    @Autowired
    SystemAccountProvisioner provisioner;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        // Hai bảng đối soát phải sạch để "cửa sổ này đã có run chưa" không phụ thuộc test trước
        jdbcTemplate.execute("TRUNCATE reconciliation_breaks, reconciliation_runs");
        jdbcTemplate.execute("TRUNCATE postings, journal_entries");
        jdbcTemplate.execute("DELETE FROM outbox_event");
        jdbcTemplate.execute("DELETE FROM processed_event");
        jdbcTemplate.execute("DELETE FROM accounts WHERE type = 'USER'");
    }

    // ------------------------------------------------------------------
    // 1 — Sổ cân thì không kêu oan
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Sổ cái cân: reconcile() trả 0, run SUCCEEDED, không ghi break nào")
    void so_cai_can_thi_khong_ghi_chenh_lech_nao() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        String walletA = openWallet(tenantA);
        String walletB = openWallet(tenantB);
        post(tenantA, topUp(walletA, 100_000));
        post(tenantA, topUp(walletA, 25_000));
        post(tenantB, topUp(walletB, 70_000));

        int breaks = reconciler.reconcile();

        assertThat(breaks).as("sổ cân thì không có chỗ lệch nào").isZero();
        Map<String, Object> run = onlyRun();
        assertThat(run.get("status")).isEqualTo("SUCCEEDED");
        assertThat(run.get("kind")).isEqualTo(LedgerInvariantReconciler.KIND);
        assertThat(run.get("break_count")).isEqualTo(0);
        assertThat((Long) run.get("checked_count"))
                .as("đã thật sự soi ít nhất hai nhóm (tenant, VND) + một phép kiểm toàn hệ thống — "
                        + "'0 break' vì không soi gì là kết luận rỗng")
                .isGreaterThanOrEqualTo(3L);
        assertThat(run.get("finished_at")).isNotNull();
        assertThat(count("reconciliation_breaks")).isZero();
    }

    // ------------------------------------------------------------------
    // 2 — ⭐ Bất biến vàng bị phá thì phải đỏ
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Một posting lẻ làm sổ lệch: ghi break LEDGER_IMBALANCE đúng tenant, đúng số tiền, OPEN, không tự sửa")
    void posting_le_lam_lech_thi_ghi_break_do() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        JournalEntry entry = post(tenant, topUp(wallet, 100_000));
        insertStrayPosting(entry.getId(), accountIdOf(tenant, wallet), tenant, 12_345);

        int breaks = reconciler.reconcile();

        // Một posting lẻ làm lệch cả nhóm (tenant, VND) LẪN tổng toàn hệ thống: hai phép kiểm cùng đỏ
        assertThat(breaks).as("nhóm tenant + tổng toàn hệ thống").isEqualTo(2);
        List<Map<String, Object>> imbalance = breaksOfType("LEDGER_IMBALANCE");
        assertThat(imbalance).as("đúng MỘT break theo nhóm").hasSize(1);
        Map<String, Object> row = imbalance.getFirst();
        assertThat(row.get("tenant_id")).isEqualTo(tenant);
        assertThat(row.get("subject")).as("subject = mã loại tiền của nhóm").isEqualTo("VND");
        assertThat(row.get("currency")).isEqualTo("VND");
        assertThat(row.get("expected_minor")).isEqualTo(0L);
        assertThat(row.get("actual_minor")).isEqualTo(12_345L);
        assertThat(row.get("delta_minor")).isEqualTo(12_345L);
        assertThat(row.get("status")).isEqualTo("OPEN");
        assertThat(row.get("auto_repaired"))
                .as("tầng 1 KHÔNG BAO GIỜ tự sửa: ledger append-only và không có 'số đúng' để chép về")
                .isEqualTo(false);
        assertThat(row.get("times_seen")).isEqualTo(1);
        assertThat(onlyRun().get("break_count")).isEqualTo(2);
        assertThat(onlyRun().get("status")).isEqualTo("SUCCEEDED");
    }

    // ------------------------------------------------------------------
    // 3 — Lệch mãi thì không nhân bản break
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Chạy lượt thứ hai khi vẫn lệch: VẪN một hàng break, times_seen=2, first_seen_at giữ nguyên, detected_at mới hơn")
    void chay_lai_khong_nhan_ban_break() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        JournalEntry entry = post(tenant, topUp(wallet, 100_000));
        insertStrayPosting(entry.getId(), accountIdOf(tenant, wallet), tenant, 777);

        reconciler.reconcile();
        Map<String, Object> first = breaksOfType("LEDGER_IMBALANCE").getFirst();

        // Dời hàng run của cửa sổ trước ra xa để lượt kế rơi vào một cửa sổ KHÁC — đây chính là thứ
        // xảy ra một chu kỳ sau. Không dời thì lượt hai bị uk_recon_run_window chặn (ca 4).
        jdbcTemplate.update("UPDATE reconciliation_runs SET window_start = window_start - interval '2 years'");
        int breaks = reconciler.reconcile();

        assertThat(breaks).as("lượt hai vẫn thấy chỗ lệch").isEqualTo(2);
        assertThat(count("reconciliation_runs")).as("hai cửa sổ khác nhau = hai run").isEqualTo(2);
        List<Map<String, Object>> after = breaksOfType("LEDGER_IMBALANCE");
        assertThat(after)
                .as("một chỗ lệch đang mở KHÔNG được nhân bản mỗi lượt (uk_recon_break_open)")
                .hasSize(1);
        Map<String, Object> second = after.getFirst();
        assertThat(second.get("id")).isEqualTo(first.get("id"));
        assertThat(second.get("times_seen")).isEqualTo(2);
        assertThat(second.get("first_seen_at"))
                .as("'lệch từ bao giờ' phải trỏ về lần ĐẦU")
                .isEqualTo(first.get("first_seen_at"));
        assertThat(((Timestamp) second.get("detected_at")).toInstant())
                .isAfter(((Timestamp) first.get("detected_at")).toInstant());
        assertThat(second.get("run_id"))
                .as("break trỏ về lượt gần nhất để truy ngược")
                .isNotEqualTo(first.get("run_id"));
        assertThat(count("reconciliation_breaks"))
                .as("kể cả break toàn cục cũng không nhân bản: tổng cộng đúng hai hàng")
                .isEqualTo(2);
    }

    // ------------------------------------------------------------------
    // 4 — Hai instance cùng cửa sổ
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Hai lần reconcile() cùng cửa sổ (mô phỏng hai instance): lần hai trả -1, chỉ MỘT hàng run, không ghi break đôi")
    void hai_instance_cung_cua_so_thi_chi_mot_luot_chay() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        JournalEntry entry = post(tenant, topUp(wallet, 100_000));
        insertStrayPosting(entry.getId(), accountIdOf(tenant, wallet), tenant, 500);

        int first = reconciler.reconcile();
        int second = reconciler.reconcile();

        assertThat(first).isEqualTo(2);
        assertThat(second)
                .as("-1 = instance khác đã giành lượt của cửa sổ này, bỏ qua im lặng")
                .isEqualTo(-1);
        assertThat(count("reconciliation_runs"))
                .as("uk_recon_run_window: đúng MỘT run cho cửa sổ")
                .isEqualTo(1);
        assertThat(breaksOfType("LEDGER_IMBALANCE")).hasSize(1);
        assertThat(breaksOfType("LEDGER_IMBALANCE").getFirst().get("times_seen"))
                .as("lượt bị bỏ qua không được đếm thêm lần thấy")
                .isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // 5 — Vế mồ côi: từng nhóm vẫn cân nhưng tổng thì không
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Posting trỏ vào account của tenant KHÁC: phép kiểm theo nhóm không thấy, break toàn cục LEDGER_IMBALANCE_GLOBAL bắt được")
    void tong_toan_he_thong_lech_thi_ghi_break_toan_cuc() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        String walletA = openWallet(tenantA);
        String walletB = openWallet(tenantB);
        JournalEntry entry = post(tenantA, topUp(walletA, 100_000));
        post(tenantB, topUp(walletB, 40_000));
        // Posting khai tenant A nhưng trỏ vào account của tenant B: JOIN ... a.tenant_id = p.tenant_id
        // loại nó khỏi phép kiểm theo nhóm. Account B cũng không thấy nó (p.tenant_id lọc).
        insertStrayPosting(entry.getId(), accountIdOf(tenantB, walletB), tenantA, 9_999);

        int breaks = reconciler.reconcile();

        assertThat(breaksOfType("LEDGER_IMBALANCE"))
                .as("mọi nhóm (tenant, currency) vẫn cân vì vế lệch tenant bị JOIN loại — đúng cái hố "
                        + "mà một mình phép kiểm theo nhóm bỏ sót")
                .isEmpty();
        List<Map<String, Object>> global = breaksOfType("LEDGER_IMBALANCE_GLOBAL");
        assertThat(global).hasSize(1);
        Map<String, Object> row = global.getFirst();
        assertThat(row.get("tenant_id")).isEqualTo(ReconciliationScope.SYSTEM_TENANT);
        assertThat(row.get("subject")).isEqualTo("SYSTEM");
        assertThat(row.get("expected_minor")).isEqualTo(0L);
        assertThat(row.get("actual_minor")).isEqualTo(9_999L);
        assertThat(row.get("delta_minor")).isEqualTo(9_999L);
        assertThat(row.get("status")).isEqualTo("OPEN");
        assertThat(row.get("auto_repaired")).isEqualTo(false);
        assertThat(breaks).isEqualTo(1);
        assertThat(onlyRun().get("break_count")).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Chèn thẳng một posting — không có bút toán đối ứng, nên phá bất biến SUM = 0. */
    private void insertStrayPosting(UUID entryId, UUID accountId, UUID tenantId, long amountMinor) {
        // postings không có cột updated_at (đã DROP ở V2) — bảng append-only
        jdbcTemplate.update("""
                INSERT INTO postings (id, entry_id, account_id, tenant_id, amount_minor, created_at)
                VALUES (?, ?, ?, ?, ?, now())
                """, UUID.randomUUID(), entryId, accountId, tenantId, amountMinor);
    }

    private UUID accountIdOf(UUID tenant, String ref) {
        return jdbcTemplate.queryForObject(
                "SELECT id FROM accounts WHERE tenant_id = ? AND external_ref = ? AND currency = 'VND'",
                UUID.class, tenant, ref);
    }

    private Map<String, Object> onlyRun() {
        List<Map<String, Object>> runs = jdbcTemplate.queryForList("SELECT * FROM reconciliation_runs");
        assertThat(runs).hasSize(1);
        return runs.getFirst();
    }

    private List<Map<String, Object>> breaksOfType(String type) {
        return jdbcTemplate.queryForList(
                "SELECT * FROM reconciliation_breaks WHERE type = ? ORDER BY first_seen_at", type);
    }

    private long count(String table) {
        Long n = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
        return n == null ? 0 : n;
    }

    /** Tenant mới có bộ tài khoản hệ thống RIÊNG (ADR-010), nên mỗi test tự dựng tenant của mình. */
    private UUID newTenant() {
        UUID tenant = UUID.randomUUID();
        provisioner.provision(tenant);
        return tenant;
    }

    private String openWallet(UUID tenant) {
        String ref = UUID.randomUUID().toString();
        TenantContext.callWith(tenant,
                () -> accountService.openUserAccount(new OpenAccountCommand(ref, ECurrency.VND)));
        return ref;
    }

    private JournalEntry post(UUID tenant, PostJournalCommand command) {
        return TenantContext.callWith(tenant, () -> ledgerService.postJournal(command));
    }

    private static PostJournalCommand topUp(String wallet, long amountMinor) {
        return new PostJournalCommand(UUID.randomUUID(), EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(BANK, -amountMinor), new Leg(wallet, amountMinor)));
    }
}
