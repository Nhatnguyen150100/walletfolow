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
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Đối soát tầng 2 khi MỌI THỨ KHÔNG SUÔN SẺ (ADR-023 *Amendments* A2, A3).
 *
 * <p>Tách khỏi {@code WalletReconciliationIT} vì hai tính chất dưới đây chỉ quan sát được với cấu
 * hình khác hẳn (trần chỗ lệch rất thấp), mà {@code @SpringBootTest} đặt property theo LỚP.
 *
 * <p>Hai ca ở đây đều nói về cùng một nỗi lo: {@code uk_recon_run_window} làm mỗi cửa sổ chỉ chạy
 * một lần, nên bất cứ thứ gì làm hỏng một lượt đều lấy đi NGUYÊN một chu kỳ không ai kiểm tiền — với
 * tầng 2 là 24 giờ. Vì vậy một lô hỏng không được phép kéo theo cả lượt, và một lần lệch diện rộng
 * phải dừng lại gọi người thay vì tự sửa hàng chục nghìn lần.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "walletflow.wallet.pending-resend.enabled=false",
        "management.tracing.enabled=false",
        "walletflow.wallet.reconciliation.initial-delay=PT24H",
        "walletflow.wallet.reconciliation.interval=PT8760H",
        "walletflow.wallet.reconciliation.settle-window=PT1H",
        // Mỗi lô ĐÚNG MỘT ví: mỗi ví là một lô, nên "lô thứ hai hỏng" nói được chính xác.
        "walletflow.wallet.reconciliation.batch-size=1",
        // Trần thấp để chạm được bằng hai ví thay vì năm trăm.
        "walletflow.wallet.reconciliation.max-breaks-per-run=1"
})
@Import(FakeLedgerConfig.class)
@Testcontainers
class WalletReconciliationLimitsIT {

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

    @Test
    @DisplayName("Lệch diện rộng: chạm trần thì DỪNG quét, lượt là FAILED và nói rõ vì sao")
    void cham_tran_so_cho_lech_thi_dung_quet_va_bao_nguoi() {
        UUID tenant = UUID.randomUUID();
        brokenWallet(tenant, 100_000, 111);
        brokenWallet(tenant, 200_000, 222);
        settle();

        int breaks = reconciler.reconcile();

        assertThat(breaks)
                .as("dừng ngay khi chạm trần, không sửa tiếp ví thứ hai")
                .isEqualTo(1);
        Map<String, Object> run = onlyRun();
        assertThat(run.get("status"))
                .as("quét chưa hết thì KHÔNG được coi là một lượt thành công — nếu không, "
                        + "last_success trẻ lại và alert 'đối soát không chạy' im lặng")
                .isEqualTo("FAILED");
        assertThat((String) run.get("failure_message")).contains("Chạm trần");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reconciliation_breaks", Long.class)).isEqualTo(1L);
    }

    @Test
    @DisplayName("Một lô hỏng không kéo theo cả lượt: các lô sau vẫn được quét")
    void mot_lo_hong_thi_cac_lo_sau_van_duoc_quet() {
        UUID tenant = UUID.randomUUID();
        // Ba ví KHỚP hoàn toàn, nên không ca nào chạm trần chỗ lệch — thứ duy nhất xảy ra là lô hỏng.
        fundedWallet(tenant, 10_000);
        fundedWallet(tenant, 20_000);
        fundedWallet(tenant, 30_000);
        settle();

        // Lô đầu tiên nổ khi hỏi ledger; hai lô sau chạy bình thường.
        AtomicInteger reads = new AtomicInteger();
        ledger.duringRead(() -> {
            if (reads.incrementAndGet() == 1) {
                throw new IllegalStateException("ledger chập chờn");
            }
        });

        reconciler.reconcile();

        Map<String, Object> run = onlyRun();
        assertThat(run.get("status")).isEqualTo("FAILED");
        assertThat((String) run.get("failure_message")).contains("1 lô hỏng");
        assertThat(((Number) run.get("checked_count")).longValue())
                .as("một blip mạng ở lô đầu KHÔNG được làm mất 24 giờ đối soát của mọi ví phía sau")
                .isEqualTo(3L);
    }

    // ------------------------------------------------------------------
    // Helpers — giữ giống WalletReconciliationIT để hai lớp đọc như nhau
    // ------------------------------------------------------------------

    /** Ví mà read model nói một đằng, sổ cái nói một nẻo. */
    private void brokenWallet(UUID tenant, long real, long wrong) {
        UUID wallet = fundedWallet(tenant, real);
        jdbcTemplate.update("UPDATE wallets SET balance_minor = ? WHERE id = ?", wrong, wallet);
    }

    private UUID fundedWallet(UUID tenant, long amount) {
        UUID wallet = TenantContext.callWith(tenant, () -> {
            UUID id = walletService.open(UUID.randomUUID(), ECurrency.VND).wallet().id();
            projector.applyAccountOpened(Events.opened(id, EAccountStatus.ACTIVE, 0));
            return id;
        });
        LedgerPostedEvent topUp = Events.topUp(wallet, amount);
        TenantContext.runWith(tenant, () -> projector.applyPosted(topUp, Instant.now()));
        ledger.declare(tenant, wallet, EAccountStatus.ACTIVE, 0, List.of(Events.lineFor(wallet, topUp)));
        return wallet;
    }

    private void settle() {
        jdbcTemplate.update("UPDATE wallets SET updated_at = now() - interval '2 hours'");
    }

    private Map<String, Object> onlyRun() {
        List<Map<String, Object>> runs = jdbcTemplate.queryForList("SELECT * FROM reconciliation_runs");
        assertThat(runs).hasSize(1);
        return runs.getFirst();
    }
}
