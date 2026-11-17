package com.walletflow.bank;

import com.walletflow.bank.services.ProviderLedgerReconciler;
import com.walletflow.bank.support.FakeLedger;
import com.walletflow.bank.support.FakeLedgerConfig;
import com.walletflow.bank.support.ProviderRows;
import com.walletflow.bank.support.ReconciliationDb;
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
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Đối soát bank-adapter ↔ sổ cái khi MỌI THỨ KHÔNG SUÔN SẺ (ADR-023 <i>Amendments</i> A2, A3; ADR-024).
 *
 * <p>Tách khỏi {@code ProviderLedgerReconciliationIT} vì hai lẽ: cấu hình khác (trần chỗ lệch rất thấp), và
 * gauge {@code last_success_timestamp} — mọi test ở đây kết thúc {@code FAILED}, nên gauge PHẢI ở nguyên 0 suốt
 * cả lớp. Nếu để chung lớp với test chạy sạch, gauge đã được đặt bởi test khác (cùng một cửa sổ) và
 * "không được cập nhật" không còn phân biệt được với "được đặt lại đúng giá trị cũ".
 *
 * <p>Nỗi lo chung: {@code uk_recon_run_window} làm mỗi cửa sổ chỉ chạy một lần, nên một lượt hỏng lấy đi cả
 * một chu kỳ không ai kiểm tiền. Vì vậy một lô hỏng không được kéo theo cả lượt, lượt hỏng phải LỘ RA (status
 * FAILED + counter + không đặt mốc thành công), và lệch diện rộng phải dừng lại gọi người.
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
        "walletflow.bank.status-check.enabled=false",
        "walletflow.bank.webhook.enabled=false",
        "walletflow.bank.reconciliation.initial-delay=PT24H",
        "walletflow.bank.reconciliation.interval=PT8760H",
        "walletflow.bank.reconciliation.settle-window=PT1H",
        // Mỗi lô hai lệnh: "lô thứ nhất hỏng, lô thứ hai vẫn chạy" nói được chính xác
        "walletflow.bank.reconciliation.batch-size=2",
        // Trần thấp để chạm được bằng sáu lệnh thay vì năm trăm (kiểm tra sau MỖI lô nên có thể vượt tối đa một lô)
        "walletflow.bank.reconciliation.max-breaks-per-run=3"
})
@Import(FakeLedgerConfig.class)
@Testcontainers
class ProviderLedgerReconciliationFailuresIT {

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
    double failuresBefore;

    @BeforeEach
    void reset() {
        rows = new ProviderRows(jdbcTemplate);
        db = new ReconciliationDb(jdbcTemplate);
        db.clean();
        ledger.reset();
        tenant = UUID.randomUUID();
        failuresBefore = failures();
    }

    @Test
    @DisplayName("⭐ Ledger chết ở MỘT lô: lô đó bị bỏ qua, lượt FAILED (lô hỏng), counter tăng, lô khác vẫn ghi break, mốc thành công KHÔNG đặt")
    void ledger_chet_o_mot_lo_thi_lo_khac_van_chay_va_luot_bao_hong() {
        List<UUID> txns = seedMismatchedCharges(4);
        // Lô 1 = hai lệnh đầu (con trỏ theo id hàng thu); lời gọi ledger đầu tiên thuộc về lô 1
        ledger.failNextMovementCalls(1);

        int breaks = reconciler.reconcile();

        Map<String, Object> run = db.onlyRun();
        assertThat(run.get("status")).as("một lô hỏng: lượt KHÔNG được báo thành công").isEqualTo("FAILED");
        assertThat((String) run.get("failure_message")).contains("lô hỏng");
        assertThat(run.get("checked_count")).as("cả bốn lệnh đã được duyệt tới, kể cả lô hỏng").isEqualTo(4L);
        assertThat(breaks).isEqualTo(2);
        assertThat(db.breaks()).extracting(b -> (String) b.get("subject"))
                .as("lô 2 (lệnh 3, 4) vẫn được soi và ghi; lô 1 bị bỏ qua chứ không bị thử lại vô hạn")
                .containsExactlyInAnyOrder(txns.get(2) + ":VND", txns.get(3) + ":VND");
        assertThat(run.get("break_count")).isEqualTo(2);
        assertThat(failures()).as("alert ReconciliationRunFailed dựa vào counter này").isEqualTo(failuresBefore + 1);
        assertThat(lastSuccess())
                .as("lượt hỏng không được đẩy mốc 'đối soát thành công gần nhất' — nếu đẩy, job chết mà alert im")
                .isEqualTo(0.0);
    }

    @Test
    @DisplayName("⭐ Trang bút toán nạp lỗi: chiều ngược dừng, lượt FAILED; break của chiều xuôi vẫn được ghi, mốc thành công KHÔNG đặt")
    void trang_nap_loi_thi_chieu_xuoi_van_ghi_va_luot_bao_hong() {
        UUID txn = UUID.randomUUID();
        rows.charge(tenant, txn, "SUCCEEDED", 100_000);
        ledger.failTopups();

        int breaks = reconciler.reconcile();

        assertThat(breaks).isEqualTo(1);
        Map<String, Object> theBreak = db.onlyBreak();
        assertThat(theBreak.get("subject")).isEqualTo(txn + ":VND");
        assertThat(theBreak.get("type")).isEqualTo("PROVIDER_MONEY_NOT_POSTED");
        Map<String, Object> run = db.onlyRun();
        assertThat(run.get("status")).isEqualTo("FAILED");
        assertThat((String) run.get("failure_message")).contains("lô hỏng").contains("chiều ngược");
        assertThat(run.get("break_count")).isEqualTo(1);
        assertThat(ledger.topupCalls()).as("không bỏ qua trang rồi quét tiếp — không biết con trỏ kế tiếp")
                .hasSize(1);
        assertThat(failures()).isEqualTo(failuresBefore + 1);
        assertThat(lastSuccess()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("⭐ Lệch diện rộng chạm trần max-breaks-per-run: DỪNG quét (không soi lô sau, không quét ngược), lượt FAILED nói rõ 'Chạm trần'")
    void cham_tran_so_cho_lech_thi_dung_quet_va_bao_nguoi() {
        List<UUID> txns = seedMismatchedCharges(6);
        // Có một bút toán nạp mồ côi: nếu chiều ngược chạy thì nó thành break thứ 5
        UUID orphan = UUID.randomUUID();
        ledger.declareTopup(tenant, orphan, 5_000, Instant.now().minus(2, ChronoUnit.HOURS));

        int breaks = reconciler.reconcile();

        Map<String, Object> run = db.onlyRun();
        assertThat(run.get("status")).isEqualTo("FAILED");
        assertThat((String) run.get("failure_message")).contains("Chạm trần");
        assertThat(breaks).as("trần 3 kiểm sau mỗi lô nên dừng sau lô thứ hai (4 break)").isEqualTo(4);
        assertThat(run.get("break_count")).isEqualTo(4);
        assertThat(db.breaks()).extracting(b -> (String) b.get("subject"))
                .as("hai lệnh cuối chưa được soi, và bút toán mồ côi chưa được quét")
                .containsExactlyInAnyOrder(txns.get(0) + ":VND", txns.get(1) + ":VND",
                        txns.get(2) + ":VND", txns.get(3) + ":VND");
        assertThat(ledger.topupCalls()).as("chiều ngược bị bỏ qua khi đã chạm trần").isEmpty();
        assertThat(failures()).isEqualTo(failuresBefore + 1);
        assertThat(lastSuccess()).isEqualTo(0.0);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * {@code n} lệnh thu SUCCEEDED 10k mà sổ cái không có gì (mỗi lệnh sẽ là một break). Id hàng thu tăng dần
     * để thứ tự duyệt (theo id) và vì thế ranh giới lô là xác định.
     */
    private List<UUID> seedMismatchedCharges(int n) {
        return IntStream.rangeClosed(1, n).mapToObj(i -> {
            UUID chargeId = UUID.fromString("00000000-0000-0000-0000-0000000000%02d".formatted(i));
            UUID txn = UUID.randomUUID();
            rows.charge(chargeId, tenant, txn, "SUCCEEDED", 10_000, ProviderRows.SETTLED_SECONDS_AGO);
            return txn;
        }).toList();
    }

    private double failures() {
        return meterRegistry.get("walletflow.reconciliation.failures").tag("tier", "bank").counter().count();
    }

    private double lastSuccess() {
        return meterRegistry.get("walletflow.reconciliation.last_success_timestamp").tag("tier", "bank")
                .gauge().value();
    }
}
