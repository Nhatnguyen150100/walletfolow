package com.walletflow.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.TransactionCompletedEvent;
import com.walletflow.common.event.TransactionFailedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.services.SagaService;
import com.walletflow.transaction.services.TransferService;
import com.walletflow.transaction.support.FakeWalletDirectory;
import com.walletflow.transaction.support.FakeWalletDirectoryConfig;
import com.walletflow.transaction.support.SagaFixture;
import com.walletflow.transaction.support.SagaFixture.Accepted;
import io.micrometer.core.instrument.Counter;
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

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Máy trạng thái của saga (roadmap 1.2b). Hai event của một giao dịch đi trên hai topic khác nhau —
 * không có thứ tự, có thể lặp. Mỗi ca dưới đây là một cách chúng đến mà production SẼ gặp.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "walletflow.transaction.saga-scan.enabled=false",
        "management.tracing.enabled=false"
})
@Import(FakeWalletDirectoryConfig.class)
@Testcontainers
class SagaStateMachineIT {

    private static final UUID TENANT = TenantContext.DEFAULT_TENANT;

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("txndb");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    TransferService transferService;
    @Autowired
    SagaService sagaService;
    @Autowired
    FakeWalletDirectory wallets;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    EnvelopeReader envelopeReader;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    MeterRegistry meterRegistry;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE resolution_requests, saga_state, transactions, idempotency_keys, outbox_event");
        wallets.reset();
    }

    @Test
    @DisplayName("LedgerPosted: PROCESSING → COMPLETED, saga DONE, đúng MỘT event TransactionCompleted đúng schema")
    void ghi_so_thi_hoan_tat() {
        Accepted txn = accept();

        onPosted(txn);

        assertThat(status(txn)).isEqualTo("COMPLETED");
        assertThat(sagaStatus(txn)).isEqualTo("DONE");
        List<String> completed = events(WalletTopics.EVT_TXN_COMPLETED);
        assertThat(completed).hasSize(1);
        EventSchemas.assertValid(WalletTopics.EVT_TXN_COMPLETED, completed.get(0));
        TransactionCompletedEvent event =
                envelopeReader.read(completed.get(0), TransactionCompletedEvent.class).payload();
        assertThat(event.transactionId()).isEqualTo(txn.transactionId());
        assertThat(event.amountMinor()).isEqualTo(txn.amount());
    }

    @Test
    @DisplayName("LedgerRejected: → FAILED với mã lỗi; event chỉ mang MÃ, không mang câu chữ (có số dư) của ledger")
    void tu_choi_thi_that_bai() {
        Accepted txn = accept();

        onRejected(txn, "INSUFFICIENT_FUNDS");

        assertThat(status(txn)).isEqualTo("FAILED");
        assertThat(jdbcTemplate.queryForObject("SELECT failure_code FROM transactions WHERE id = ?",
                String.class, txn.transactionId())).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(sagaStatus(txn)).isEqualTo("FAILED");
        List<String> failed = events(WalletTopics.EVT_TXN_FAILED);
        assertThat(failed).hasSize(1);
        EventSchemas.assertValid(WalletTopics.EVT_TXN_FAILED, failed.get(0));
        assertThat(envelopeReader.read(failed.get(0), TransactionFailedEvent.class).payload().failureCode())
                .isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(failed.get(0)).as("thông điệp kỹ thuật của ledger không đi tới người dùng")
                .doesNotContain("Balance =");
    }

    @Test
    @DisplayName("LedgerPosted giao lại 3 lần: vẫn chỉ MỘT thông báo hoàn tất")
    void su_kien_lap_chi_phat_mot_thong_bao() {
        Accepted txn = accept();

        onPosted(txn);
        onPosted(txn);
        onPosted(txn);

        assertThat(events(WalletTopics.EVT_TXN_COMPLETED))
                .as("thông báo trùng là lỗi người dùng NHÌN THẤY được").hasSize(1);
    }

    @Test
    @DisplayName("Rejected của lệnh gốc đến SAU posted của lần gửi lại: vẫn COMPLETED — tiền đã đi")
    void tu_choi_den_sau_ghi_so_bi_bo_qua() {
        Accepted txn = accept();

        onPosted(txn);
        onRejected(txn, "INSUFFICIENT_FUNDS");

        assertThat(status(txn)).isEqualTo("COMPLETED");
        assertThat(events(WalletTopics.EVT_TXN_FAILED)).isEmpty();
    }

    @Test
    @DisplayName("⭐ Đã báo FAILED rồi ledger GHI SỔ lần gửi lại: ledger thắng → COMPLETED, kèm cảnh báo xung đột")
    void ghi_so_sau_that_bai_thi_ledger_thang() {
        Accepted txn = accept();
        double conflictsBefore = conflicts();

        onRejected(txn, "INSUFFICIENT_FUNDS");
        onPosted(txn);

        assertThat(status(txn))
                .as("giữ FAILED là báo cáo nói dối về tiền đã thật sự di chuyển")
                .isEqualTo("COMPLETED");
        assertThat(events(WalletTopics.EVT_TXN_FAILED)).hasSize(1);
        assertThat(events(WalletTopics.EVT_TXN_COMPLETED)).hasSize(1);
        assertThat(conflicts()).isEqualTo(conflictsBefore + 1);
    }

    @Test
    @DisplayName("MANUAL_REVIEW rồi câu trả lời đến muộn: saga tự đóng (posted → COMPLETED, rejected → FAILED)")
    void manual_review_tu_dong_khi_co_cau_tra_loi() {
        Accepted posted = accept();
        Accepted rejected = accept();
        jdbcTemplate.update("UPDATE transactions SET status = 'MANUAL_REVIEW'");

        onPosted(posted);
        onRejected(rejected, "ACCOUNT_FROZEN");

        assertThat(status(posted)).isEqualTo("COMPLETED");
        assertThat(status(rejected)).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("Event không phải của mình (giao dịch lạ, tenant khác, REVERSAL cùng transactionId) bị bỏ qua")
    void bo_qua_event_khong_phai_cua_minh() {
        Accepted txn = accept();
        Accepted stranger = new Accepted(TENANT, UUID.randomUUID(), txn.from(), txn.to(), 1);

        onPosted(stranger);
        TenantContext.runWith(UUID.randomUUID(), () -> sagaService.onPosted(SagaFixture.posted(txn)));
        TenantContext.runWith(TENANT, () -> sagaService.onPosted(SagaFixture.posted(txn, EJournalType.REVERSAL)));

        assertThat(status(txn))
                .as("REVERSAL bù trừ về sau cho giao dịch này không phải câu trả lời cho lệnh TRANSFER")
                .isEqualTo("PROCESSING");
        assertThat(events(WalletTopics.EVT_TXN_COMPLETED)).isEmpty();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private Accepted accept() {
        return SagaFixture.accept(transferService, wallets, TENANT, 25_000);
    }

    private void onPosted(Accepted txn) {
        TenantContext.runWith(txn.tenantId(), () -> sagaService.onPosted(SagaFixture.posted(txn)));
    }

    private void onRejected(Accepted txn, String code) {
        TenantContext.runWith(txn.tenantId(), () -> sagaService.onRejected(SagaFixture.rejected(txn, code)));
    }

    private String status(Accepted txn) {
        return jdbcTemplate.queryForObject("SELECT status FROM transactions WHERE id = ?",
                String.class, txn.transactionId());
    }

    private String sagaStatus(Accepted txn) {
        return jdbcTemplate.queryForObject("SELECT step_status FROM saga_state WHERE transaction_id = ?",
                String.class, txn.transactionId());
    }

    private List<String> events(String topic) {
        return jdbcTemplate.queryForList("SELECT payload FROM outbox_event WHERE topic = ? ORDER BY created_at",
                String.class, topic);
    }

    private double conflicts() {
        Counter counter = meterRegistry.find("walletflow.saga.status_conflict").counter();
        return counter == null ? 0 : counter.count();
    }
}
