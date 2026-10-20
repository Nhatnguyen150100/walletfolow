package com.walletflow.transaction;

import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.services.StuckSagaScanner;
import com.walletflow.transaction.services.TopupService;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Job quét saga treo (roadmap 1.2b). Gọi {@code scan()} trực tiếp — lịch tự chạy được đẩy ra xa để
 * không chen vào giữa test.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "walletflow.transaction.saga-scan.interval-ms=3600000",
        "walletflow.transaction.saga-scan.max-attempts=3",
        "walletflow.transaction.saga-deadline=PT2M",
        "management.tracing.enabled=false"
})
@Import(FakeWalletDirectoryConfig.class)
@Testcontainers
class StuckSagaIT {

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
    TopupService topupService;
    @Autowired
    StuckSagaScanner scanner;
    @Autowired
    FakeWalletDirectory wallets;
    @Autowired
    EnvelopeReader envelopeReader;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    MeterRegistry meterRegistry;
    @Autowired
    PlatformTransactionManager transactionManager;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE saga_state, transactions, idempotency_keys, outbox_event");
        wallets.reset();
    }

    @Test
    @DisplayName("⭐ Quá hạn: gửi lại CÙNG transactionId + vế, eventId MỚI; đếm lần thử; hạn chót giãn ra")
    void qua_han_thi_gui_lai_voi_event_id_moi() {
        Accepted txn = accept(TENANT);
        UUID originalEventId = commandEventId(txn);
        makeOverdue(txn);

        assertThat(scanner.scan()).isEqualTo(1);

        List<EventEnvelope<PostJournalCommand>> commands = commands(txn);
        assertThat(commands).hasSize(2);
        EventEnvelope<PostJournalCommand> retry = commands.get(1);
        assertThat(retry.eventId())
                .as("eventId cũ bị ledger bỏ qua MÀ KHÔNG phát lại kết quả -> saga treo vĩnh viễn")
                .isNotEqualTo(originalEventId);
        assertThat(retry.payload())
                .as("cùng transactionId và cùng vế -> ledger chỉ ghi MỘT bút toán dù nhận hai lệnh")
                .isEqualTo(commands.get(0).payload());

        Map<String, Object> saga = saga(txn);
        assertThat(saga.get("attempt_count")).isEqualTo(2);
        assertThat(saga.get("command_event_id")).isEqualTo(retry.eventId());
        assertThat(((Timestamp) saga.get("deadline_at")).toInstant())
                .as("lần thử thứ 2 chờ 2 lần hạn chót")
                .isAfter(Instant.now().plusSeconds(200));
    }

    @Test
    @DisplayName("Chưa quá hạn, hoặc saga đã đóng (DONE/FAILED): không gửi lại")
    void khong_dong_vao_saga_chua_qua_han_hoac_da_dong() {
        Accepted fresh = accept(TENANT);
        Accepted done = accept(TENANT);
        Accepted failed = accept(TENANT);
        makeOverdue(done);
        makeOverdue(failed);
        jdbcTemplate.update("UPDATE saga_state SET step_status = 'DONE' WHERE transaction_id = ?", done.transactionId());
        jdbcTemplate.update("UPDATE saga_state SET step_status = 'FAILED' WHERE transaction_id = ?", failed.transactionId());

        assertThat(scanner.scan()).isZero();
        assertThat(commands(fresh)).hasSize(1);
        assertThat(commands(done)).hasSize(1);
        assertThat(commands(failed))
                .as("gửi lại một giao dịch đã báo FAILED có thể làm tiền đi sau khi người dùng được báo thất bại")
                .hasSize(1);
    }

    @Test
    @DisplayName("Hết lượt thử: MANUAL_REVIEW + metric cho alert, KHÔNG gửi thêm, KHÔNG đoán FAILED/COMPLETED")
    void het_luot_thi_chuyen_manual_review() {
        Accepted txn = accept(TENANT);
        jdbcTemplate.update("UPDATE saga_state SET attempt_count = 3 WHERE transaction_id = ?", txn.transactionId());
        makeOverdue(txn);
        double before = counter("walletflow.saga.manual_review");

        scanner.scan();

        assertThat(jdbcTemplate.queryForObject("SELECT status FROM transactions WHERE id = ?",
                String.class, txn.transactionId())).isEqualTo("MANUAL_REVIEW");
        assertThat(jdbcTemplate.queryForObject("SELECT failure_code FROM transactions WHERE id = ?",
                String.class, txn.transactionId())).isEqualTo("LEDGER_NO_RESPONSE");
        assertThat(saga(txn).get("step_status")).isEqualTo("MANUAL_REVIEW");
        assertThat(commands(txn)).hasSize(1);
        assertThat(counter("walletflow.saga.manual_review")).isEqualTo(before + 1);

        assertThat(scanner.scan()).as("không quét lại saga đã chuyển người xử lý").isZero();
    }

    @Test
    @DisplayName("Saga của tenant khác: lệnh gửi lại mang ĐÚNG tenant của saga trong envelope")
    void gui_lai_dung_tenant() {
        UUID tenantB = UUID.randomUUID();
        Accepted txn = accept(tenantB);
        makeOverdue(txn);

        scanner.scan();

        assertThat(commands(txn).get(1).tenantId())
                .as("job chạy xuyên tenant — quên đặt TenantContext là lệnh ghi vào sổ của khách khác")
                .isEqualTo(tenantB);
    }

    @Test
    @DisplayName("⭐ 4 job quét chạy song song trên 5 saga quá hạn: mỗi saga được gửi lại ĐÚNG MỘT lần (SKIP LOCKED)")
    void nhieu_instance_khong_gui_trung() throws Exception {
        List<Accepted> overdue = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Accepted txn = accept(TENANT);
            makeOverdue(txn);
            overdue.add(txn);
        }

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        int total = 0;
        try {
            List<Future<Integer>> results = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                results.add(pool.submit((Callable<Integer>) () -> {
                    start.await();
                    return scanner.scan();
                }));
            }
            start.countDown();
            for (Future<Integer> result : results) {
                total += result.get();
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(total).isEqualTo(5);
        for (Accepted txn : overdue) {
            assertThat(commands(txn)).as("không bị hai instance cùng gửi lại").hasSize(2);
        }
    }

    @Test
    @DisplayName("Saga đang được trả lời (hàng saga đang bị khoá): job quét BỎ QUA, không gửi lại")
    void bo_qua_saga_dang_duoc_tra_loi() throws Exception {
        Accepted txn = accept(TENANT);
        makeOverdue(txn);

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            // Đóng vai SagaService đang xử lý câu trả lời của ledger: giữ khoá hàng saga
            Future<?> answering = pool.submit(() -> new TransactionTemplate(transactionManager).executeWithoutResult(s -> {
                jdbcTemplate.queryForList("SELECT * FROM saga_state WHERE transaction_id = ? FOR UPDATE",
                        txn.transactionId());
                locked.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
            locked.await(10, TimeUnit.SECONDS);

            assertThat(scanner.scan())
                    .as("không chờ (treo cả lô) cũng không gửi lại một giao dịch sắp có câu trả lời")
                    .isZero();

            release.countDown();
            answering.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertThat(commands(txn)).hasSize(1);
    }

    @Test
    @DisplayName("Một saga lỗi lặp lại KHÔNG chặn các saga treo khác: bị đẩy lùi hạn chót + metric")
    void saga_loi_khong_chan_hang_doi() {
        Accepted poison = accept(TENANT);
        Accepted healthy = accept(TENANT);
        // Saga quá hạn LÂU NHẤT đứng đầu hàng — và xử lý nó luôn lỗi (dữ liệu hỏng: currency lạ)
        jdbcTemplate.update("UPDATE saga_state SET deadline_at = now() - interval '1 hour' WHERE transaction_id = ?",
                poison.transactionId());
        jdbcTemplate.update("UPDATE transactions SET currency = 'XXX' WHERE id = ?", poison.transactionId());
        makeOverdue(healthy);
        double failuresBefore = counter("walletflow.saga.scan_failures");

        scanner.scan();

        assertThat(commands(healthy))
                .as("không có cơ chế này thì saga lỗi đứng đầu hàng ở MỌI lượt và saga khỏe không bao giờ tới lượt")
                .hasSize(2);
        assertThat(((Timestamp) saga(poison).get("deadline_at")).toInstant())
                .as("saga lỗi bị đẩy ra sau, không còn chặn đầu hàng")
                .isAfter(Instant.now());
        assertThat(counter("walletflow.saga.scan_failures")).isEqualTo(failuresBefore + 1);
    }

    @Test
    @DisplayName("Runbook: mở lại một ca MANUAL_REVIEW (đặt lại CẢ saga lẫn giao dịch) thì job quét gửi lại")
    void mo_lai_manual_review_theo_runbook() {
        Accepted txn = accept(TENANT);
        jdbcTemplate.update("UPDATE saga_state SET attempt_count = 3 WHERE transaction_id = ?", txn.transactionId());
        makeOverdue(txn);
        scanner.scan();
        assertThat(saga(txn).get("step_status")).isEqualTo("MANUAL_REVIEW");

        // Đúng câu lệnh trong docs/runbooks/saga-stuck.md
        new TransactionTemplate(transactionManager).executeWithoutResult(s -> {
            jdbcTemplate.update("UPDATE transactions SET status = 'PROCESSING', failure_code = NULL, "
                    + "failure_reason = NULL, updated_at = now() WHERE id = ? AND status = 'MANUAL_REVIEW'",
                    txn.transactionId());
            jdbcTemplate.update("UPDATE saga_state SET step_status = 'SENT', attempt_count = 0, "
                    + "deadline_at = now(), updated_at = now() WHERE transaction_id = ? AND step_status = 'MANUAL_REVIEW'",
                    txn.transactionId());
        });
        jdbcTemplate.update("UPDATE saga_state SET deadline_at = now() - interval '1 second' WHERE transaction_id = ?",
                txn.transactionId());

        assertThat(scanner.scan()).isEqualTo(1);

        assertThat(commands(txn))
                .as("lệnh gửi lại giữ transactionId: nếu ledger đã ghi sổ, nó trả bút toán cũ và saga tự đóng")
                .hasSize(2);
        assertThat(saga(txn).get("step_status")).isEqualTo("SENT");
    }

    @Test
    @DisplayName("⭐ Lệnh nạp quá hạn ở bước thu tiền: gửi lại lệnh THU, KHÔNG phải lệnh ghi sổ")
    void lenh_nap_qua_han_thi_gui_lai_lenh_thu() {
        Accepted txn = acceptTopup();
        makeOverdue(txn);

        assertThat(scanner.scan()).isEqualTo(1);

        assertThat(outbox(WalletTopics.CMD_BANK_CHARGE, txn))
                .as("bank-adapter idempotent theo (tenant, transactionId): gửi lại chỉ phát lại câu trả lời")
                .hasSize(2);
        assertThat(commands(txn))
                .as("gửi lệnh ghi sổ cho một lệnh nạp chưa ai trả tiền là cộng tiền từ không khí vào ví")
                .isEmpty();
    }

    @Test
    @DisplayName("⭐ Lệnh nạp đang chờ người dùng trả tiền (AWAITING_PAYMENT): saga KHÔNG bị đóng nhầm")
    void khong_dong_nham_saga_nap_dang_cho_tra_tien() {
        Accepted txn = acceptTopup();
        jdbcTemplate.update("UPDATE transactions SET status = 'AWAITING_PAYMENT' WHERE id = ?", txn.transactionId());
        makeOverdue(txn);

        scanner.scan();

        assertThat(saga(txn).get("step_status"))
                .as("đóng saga ở đây là bỏ rơi một lệnh nạp còn sống: không ai gửi lại, không ai báo lỗi")
                .isEqualTo("SENT");
        assertThat(outbox(WalletTopics.CMD_BANK_CHARGE, txn)).hasSize(2);
    }

    @Test
    @DisplayName("Lệnh nạp quá hạn ở bước ghi sổ: gửi lại lệnh ghi sổ TOPUP với vế EXTERNAL_BANK")
    void lenh_nap_qua_han_o_buoc_ghi_so() {
        Accepted txn = acceptTopup();
        jdbcTemplate.update("UPDATE transactions SET status = 'POSTING' WHERE id = ?", txn.transactionId());
        jdbcTemplate.update("UPDATE saga_state SET current_step = 'POST_JOURNAL' WHERE transaction_id = ?",
                txn.transactionId());
        makeOverdue(txn);

        scanner.scan();

        List<EventEnvelope<PostJournalCommand>> journals = commands(txn);
        assertThat(journals).hasSize(1);
        assertThat(journals.get(0).payload().type()).isEqualTo(EJournalType.TOPUP);
        assertThat(journals.get(0).payload().legs()).extracting("accountRef")
                .containsExactly("EXTERNAL_BANK", txn.to().toString());
        assertThat(outbox(WalletTopics.CMD_BANK_CHARGE, txn))
                .as("đã qua bước thu tiền — gửi lại lệnh thu là tạo thêm một phiên thanh toán").hasSize(1);
    }

    @Test
    @DisplayName("Counter của saga tồn tại ở 0 ngay từ khởi động — nếu không, alert increase() bỏ lỡ lần đầu tiên")
    void counter_ton_tai_tu_khoi_dong() {
        // status_conflict không bao giờ được tăng trong lớp test này: nó tồn tại chỉ khi được đăng ký
        // lúc khởi động.
        for (String name : List.of("walletflow.saga.retries", "walletflow.saga.manual_review",
                "walletflow.saga.scan_failures", "walletflow.saga.status_conflict")) {
            assertThat(meterRegistry.find(name).counter()).as(name).isNotNull();
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private Accepted accept(UUID tenant) {
        return SagaFixture.accept(transferService, wallets, tenant, 40_000);
    }

    private Accepted acceptTopup() {
        return SagaFixture.acceptTopup(topupService, wallets, TENANT, 40_000);
    }

    private List<String> outbox(String topic, Accepted txn) {
        return jdbcTemplate.queryForList(
                "SELECT payload FROM outbox_event WHERE topic = ? AND payload LIKE ? ORDER BY created_at",
                String.class, topic, "%" + txn.transactionId() + "%");
    }

    private void makeOverdue(Accepted txn) {
        jdbcTemplate.update("UPDATE saga_state SET deadline_at = now() - interval '1 second' WHERE transaction_id = ?",
                txn.transactionId());
    }

    private UUID commandEventId(Accepted txn) {
        return jdbcTemplate.queryForObject("SELECT command_event_id FROM saga_state WHERE transaction_id = ?",
                UUID.class, txn.transactionId());
    }

    private Map<String, Object> saga(Accepted txn) {
        return jdbcTemplate.queryForMap("SELECT * FROM saga_state WHERE transaction_id = ?", txn.transactionId());
    }

    private List<EventEnvelope<PostJournalCommand>> commands(Accepted txn) {
        return jdbcTemplate.queryForList(
                        "SELECT payload FROM outbox_event WHERE topic = ? AND payload LIKE ? ORDER BY created_at",
                        String.class, WalletTopics.CMD_POST_JOURNAL, "%" + txn.transactionId() + "%")
                .stream().map(json -> envelopeReader.read(json, PostJournalCommand.class)).toList();
    }

    private double counter(String name) {
        Counter counter = meterRegistry.find(name).counter();
        return counter == null ? 0 : counter.count();
    }
}
