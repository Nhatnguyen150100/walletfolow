package com.walletflow.transaction;

import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.BankRefundCommand;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.TransactionFailedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.repositories.SagaRepository;
import com.walletflow.transaction.services.SagaService;
import com.walletflow.transaction.services.TopupService;
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

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bù trừ của saga nạp tiền (roadmap 2.4b, ADR-020) — gồm cả ca hoàn KHÔNG thành công
 * ({@code CompensationFailureIT} của roadmap §2.4): hai nhóm ca dùng chung đúng một bối cảnh Spring,
 * nên để chung một lớp thay vì khởi động hai container Postgres cho cùng một luồng.
 *
 * <p>Tính chất phải chứng minh, theo thứ tự quan trọng:
 * <ol>
 *   <li><b>Đã thu tiền mà không ghi sổ được thì tiền phải quay về người dùng</b>, không nằm chờ người.</li>
 *   <li><b>Không bao giờ vừa hoàn vừa cộng ví.</b> Trong lúc hoàn, không lệnh ghi sổ nào được gửi; và
 *       nếu ledger vẫn ghi sổ (một instance chưa có hàng rào V6) thì dừng lại gọi người, không tự quyết.</li>
 *   <li><b>Chỉ hoàn hai ca biết CHẮC đã thu tiền.</b> "Không biết chắc" thì vẫn chờ người — không biết
 *       đã thu bao nhiêu thì không biết phải hoàn bao nhiêu (ADR-020 Context).</li>
 *   <li><b>Hoàn không được thì không giấu.</b> Tiền vẫn ở provider ⇒ MANUAL_REVIEW + counter.</li>
 * </ol>
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "walletflow.transaction.saga-scan.enabled=false",
        // Cờ của ADR-020 §5: mặc định TẮT ở prod (bật sau khi mọi instance ledger lên V6). Hành vi khi
        // tắt được giữ bởi TopupSagaIT.
        "walletflow.transaction.topup.refund-enabled=true",
        "management.tracing.enabled=false"
})
@Import(FakeWalletDirectoryConfig.class)
@Testcontainers
class TopupCompensationIT {

    private static final UUID TENANT = TenantContext.DEFAULT_TENANT;
    private static final long AMOUNT = 500_000L;

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
    TopupService topupService;
    @Autowired
    SagaService sagaService;
    @Autowired
    FakeWalletDirectory wallets;
    @Autowired
    EnvelopeReader envelopeReader;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    MeterRegistry meterRegistry;
    @Autowired
    SagaRepository sagaRepository;
    @Autowired
    PlatformTransactionManager transactionManager;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE resolution_requests, saga_state, transactions, idempotency_keys, outbox_event");
        wallets.reset();
    }

    @Test
    @DisplayName("⭐ Thu được tiền nhưng ledger TỪ CHỐI: hoàn tự động, ví không được cộng, kết thúc FAILED")
    void ledger_tu_choi_thi_hoan_tien_tu_dong() {
        Accepted txn = chargedTopup();
        double refundsBefore = counter("walletflow.topup.refund_started");
        double reviewsBefore = counter("walletflow.topup.manual_review");
        int journalsBefore = journalCommands(txn).size();

        reject(txn, "ACCOUNT_CLOSED");

        assertThat(status(txn)).isEqualTo("REFUNDING");
        assertThat(sagaStep(txn)).isEqualTo(SagaService.STEP_BANK_REFUND);
        assertThat(sagaStatus(txn))
                .as("saga phải còn MỞ: lệnh hoàn mất trên đường thì job quét mới gửi lại được")
                .isEqualTo("SENT");
        List<EventEnvelope<BankRefundCommand>> refunds = refundCommands(txn);
        assertThat(refunds).hasSize(1);
        BankRefundCommand refund = refunds.get(0).payload();
        assertThat(refund.walletId()).isEqualTo(txn.to());
        assertThat(refund.amountMinor())
                .as("chỉ hoàn TRỌN số đã thu — hoàn theo con số khác là trả người dùng tiền họ chưa trả")
                .isEqualTo(AMOUNT);
        assertThat(refund.currency()).isEqualTo(ECurrency.VND);
        assertThat(refund.reasonCode()).isEqualTo("ACCOUNT_CLOSED");
        EventSchemas.assertValid(WalletTopics.CMD_BANK_REFUND, rawRefundCommands(txn).get(0));
        assertThat(counter("walletflow.topup.refund_started")).isEqualTo(refundsBefore + 1);
        assertThat(counter("walletflow.topup.manual_review"))
                .as("ca tự phục hồi được không được tính vào alert 'cần người can thiệp'")
                .isEqualTo(reviewsBefore);
        assertThat(events(WalletTopics.EVT_TXN_FAILED))
                .as("chưa hoàn xong thì chưa được báo kết quả cho người dùng").isEmpty();

        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(SagaFixture.refundCompleted(txn)));

        assertThat(status(txn)).isEqualTo("FAILED");
        assertThat(sagaStatus(txn)).isEqualTo("FAILED");
        assertThat(failureCode(txn))
                .as("mã nguyên nhân gốc là thứ báo cáo dùng để biết vì sao lệnh nạp hỏng")
                .isEqualTo("ACCOUNT_CLOSED");
        assertThat(journalCommands(txn))
                .as("vẫn đúng lệnh ghi sổ GỐC của startPosting — một lệnh thứ hai sau khi đã hoàn tiền "
                        + "là vừa trả tiền về thẻ vừa cộng ví")
                .hasSize(journalsBefore);
        List<String> failed = events(WalletTopics.EVT_TXN_FAILED);
        assertThat(failed).hasSize(1);
        EventSchemas.assertValid(WalletTopics.EVT_TXN_FAILED, failed.get(0));
    }

    @Test
    @DisplayName("⭐ Đã FAILED rồi provider mới xác nhận ĐÃ THU: hoàn tiền với mã BANK_RESULT_CONFLICT")
    void hai_ket_qua_trai_nhau_thi_hoan_tien() {
        Accepted txn = SagaFixture.acceptTopup(topupService, wallets, TENANT, AMOUNT);
        TenantContext.runWith(TENANT, () -> sagaService.onBankFailed(
                SagaFixture.bankFailed(txn, "PAYMENT_EXPIRED")));
        assertThat(status(txn)).isEqualTo("FAILED");

        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(SagaFixture.bankCompleted(txn)));

        assertThat(status(txn))
                .as("FAILED là trạng thái đóng, nhưng bù trừ là bước TIẾP của saga — tiền đã thu phải trả lại")
                .isEqualTo("REFUNDING");
        assertThat(refundCommands(txn)).singleElement()
                .extracting(envelope -> envelope.payload().reasonCode())
                .isEqualTo("BANK_RESULT_CONFLICT");
        assertThat(journalCommands(txn))
                .as("hai kết quả trái nhau thì KHÔNG ghi sổ — chỉ trả lại tiền").isEmpty();

        UUID failedEventId = envelopeReader.read(events(WalletTopics.EVT_TXN_FAILED).get(0),
                TransactionFailedEvent.class).eventId();
        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(SagaFixture.refundCompleted(txn)));

        assertThat(status(txn)).isEqualTo("FAILED");
        assertThat(failureCode(txn)).isEqualTo("BANK_RESULT_CONFLICT");
        assertThat(events(WalletTopics.EVT_TXN_FAILED))
                .as("báo thất bại lần hai mang ĐÚNG eventId cũ nên consumer dedup được — người dùng không "
                        + "nhận hai thông báo cho một lệnh")
                .allSatisfy(json -> assertThat(
                        envelopeReader.read(json, TransactionFailedEvent.class).eventId())
                        .isEqualTo(failedEventId));
    }

    @Test
    @DisplayName("⭐ Lời từ chối của ledger giao lại nhiều lần: vẫn đúng MỘT lệnh hoàn")
    void tu_choi_lap_chi_gui_mot_lenh_hoan() {
        Accepted txn = chargedTopup();

        reject(txn, "ACCOUNT_CLOSED");
        reject(txn, "ACCOUNT_CLOSED");
        reject(txn, "ACCOUNT_CLOSED");

        assertThat(refundCommands(txn))
                .as("lệnh hoàn thứ hai với eventId khác là một lần hoàn thứ hai nếu bank-adapter lỡ mất "
                        + "tính idempotent — không được dựa vào đó")
                .hasSize(1);
        assertThat(attemptCount(txn))
                .as("event lặp không được nạp lại ngân sách thử lại của bước hoàn")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("⭐ Hàng rào của repository: gọi thẳng startCompensation lần hai không nạp lại ngân sách thử lại")
    void mo_buoc_hoan_lan_hai_khong_an() {
        Accepted txn = chargedTopup();
        reject(txn, "ACCOUNT_CLOSED");
        // Job quét đã gửi lại hai lần trước khi event lặp tới — nếu lần mở bước thứ hai "ăn", nó kéo
        // attempt_count về 1 và bước hoàn có thêm một ngân sách thử lại nữa, lặp vô hạn.
        jdbcTemplate.update("UPDATE saga_state SET attempt_count = 3 WHERE transaction_id = ?",
                txn.transactionId());

        Integer opened = new TransactionTemplate(transactionManager).execute(ignored ->
                sagaRepository.startCompensation(TENANT, txn.transactionId(),
                        SagaService.STEP_BANK_REFUND, UUID.randomUUID(), 120));

        assertThat(opened)
                .as("saga đã ở bước hoàn rồi — câu UPDATE phải không ăn hàng nào")
                .isZero();
        assertThat(attemptCount(txn)).isEqualTo(3);
    }

    @Test
    @DisplayName("⭐ Ledger ghi sổ SAU KHI đã hoàn xong: không bao giờ COMPLETED, không báo thành công")
    void ledger_ghi_so_sau_khi_da_hoan_xong_thi_khong_completed() {
        Accepted txn = chargedTopup();
        reject(txn, "ACCOUNT_CLOSED");
        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(SagaFixture.refundCompleted(txn)));
        assertThat(status(txn)).isEqualTo("FAILED");
        double conflictsBefore = counter("walletflow.topup.result_conflict");

        // Bản gửi lại của lệnh ghi sổ nằm sẵn trên topic, hoặc một DLT vừa được giải phóng, hoặc một
        // instance ledger chưa có hàng rào V6. Cửa sổ REFUNDING đã đóng từ lâu — nếu hàng rào chỉ treo
        // trên trạng thái đó thì ca này rơi vào "ledger thắng" và giao dịch thành COMPLETED.
        TenantContext.runWith(TENANT, () -> sagaService.onPosted(SagaFixture.topupPosted(txn)));

        assertThat(status(txn))
                .as("COMPLETED ở đây là ví được cộng VÀ tiền đã về thẻ, lại còn báo THÀNH CÔNG nên không "
                        + "ai nhìn lại — ca tạo ra tiền nguy hiểm nhất của luồng bù trừ")
                .isEqualTo("MANUAL_REVIEW");
        assertThat(failureCode(txn)).isEqualTo("REFUND_POSTED_CONFLICT");
        assertThat(counter("walletflow.topup.result_conflict")).isEqualTo(conflictsBefore + 1);
        assertThat(events(WalletTopics.EVT_TXN_COMPLETED))
                .as("đã báo FAILED cho người dùng rồi; báo tiếp COMPLETED là nói ngược lại chính mình")
                .isEmpty();
    }

    @Test
    @DisplayName("⭐ Xác nhận thu tiền giao lại SAU KHI đã hoàn xong: giao dịch đóng đúng, không alert giả")
    void xac_nhan_thu_tien_lap_sau_khi_hoan_xong_khong_mo_lai() {
        Accepted txn = chargedTopup();
        reject(txn, "ACCOUNT_CLOSED");
        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(SagaFixture.refundCompleted(txn)));
        double conflictsBefore = counter("walletflow.topup.result_conflict");
        double reviewsBefore = counter("walletflow.topup.manual_review");

        // Outbox at-least-once, hoặc bank-adapter phát lại câu trả lời hiện tại sau một lần gửi lại
        // (ADR-016). Sau 2.4b, FAILED là kết cục ĐÚNG của bù trừ — nên `case FAILED` gặp bản lặp này
        // là kịch bản thường ngày, không phải "hai kết quả trái nhau".
        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(SagaFixture.bankCompleted(txn)));

        assertThat(status(txn)).isEqualTo("FAILED");
        assertThat(failureCode(txn))
                .as("kéo sang MANUAL_REVIEW là xoá mã nguyên nhân gốc — runbook mất thứ nó dùng để phân loại")
                .isEqualTo("ACCOUNT_CLOSED");
        assertThat(refundCommands(txn))
                .as("lệnh hoàn thứ hai cho một giao dịch đã hoàn xong là hoàn hai lần").hasSize(1);
        assertThat(counter("walletflow.topup.manual_review"))
                .as("alert mức page cho một ca không có gì sai làm người trực ngừng tin alert")
                .isEqualTo(reviewsBefore);
        assertThat(counter("walletflow.topup.result_conflict")).isEqualTo(conflictsBefore);
    }

    @Test
    @DisplayName("Xác nhận hoàn tiền tới khi ví ĐÃ được cộng: kêu to, không nuốt im lặng")
    void xac_nhan_hoan_khi_da_completed_thi_keu_to() {
        Accepted txn = chargedTopup();
        TenantContext.runWith(TENANT, () -> sagaService.onPosted(SagaFixture.topupPosted(txn)));
        assertThat(status(txn)).isEqualTo("COMPLETED");
        double conflictsBefore = counter("walletflow.topup.result_conflict");

        // Lần hoàn do người vận hành gửi tay, hoặc tàn dư của một ca đã mở lại theo runbook. Dù đường
        // nào thì đây cũng là tín hiệu "ví được cộng VÀ tiền đã trả lại" — không được dừng ở debug.
        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(SagaFixture.refundCompleted(txn)));

        assertThat(status(txn))
                .as("không tự lật COMPLETED: bút toán là sự thật, sửa nó phải bằng REVERSAL có audit (2.5)")
                .isEqualTo("COMPLETED");
        assertThat(counter("walletflow.topup.result_conflict")).isEqualTo(conflictsBefore + 1);
    }

    @Test
    @DisplayName("⭐ Provider TỪ CHỐI hoàn (VNPay trước 2.4c): MANUAL_REVIEW, không giấu đi")
    void khong_hoan_duoc_thi_chuyen_nguoi_xu_ly() {
        Accepted txn = chargedTopup();
        reject(txn, "ACCOUNT_CLOSED");
        double failuresBefore = counter("walletflow.topup.refund_failed");
        double reviewsBefore = counter("walletflow.topup.manual_review");

        TenantContext.runWith(TENANT, () -> sagaService.onBankFailed(
                SagaFixture.refundFailed(txn, "REFUND_UNSUPPORTED")));

        assertThat(status(txn))
                .as("tiền vẫn ở chỗ provider — FAILED ở đây là nói với người dùng 'bạn không mất tiền'")
                .isEqualTo("MANUAL_REVIEW");
        assertThat(failureCode(txn)).isEqualTo("REFUND_DECLINED");
        assertThat(failureReason(txn))
                .as("người hoàn tay cần cả lời từ chối của provider lẫn nguyên nhân phải hoàn")
                .contains("REFUND_UNSUPPORTED").contains("ACCOUNT_CLOSED");
        assertThat(events(WalletTopics.EVT_TXN_FAILED)).isEmpty();
        assertThat(counter("walletflow.topup.refund_failed")).isEqualTo(failuresBefore + 1);
        assertThat(counter("walletflow.topup.manual_review")).isEqualTo(reviewsBefore + 1);
    }

    @Test
    @DisplayName("⭐ Ledger ghi sổ TRONG KHI đang hoàn tiền: dừng lại gọi người, không tự quyết bên nào thắng")
    void ledger_ghi_so_trong_khi_dang_hoan_thi_goi_nguoi() {
        Accepted txn = chargedTopup();
        reject(txn, "ACCOUNT_CLOSED");
        double conflictsBefore = counter("walletflow.topup.result_conflict");

        // Chỉ xảy ra khi một instance ledger chưa có hàng rào rejected_topups (V6) — hoặc bút toán được
        // ghi tay. Hàng rào sinh ra để ca này KHÔNG tồn tại; nếu nó vẫn tới, đây là lúc có người thức dậy.
        TenantContext.runWith(TENANT, () -> sagaService.onPosted(SagaFixture.topupPosted(txn)));

        assertThat(status(txn))
                .as("COMPLETED ở đây là nói ví đã được cộng trong khi tiền cũng đang về thẻ — tạo ra tiền")
                .isEqualTo("MANUAL_REVIEW");
        assertThat(failureCode(txn)).isEqualTo("REFUND_POSTED_CONFLICT");
        assertThat(counter("walletflow.topup.result_conflict")).isEqualTo(conflictsBefore + 1);
        assertThat(events(WalletTopics.EVT_TXN_COMPLETED)).isEmpty();
    }

    @Test
    @DisplayName("⭐ Xác nhận thu tiền LỆCH số tiền: vẫn chờ người, KHÔNG hoàn tự động")
    void ca_khong_biet_chac_da_thu_bao_nhieu_thi_khong_hoan() {
        Accepted txn = SagaFixture.acceptTopup(topupService, wallets, TENANT, AMOUNT);
        TenantContext.runWith(TENANT, () -> sagaService.onChargePending(SagaFixture.chargePending(txn)));

        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(
                SagaFixture.bankCompleted(txn, txn.to(), AMOUNT + 1, ECurrency.VND)));

        assertThat(status(txn)).isEqualTo("MANUAL_REVIEW");
        assertThat(failureCode(txn)).isEqualTo("BANK_AMOUNT_MISMATCH");
        assertThat(refundCommands(txn))
                .as("hai bên nói lệch nhau về số tiền thì không biết phải hoàn bao nhiêu — đoán là sai tiền")
                .isEmpty();
    }

    @Test
    @DisplayName("Counter của luồng hoàn tiền tồn tại ở 0 từ khởi động — nếu không, alert increase() bỏ lỡ lần đầu")
    void counter_ton_tai_tu_khoi_dong() {
        for (String name : List.of("walletflow.topup.refund_started", "walletflow.topup.refund_failed")) {
            assertThat(meterRegistry.find(name).counter()).as(name).isNotNull();
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Lệnh nạp đã được provider xác nhận thu tiền — điểm xuất phát của mọi ca bù trừ. */
    private Accepted chargedTopup() {
        Accepted txn = SagaFixture.acceptTopup(topupService, wallets, TENANT, AMOUNT);
        TenantContext.runWith(TENANT, () -> sagaService.onChargePending(SagaFixture.chargePending(txn)));
        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(SagaFixture.bankCompleted(txn)));
        return txn;
    }

    private void reject(Accepted txn, String code) {
        TenantContext.runWith(TENANT, () -> sagaService.onRejected(SagaFixture.topupRejected(txn, code)));
    }

    private String status(Accepted txn) {
        return column(txn, "status");
    }

    private String failureCode(Accepted txn) {
        return column(txn, "failure_code");
    }

    private String failureReason(Accepted txn) {
        return column(txn, "failure_reason");
    }

    private String column(Accepted txn, String name) {
        return jdbcTemplate.queryForObject("SELECT " + name + " FROM transactions WHERE id = ?",
                String.class, txn.transactionId());
    }

    private String sagaStatus(Accepted txn) {
        return jdbcTemplate.queryForObject("SELECT step_status FROM saga_state WHERE transaction_id = ?",
                String.class, txn.transactionId());
    }

    private String sagaStep(Accepted txn) {
        return jdbcTemplate.queryForObject("SELECT current_step FROM saga_state WHERE transaction_id = ?",
                String.class, txn.transactionId());
    }

    private int attemptCount(Accepted txn) {
        return jdbcTemplate.queryForObject("SELECT attempt_count FROM saga_state WHERE transaction_id = ?",
                Integer.class, txn.transactionId());
    }

    private List<String> events(String topic) {
        return jdbcTemplate.queryForList("SELECT payload FROM outbox_event WHERE topic = ? ORDER BY created_at",
                String.class, topic);
    }

    private List<EventEnvelope<BankRefundCommand>> refundCommands(Accepted txn) {
        return rawRefundCommands(txn).stream()
                .map(json -> envelopeReader.read(json, BankRefundCommand.class)).toList();
    }

    private List<String> rawRefundCommands(Accepted txn) {
        return outbox(WalletTopics.CMD_BANK_REFUND, txn);
    }

    private List<PostJournalCommand> journalCommands(Accepted txn) {
        return outbox(WalletTopics.CMD_POST_JOURNAL, txn).stream()
                .map(json -> envelopeReader.read(json, PostJournalCommand.class).payload()).toList();
    }

    private List<String> outbox(String topic, Accepted txn) {
        return jdbcTemplate.queryForList(
                "SELECT payload FROM outbox_event WHERE topic = ? AND payload LIKE ? ORDER BY created_at",
                String.class, topic, "%" + txn.transactionId() + "%");
    }

    private double counter(String name) {
        Counter counter = meterRegistry.find(name).counter();
        return counter == null ? 0 : counter.count();
    }
}
