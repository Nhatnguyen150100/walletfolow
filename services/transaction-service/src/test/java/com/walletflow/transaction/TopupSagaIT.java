package com.walletflow.transaction;

import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.BankCompletedEvent;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.TransactionCompletedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Saga nạp tiền (roadmap 2.4a, ADR-019).
 *
 * <p>Tính chất phải chứng minh ở đây, theo thứ tự quan trọng:
 * <ol>
 *   <li><b>Không ghi sổ khi chưa thu được tiền.</b> Lời báo "đã tạo phiên thanh toán" không phải kết
 *       quả; chỉ lời XÁC NHẬN của provider mới mở đường tới ledger (ADR-016 §1).</li>
 *   <li><b>Thu một lần, ghi sổ một lần.</b> Event của bank-adapter trùng/muộn/sai thứ tự là chuyện
 *       thường ngày, không cái nào được tạo thêm một lệnh ghi sổ.</li>
 *   <li><b>Không nói dối khi tiền đã đi.</b> Thu được tiền mà ledger từ chối thì phải có người xử lý,
 *       không được lặng lẽ đánh FAILED.</li>
 * </ol>
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
class TopupSagaIT {

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

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE saga_state, transactions, idempotency_keys, outbox_event");
        wallets.reset();
    }

    @Test
    @DisplayName("Đường đi đủ: nhận lệnh → phiên thanh toán → provider xác nhận → ledger ghi sổ → COMPLETED")
    void nap_tien_di_het_duong() {
        Accepted txn = acceptTopup();
        assertThat(status(txn)).isEqualTo("PROCESSING");
        assertThat(chargeCommands(txn)).as("lệnh thu tiền được gửi ngay khi nhận lệnh").hasSize(1);

        onChargePending(txn);
        assertThat(status(txn)).isEqualTo("AWAITING_PAYMENT");
        assertThat(jdbcTemplate.queryForObject("SELECT payment_url FROM transactions WHERE id = ?",
                String.class, txn.transactionId()))
                .as("không lưu được trang thanh toán thì người dùng không có đường để trả tiền")
                .isEqualTo("https://sandbox.example/pay/MOCK-" + txn.transactionId());

        onBankCompleted(txn);
        assertThat(status(txn)).isEqualTo("POSTING");
        List<PostJournalCommand> journals = journalCommands(txn);
        assertThat(journals).hasSize(1);
        assertThat(journals.get(0).type()).isEqualTo(EJournalType.TOPUP);
        assertThat(journals.get(0).legs())
                .as("tiền rời tài khoản hệ thống EXTERNAL_BANK và vào ví — hai vế triệt tiêu, SUM = 0")
                .extracting("accountRef", "amountMinor")
                .containsExactly(tuple("EXTERNAL_BANK", -AMOUNT), tuple(txn.to().toString(), AMOUNT));

        onPosted(txn);
        assertThat(status(txn)).isEqualTo("COMPLETED");
        assertThat(sagaStatus(txn)).isEqualTo("DONE");

        List<String> completed = events(WalletTopics.EVT_TXN_COMPLETED);
        assertThat(completed).hasSize(1);
        EventSchemas.assertValid(WalletTopics.EVT_TXN_COMPLETED, completed.get(0));
        TransactionCompletedEvent event =
                envelopeReader.read(completed.get(0), TransactionCompletedEvent.class).payload();
        assertThat(event.fromWalletId()).as("nạp tiền không có ví nguồn").isNull();
        assertThat(event.toWalletId()).isEqualTo(txn.to());
        assertThat(event.amountMinor()).isEqualTo(AMOUNT);
    }

    @Test
    @DisplayName("⭐ Mới có trang thanh toán, chưa ai trả tiền: KHÔNG lệnh ghi sổ nào được gửi")
    void chua_thu_duoc_tien_thi_khong_ghi_so() {
        Accepted txn = acceptTopup();

        onChargePending(txn);
        onChargePending(txn);

        assertThat(journalCommands(txn))
                .as("ghi sổ ở đây là cộng vào ví số tiền chưa ai trả — tiền tự sinh từ không khí")
                .isEmpty();
        assertThat(status(txn)).isEqualTo("AWAITING_PAYMENT");
    }

    @Test
    @DisplayName("⭐ Xác nhận thu tiền giao lại 3 lần: vẫn đúng MỘT lệnh ghi sổ")
    void xac_nhan_lap_chi_ghi_so_mot_lan() {
        Accepted txn = acceptTopup();
        onChargePending(txn);

        onBankCompleted(txn);
        onBankCompleted(txn);
        onBankCompleted(txn);

        assertThat(journalCommands(txn))
                .as("lệnh ghi sổ thứ hai với transactionId khác eventId là một bút toán thứ hai")
                .hasSize(1);
    }

    @Test
    @DisplayName("Kết quả cuối đến TRƯỚC lời báo tạo phiên: không bị kéo ngược về phòng chờ")
    void ket_qua_den_truoc_loi_bao_tao_phien() {
        Accepted txn = acceptTopup();

        onBankCompleted(txn);
        onChargePending(txn);

        assertThat(status(txn))
                .as("hai topic không có thứ tự — máy trạng thái chỉ được tiến, không được lùi")
                .isEqualTo("POSTING");
        assertThat(journalCommands(txn)).hasSize(1);
    }

    @Test
    @DisplayName("Người dùng không trả tiền / phiên hết hạn: FAILED sạch, không ai mất tiền")
    void provider_tu_choi_thi_that_bai_sach() {
        Accepted txn = acceptTopup();
        onChargePending(txn);

        onBankFailed(txn, "PAYMENT_EXPIRED");

        assertThat(status(txn)).isEqualTo("FAILED");
        assertThat(sagaStatus(txn)).isEqualTo("FAILED");
        assertThat(journalCommands(txn)).isEmpty();
        List<String> failed = events(WalletTopics.EVT_TXN_FAILED);
        assertThat(failed).hasSize(1);
        EventSchemas.assertValid(WalletTopics.EVT_TXN_FAILED, failed.get(0));
    }

    @Test
    @DisplayName("⭐ Đã thu được tiền nhưng ledger TỪ CHỐI: MANUAL_REVIEW, KHÔNG phải FAILED")
    void ledger_tu_choi_sau_khi_da_thu_tien_thi_can_nguoi_xem() {
        Accepted txn = acceptTopup();
        onChargePending(txn);
        onBankCompleted(txn);
        double before = counter("walletflow.topup.manual_review");

        TenantContext.runWith(TENANT, () -> sagaService.onRejected(SagaFixture.topupRejected(txn, "ACCOUNT_CLOSED")));

        assertThat(status(txn))
                .as("FAILED nói với người dùng 'không mất tiền', trong khi ngân hàng đã trừ họ")
                .isEqualTo("MANUAL_REVIEW");
        assertThat(sagaStatus(txn)).isEqualTo("MANUAL_REVIEW");
        assertThat(events(WalletTopics.EVT_TXN_FAILED)).isEmpty();
        assertThat(counter("walletflow.topup.manual_review")).isEqualTo(before + 1);
        assertThat(jdbcTemplate.queryForObject("SELECT provider_ref FROM transactions WHERE id = ?",
                String.class, txn.transactionId()))
                .as("người xử lý cần mã của provider để đối chiếu rồi hoàn tiền")
                .isEqualTo("MOCK-" + txn.transactionId());
    }

    @Test
    @DisplayName("⭐ Xác nhận thu tiền LỆCH số tiền so với lệnh đã gửi: không ghi sổ, chuyển người xử lý")
    void xac_nhan_lech_so_tien_thi_khong_ghi_so() {
        Accepted txn = acceptTopup();
        onChargePending(txn);
        double before = counter("walletflow.topup.result_conflict");

        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(
                SagaFixture.bankCompleted(txn, txn.to(), AMOUNT + 1, ECurrency.VND)));

        assertThat(journalCommands(txn))
                .as("ghi theo số của ta thì lệch sổ với ngân hàng, ghi theo số của họ thì cộng sai cho ví")
                .isEmpty();
        assertThat(status(txn)).isEqualTo("MANUAL_REVIEW");
        assertThat(jdbcTemplate.queryForObject("SELECT failure_code FROM transactions WHERE id = ?",
                String.class, txn.transactionId())).isEqualTo("BANK_AMOUNT_MISMATCH");
        assertThat(counter("walletflow.topup.result_conflict")).isEqualTo(before + 1);
    }

    @Test
    @DisplayName("Xác nhận thu tiền vào VÍ KHÁC với lệnh đã gửi: không ghi sổ")
    void xac_nhan_lech_vi_thi_khong_ghi_so() {
        Accepted txn = acceptTopup();
        onChargePending(txn);

        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(
                SagaFixture.bankCompleted(txn, UUID.randomUUID(), AMOUNT, ECurrency.VND)));

        assertThat(journalCommands(txn)).isEmpty();
        assertThat(status(txn)).isEqualTo("MANUAL_REVIEW");
    }

    @Test
    @DisplayName("Provider báo THẤT BẠI sau khi đã xác nhận thu được tiền: giữ nguyên trạng thái + đếm xung đột")
    void bao_that_bai_sau_khi_da_xac_nhan_khong_keo_nguoc() {
        Accepted txn = acceptTopup();
        onChargePending(txn);
        onBankCompleted(txn);
        onPosted(txn);
        double before = counter("walletflow.topup.result_conflict");

        onBankFailed(txn, "DECLINED");

        assertThat(status(txn))
                .as("ledger đã ghi bút toán — không trạng thái nào ở đây được phủ nhận điều đó")
                .isEqualTo("COMPLETED");
        assertThat(counter("walletflow.topup.result_conflict"))
                .as("mâu thuẫn thật thì phải có người biết, dù máy không tự sửa")
                .isEqualTo(before + 1);
    }

    @Test
    @DisplayName("⭐ Saga bị đóng tay nhưng tiền đã thu: KHÔNG biến mất im lặng, chuyển người xử lý")
    void saga_lech_voi_giao_dich_thi_khong_bien_mat_im_lang() {
        Accepted txn = acceptTopup();
        onChargePending(txn);
        // Ca của runbook mục D làm sai: mở lại hàng transactions mà quên saga_state (hoặc ngược lại).
        jdbcTemplate.update("UPDATE saga_state SET step_status = 'DONE' WHERE transaction_id = ?",
                txn.transactionId());
        double before = counter("walletflow.topup.manual_review");

        onBankCompleted(txn);

        assertThat(status(txn))
                .as("để nguyên AWAITING_PAYMENT là tiền của khách nằm ở provider mà không query, job quét "
                        + "hay counter nào nhìn thấy")
                .isEqualTo("MANUAL_REVIEW");
        assertThat(jdbcTemplate.queryForObject("SELECT failure_code FROM transactions WHERE id = ?",
                String.class, txn.transactionId())).isEqualTo("SAGA_STATE_DESYNC");
        assertThat(journalCommands(txn)).isEmpty();
        assertThat(counter("walletflow.topup.manual_review")).isEqualTo(before + 1);
    }

    @Test
    @DisplayName("⭐ Bản lặp đến sau khi đã MANUAL_REVIEW: KHÔNG ghi đè nguyên nhân gốc")
    void ban_lap_khong_ghi_de_nguyen_nhan_goc() {
        Accepted txn = acceptTopup();
        onChargePending(txn);
        onBankCompleted(txn);
        TenantContext.runWith(TENANT, () -> sagaService.onRejected(SagaFixture.topupRejected(txn, "ACCOUNT_CLOSED")));
        double reviewsAfterFirst = counter("walletflow.topup.manual_review");

        onBankCompleted(txn);
        TenantContext.runWith(TENANT, () -> sagaService.onRejected(SagaFixture.topupRejected(txn, "ACCOUNT_CLOSED")));

        assertThat(jdbcTemplate.queryForObject("SELECT failure_code FROM transactions WHERE id = ?",
                String.class, txn.transactionId()))
                .as("mã này là thứ runbook dùng để phân loại ca — bản lặp xoá nó là đẩy người xử lý sang "
                        + "đúng mục sai")
                .isEqualTo("ACCOUNT_CLOSED");
        assertThat(counter("walletflow.topup.manual_review"))
                .as("alert mức page không được thổi phồng bởi message giao lại")
                .isEqualTo(reviewsAfterFirst);
    }

    @Test
    @DisplayName("⭐ LedgerRejected lạc tới lệnh nạp chưa gửi lệnh ghi sổ: KHÔNG đóng saga còn sống")
    void tu_choi_lac_khong_dong_saga_con_song() {
        Accepted txn = acceptTopup();
        onChargePending(txn);
        double before = counter("walletflow.topup.manual_review");

        TenantContext.runWith(TENANT, () -> sagaService.onRejected(SagaFixture.topupRejected(txn, "ACCOUNT_CLOSED")));

        assertThat(status(txn))
                .as("chưa ai mất đồng nào, và đóng saga ở đây là bỏ rơi phiên thanh toán đang mở")
                .isEqualTo("AWAITING_PAYMENT");
        assertThat(sagaStatus(txn)).isEqualTo("SENT");
        assertThat(counter("walletflow.topup.manual_review")).isEqualTo(before);
    }

    @Test
    @DisplayName("Đã FAILED rồi provider mới xác nhận ĐÃ THU: không tự ghi sổ, chuyển người xử lý")
    void xac_nhan_den_sau_khi_da_that_bai() {
        Accepted txn = acceptTopup();
        onBankFailed(txn, "PAYMENT_EXPIRED");

        onBankCompleted(txn);

        assertThat(status(txn)).isEqualTo("MANUAL_REVIEW");
        assertThat(journalCommands(txn))
                .as("hai kết quả trái nhau từ provider — không đoán bên nào đúng rồi ghi tiền theo")
                .isEmpty();
    }

    @Test
    @DisplayName("Event của thao tác khác (REFUND) hoặc của tenant khác: không đụng vào lệnh nạp")
    void bo_qua_event_khong_phai_cua_minh() {
        Accepted txn = acceptTopup();
        onChargePending(txn);

        BankCompletedEvent refund = new BankCompletedEvent(txn.transactionId(), EBankOperation.REFUND,
                txn.to(), SagaFixture.PROVIDER, "MOCK-RF", AMOUNT, ECurrency.VND);
        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(refund));
        TenantContext.runWith(UUID.randomUUID(), () -> sagaService.onBankCompleted(SagaFixture.bankCompleted(txn)));

        assertThat(status(txn))
                .as("hoàn tiền là luồng của 2.4b, không phải câu trả lời cho bước thu tiền")
                .isEqualTo("AWAITING_PAYMENT");
        assertThat(journalCommands(txn)).isEmpty();
    }

    @Test
    @DisplayName("Counter của luồng nạp tồn tại ở 0 từ khởi động — nếu không, alert increase() bỏ lỡ lần đầu")
    void counter_ton_tai_tu_khoi_dong() {
        for (String name : List.of("walletflow.topup.manual_review", "walletflow.topup.result_conflict")) {
            assertThat(meterRegistry.find(name).counter()).as(name).isNotNull();
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private Accepted acceptTopup() {
        return SagaFixture.acceptTopup(topupService, wallets, TENANT, AMOUNT);
    }

    private void onChargePending(Accepted txn) {
        TenantContext.runWith(txn.tenantId(), () -> sagaService.onChargePending(SagaFixture.chargePending(txn)));
    }

    private void onBankCompleted(Accepted txn) {
        TenantContext.runWith(txn.tenantId(), () -> sagaService.onBankCompleted(SagaFixture.bankCompleted(txn)));
    }

    private void onBankFailed(Accepted txn, String code) {
        TenantContext.runWith(txn.tenantId(), () -> sagaService.onBankFailed(SagaFixture.bankFailed(txn, code)));
    }

    private void onPosted(Accepted txn) {
        TenantContext.runWith(txn.tenantId(), () -> sagaService.onPosted(SagaFixture.topupPosted(txn)));
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

    private List<PostJournalCommand> journalCommands(Accepted txn) {
        return outbox(WalletTopics.CMD_POST_JOURNAL, txn).stream()
                .map(json -> envelopeReader.read(json, PostJournalCommand.class).payload()).toList();
    }

    private List<String> chargeCommands(Accepted txn) {
        return outbox(WalletTopics.CMD_BANK_CHARGE, txn);
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
