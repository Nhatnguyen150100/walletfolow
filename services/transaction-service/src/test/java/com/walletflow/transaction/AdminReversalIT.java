package com.walletflow.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.walletflow.common.bank.ProviderContext;
import com.walletflow.common.bank.ProviderOperationView;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.LedgerRejectedEvent;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.TransactionFailedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.ledger.EntryLine;
import com.walletflow.common.ledger.JournalEntryView;
import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.transaction.services.SagaService;
import com.walletflow.transaction.support.FakeCrossContextConfig;
import com.walletflow.transaction.support.FakeLedgerEntriesClient;
import com.walletflow.transaction.support.FakeProviderContextClient;
import com.walletflow.transaction.support.FakeWalletDirectory;
import com.walletflow.transaction.support.FakeWalletDirectoryConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Đảo bút toán có 4-eyes cho ca {@code REFUND_POSTED_CONFLICT} (roadmap 2.5 PR4, ADR-022).
 *
 * <p>Ca áp dụng: ledger đã ghi bút toán nạp TRONG KHI tiền cũng đã hoàn cho người dùng — ví đang giữ
 * một khoản họ đã được trả lại. Ba tính chất, theo thứ tự quan trọng:
 * <ol>
 *   <li><b>Ledger từ chối đảo thì ca PHẢI ở lại hàng đợi.</b> Ví có thể đã tiêu mất tiền, ledger từ
 *       chối là ĐÚNG, và người trực phải thấy ca còn đó — đóng ca ở đây là xoá một khoản phải thu
 *       khỏi tầm nhìn của mọi người.</li>
 *   <li><b>Không đảo khi chưa chắc khách đã được trả lại</b>: không hỏi được ledger/bank, hoặc lần
 *       hoàn ở provider chưa {@code SUCCEEDED} ⇒ từ chối. Đảo lúc đó là rút tiền khỏi ví trong khi
 *       khách chưa nhận lại đồng nào.</li>
 *   <li><b>Lệnh đảo đúng hình dạng và đúng MỘT lần</b>: hai vế đối xứng với bút toán gốc, trỏ đúng
 *       bút toán người đề xuất đã xem, không gửi lặp.</li>
 * </ol>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eureka.client.enabled=false",
                "spring.cloud.config.enabled=false",
                "outbox.relay.delay-ms=3600000",
                "spring.kafka.listener.auto-startup=false",
                "walletflow.kafka.declare-topics=false",
                "walletflow.transaction.saga-scan.enabled=false",
                "management.tracing.enabled=false"
        })
@Import({FakeWalletDirectoryConfig.class, FakeCrossContextConfig.class})
@Testcontainers
class AdminReversalIT {

    private static final UUID TENANT = UUID.randomUUID();
    private static final UUID OTHER_TENANT = UUID.randomUUID();
    private static final long AMOUNT = 500_000;
    private static final String CONFLICT = "REFUND_POSTED_CONFLICT";

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
    TestRestTemplate http;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    FakeWalletDirectory wallets;
    @Autowired
    FakeLedgerEntriesClient ledger;
    @Autowired
    FakeProviderContextClient bank;
    @Autowired
    SagaService sagaService;
    @Autowired
    EnvelopeReader envelopeReader;
    @Autowired
    MeterRegistry meterRegistry;

    private UUID ops;
    private UUID admin;
    private UUID otherAdmin;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE resolution_requests, saga_state, transactions, "
                + "idempotency_keys, outbox_event, audit_events CASCADE");
        wallets.reset();
        ledger.reset();
        bank.reset();
        ops = UUID.randomUUID();
        admin = UUID.randomUUID();
        otherAdmin = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Đường chính
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đủ điều kiện + 4-eyes: ĐÚNG MỘT lệnh REVERSAL, hai vế đảo dấu, ca vẫn mở ở REVERSAL_SENT")
    void duyet_du_dieu_kien_gui_dung_mot_lenh_dao_hai_ve_doi_dau() {
        UUID txn = conflictCase();
        UUID entryId = ledgerHasTopup(txn);
        bankHasRefund(txn, "SUCCEEDED");
        UUID wallet = toWallet(txn);

        UUID requestId = propose(txn, ops, "OPS");
        ResponseEntity<JsonNode> decided = decide(requestId, admin);

        assertThat(decided.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<PostJournalCommand> commands = journalCommands(txn);
        assertThat(commands)
                .as("lệnh đảo gửi hai lần là hai lần rút tiền khỏi ví — chỉ ledger.uk_journal_one_reversal "
                        + "đỡ được, mà ta không muốn dựa vào chốt chặn cuối cho việc tự gây ra")
                .hasSize(1);
        PostJournalCommand command = commands.get(0);
        assertThat(command.type()).isEqualTo(EJournalType.REVERSAL);
        assertThat(command.transactionId()).isEqualTo(txn);
        assertThat(command.referenceId())
                .as("referenceId là sợi dây duy nhất nối bút toán đảo với bút toán gốc")
                .isEqualTo(entryId);
        assertThat(command.currency()).isEqualTo(ECurrency.VND);
        assertThat(command.legs()).containsExactlyInAnyOrder(
                new Leg(wallet.toString(), -AMOUNT),
                new Leg("EXTERNAL_BANK", AMOUNT));
        assertThat(command.legs().stream().mapToLong(Leg::amountMinor).sum())
                .as("bút toán không cân là ledger từ chối, và là dấu hiệu số tiền đang lấy từ chỗ sai")
                .isZero();

        Map<String, Object> row = txnRow(txn);
        assertThat(row.get("status"))
                .as("mới GỬI lệnh, chưa có câu trả lời của ledger — chưa được đóng ca")
                .isEqualTo("MANUAL_REVIEW");
        assertThat(row.get("failure_code")).isEqualTo("REVERSAL_SENT");
        assertThat(row.get("resolved_at"))
                .as("⚠ resolved_at ở đây sẽ làm hàng rào alreadyResolved nuốt luôn lời từ chối của ledger")
                .isNull();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM resolution_requests WHERE id = ?", String.class, requestId))
                .isEqualTo("APPROVED");
    }

    @Test
    @DisplayName("⭐ LedgerPosted(REVERSAL) về: ca đóng FAILED + LEDGER_REVERSED + resolved_at, phát TransactionFailed")
    void ledger_da_dao_thi_ca_dong_failed_ledger_reversed() {
        UUID txn = approvedReversal();

        TenantContext.runWith(TENANT, () -> sagaService.onPosted(reversalPosted(txn)));

        Map<String, Object> row = txnRow(txn);
        assertThat(row.get("status")).isEqualTo("FAILED");
        assertThat(row.get("failure_code")).isEqualTo("LEDGER_REVERSED");
        assertThat(row.get("resolved_at"))
                .as("dấu bền chặn mọi ghi tiếp của saga lên ca đã chốt").isNotNull();
        assertThat(sagaStepStatus(txn)).isEqualTo("FAILED");
        List<TransactionFailedEvent> failed = failedEvents(txn);
        assertThat(failed)
                .as("ca MANUAL_REVIEW chưa từng phát kết cục; không có event này thì đối soát Phase 3 "
                        + "thấy một giao dịch không có kết thúc")
                .hasSize(1);
        assertThat(failed.get(0).failureCode()).isEqualTo("LEDGER_REVERSED");
        assertThat(failed.get(0).amountMinor()).isEqualTo(AMOUNT);
    }

    @Test
    @DisplayName("LedgerPosted(REVERSAL) về LẶP (Kafka at-least-once): vẫn đúng một TransactionFailed, ca không đổi")
    void ledger_posted_dao_lap_khong_phat_them_event() {
        UUID txn = approvedReversal();
        TenantContext.runWith(TENANT, () -> sagaService.onPosted(reversalPosted(txn)));

        TenantContext.runWith(TENANT, () -> sagaService.onPosted(reversalPosted(txn)));

        assertThat(failedEvents(txn)).hasSize(1);
        assertThat(txnRow(txn).get("failure_code")).isEqualTo("LEDGER_REVERSED");
    }

    @Test
    @DisplayName("⭐ LedgerRejected(REVERSAL) LẶP: counter chỉ tăng MỘT lần — alert mức page không được kêu hai lần cho một sự cố")
    void ledger_rejected_dao_lap_khong_dem_hai_lan() {
        UUID txn = approvedReversal();
        double before = counter("walletflow.topup.reversal_rejected");

        TenantContext.runWith(TENANT, () -> sagaService.onRejected(reversalRejected(txn, "INSUFFICIENT_FUNDS")));
        // Bản giao lại: outbox at-least-once, consumer rebalance, hay một DLT vừa được giải phóng.
        // LedgerResultListener cố ý không có bảng dedup — máy trạng thái phải tự chịu được event lặp.
        TenantContext.runWith(TENANT, () -> sagaService.onRejected(reversalRejected(txn, "INSUFFICIENT_FUNDS")));

        assertThat(counter("walletflow.topup.reversal_rejected") - before).isEqualTo(1);
        assertThat(txnRow(txn).get("failure_code")).isEqualTo("REVERSAL_REJECTED");
    }

    @Test
    @DisplayName("⭐ Lời từ chối đảo tới SAU khi ledger đã đảo xong: KHÔNG kéo ca đã đóng ngược về hàng đợi")
    void loi_tu_choi_dao_toi_sau_khi_da_dao_xong_khong_mo_lai_ca() {
        UUID txn = approvedReversal();
        TenantContext.runWith(TENANT, () -> sagaService.onPosted(reversalPosted(txn)));
        double before = counter("walletflow.topup.reversal_rejected");

        // posted và rejected đi trên HAI topic, không có thứ tự giữa chúng. Nếu lời từ chối cũ ghi đè
        // được, ca sẽ vừa có resolved_at vừa ở MANUAL_REVIEW: hiện trong hàng đợi nhưng `lockOpenCase`
        // từ chối mọi thao tác — kẹt vĩnh viễn, chỉ gỡ được bằng psql. Và nó nói SAI về tiền: ví đã
        // bị đảo rồi mà ca báo "vẫn giữ khoản cộng nhầm".
        TenantContext.runWith(TENANT, () -> sagaService.onRejected(reversalRejected(txn, "INSUFFICIENT_FUNDS")));

        Map<String, Object> row = txnRow(txn);
        assertThat(row.get("status")).isEqualTo("FAILED");
        assertThat(row.get("failure_code")).isEqualTo("LEDGER_REVERSED");
        assertThat(row.get("resolved_at")).isNotNull();
        assertThat(counter("walletflow.topup.reversal_rejected") - before)
                .as("không có sự cố mới nào — đừng gọi người dậy")
                .isZero();
    }

    @Test
    @DisplayName("⭐ LedgerRejected(REVERSAL, INSUFFICIENT_FUNDS): ca Ở LẠI MANUAL_REVIEW, REVERSAL_REJECTED, counter +1")
    void ledger_tu_choi_dao_thi_ca_o_lai_hang_doi() {
        UUID txn = approvedReversal();
        double before = counter("walletflow.topup.reversal_rejected");

        TenantContext.runWith(TENANT, () -> sagaService.onRejected(reversalRejected(txn, "INSUFFICIENT_FUNDS")));

        Map<String, Object> row = txnRow(txn);
        assertThat(row.get("status"))
                .as("⚠ ví đã tiêu mất tiền, ledger từ chối là ĐÚNG — đóng ca ở đây là xoá một khoản phải "
                        + "thu khỏi tầm nhìn của người trực")
                .isEqualTo("MANUAL_REVIEW");
        assertThat(row.get("failure_code")).isEqualTo("REVERSAL_REJECTED");
        assertThat((String) row.get("failure_reason")).contains("INSUFFICIENT_FUNDS");
        assertThat(row.get("resolved_at"))
                .as("có resolved_at thì ca biến khỏi hàng đợi và mọi câu trả lời sau của ledger bị chặn")
                .isNull();
        assertThat(counter("walletflow.topup.reversal_rejected") - before)
                .as("tín hiệu để alert: một quyết định có 4-eyes vừa không thực hiện được")
                .isEqualTo(1);
        assertThat(failedEvents(txn))
                .as("ca CHƯA kết thúc nên không được phát TransactionFailed").isEmpty();
        assertThat(sagaStepStatus(txn)).isEqualTo("MANUAL_REVIEW");
    }

    @Test
    @DisplayName("Ca REVERSAL_REJECTED vẫn hiện trong hàng đợi cần can thiệp của người trực")
    void ca_bi_tu_choi_dao_van_hien_trong_hang_doi() {
        UUID txn = approvedReversal();
        TenantContext.runWith(TENANT, () -> sagaService.onRejected(reversalRejected(txn, "INSUFFICIENT_FUNDS")));

        ResponseEntity<JsonNode> queue = http.exchange("/api/v1/admin/transactions/manual-review",
                HttpMethod.GET, new HttpEntity<>(identity(ops, "OPS")), JsonNode.class);

        assertThat(queue.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<String> ids = new java.util.ArrayList<>();
        queue.getBody().path("data").forEach(item -> ids.add(item.path("transactionId").asText()));
        assertThat(ids).contains(txn.toString());
    }

    @Test
    @DisplayName("Chốt bút toán lúc ĐỀ XUẤT: ledger có thêm bút toán TOPUP khác trước lúc duyệt, lệnh vẫn trỏ bút toán đã xem")
    void lenh_dao_tro_dung_but_toan_da_chot_luc_de_xuat() {
        UUID txn = conflictCase();
        UUID entryNguoiDeXuatDaXem = ledgerHasTopup(txn);
        bankHasRefund(txn, "SUCCEEDED");
        UUID requestId = propose(txn, ops, "OPS");
        // Ledger ghi thêm một bút toán TOPUP đứng đầu danh sách giữa hai bước.
        UUID entryMoi = UUID.randomUUID();
        ledger.put(TENANT, new TransactionEntries(txn, List.of(
                topupEntry(entryMoi, txn), topupEntry(entryNguoiDeXuatDaXem, txn))));

        decide(requestId, admin);

        assertThat(journalCommands(txn)).singleElement().satisfies(command ->
                assertThat(command.referenceId())
                        .as("4-eyes: người duyệt ký đúng thứ người đề xuất đã xem, không phải thứ tra lại")
                        .isEqualTo(entryNguoiDeXuatDaXem));
    }

    // ------------------------------------------------------------------
    // Hàng rào lúc đề xuất
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Không hỏi được ledger: 503 CONTEXT_UNAVAILABLE, KHÔNG tạo yêu cầu nào")
    void ledger_chet_thi_khong_cho_de_xuat() {
        UUID txn = conflictCase();
        ledgerHasTopup(txn);
        bankHasRefund(txn, "SUCCEEDED");
        ledger.goDown();

        ResponseEntity<JsonNode> response = proposeRaw(txn, ops, "OPS");

        assertRejected(response, HttpStatus.SERVICE_UNAVAILABLE, "CONTEXT_UNAVAILABLE");
        assertThat(requestCount())
                .as("'ledger không trả lời nên chắc là chưa có gì' là cách tạo ra một mất mát thứ hai")
                .isZero();
    }

    @Test
    @DisplayName("⭐ Không hỏi được bank-adapter: 503 CONTEXT_UNAVAILABLE, KHÔNG tạo yêu cầu nào")
    void bank_chet_thi_khong_cho_de_xuat() {
        UUID txn = conflictCase();
        ledgerHasTopup(txn);
        bankHasRefund(txn, "SUCCEEDED");
        bank.goDown();

        ResponseEntity<JsonNode> response = proposeRaw(txn, ops, "OPS");

        assertRejected(response, HttpStatus.SERVICE_UNAVAILABLE, "CONTEXT_UNAVAILABLE");
        assertThat(requestCount())
                .as("không biết khách đã được hoàn chưa thì không được rút tiền khỏi ví").isZero();
    }

    @Test
    @DisplayName("⭐ Lần hoàn ở provider chưa SUCCEEDED (không có / PENDING / FAILED): 409 REFUND_NOT_CONFIRMED")
    void hoan_chua_thanh_cong_thi_khong_cho_dao() {
        // Một lần THU thành công không được tính là lần hoàn — dễ nhầm vì cả hai đều là thao tác SUCCEEDED.
        List<List<ProviderOperationView>> cacTinhHuong = List.of(
                List.of(operation(EBankOperation.CHARGE, "SUCCEEDED")),
                List.of(operation(EBankOperation.CHARGE, "SUCCEEDED"), operation(EBankOperation.REFUND, "PENDING")),
                List.of(operation(EBankOperation.CHARGE, "SUCCEEDED"), operation(EBankOperation.REFUND, "FAILED")));

        for (List<ProviderOperationView> operations : cacTinhHuong) {
            UUID txn = conflictCase();
            ledgerHasTopup(txn);
            bank.put(TENANT, new ProviderContext(txn, operations));

            ResponseEntity<JsonNode> response = proposeRaw(txn, ops, "OPS");

            assertRejected(response, HttpStatus.CONFLICT, "REFUND_NOT_CONFIRMED");
        }
        assertThat(requestCount())
                .as("đảo lúc này là rút tiền khỏi ví trong khi khách chưa được trả lại — mất tiền của KHÁCH")
                .isZero();
    }

    @Test
    @DisplayName("Đã có bút toán REVERSAL trong ledger: 409 ALREADY_REVERSED, không để hai người ký rồi mới nổ")
    void da_dao_roi_thi_khong_cho_de_xuat() {
        UUID txn = conflictCase();
        UUID entryId = UUID.randomUUID();
        ledger.put(TENANT, new TransactionEntries(txn, List.of(
                topupEntry(entryId, txn),
                new JournalEntryView(UUID.randomUUID(), EJournalType.REVERSAL, entryId, Instant.now(), List.of(
                        new EntryLine(UUID.randomUUID(), "WALLET-" + txn, -AMOUNT),
                        new EntryLine(UUID.randomUUID(), "EXTERNAL_BANK", AMOUNT))))));
        bankHasRefund(txn, "SUCCEEDED");

        ResponseEntity<JsonNode> response = proposeRaw(txn, ops, "OPS");

        assertRejected(response, HttpStatus.CONFLICT, "ALREADY_REVERSED");
        assertThat(requestCount()).isZero();
    }

    @Test
    @DisplayName("⭐ Ledger trả lời 'chưa có bút toán TOPUP nào': 409 NO_ENTRY_TO_REVERSE — KHÁC 'không hỏi được'")
    void ledger_chua_ghi_gi_thi_khong_co_gi_de_dao() {
        UUID txn = conflictCase();
        // Không put gì: ledger trả lời "chưa có gì". Rỗng ≠ không hỏi được (503) — hai điều dẫn tới
        // hai quyết định khác nhau, nên mã lỗi phải khác nhau.
        bankHasRefund(txn, "SUCCEEDED");

        ResponseEntity<JsonNode> response = proposeRaw(txn, ops, "OPS");

        assertRejected(response, HttpStatus.CONFLICT, "NO_ENTRY_TO_REVERSE");
        assertThat(requestCount()).isZero();
    }

    // ------------------------------------------------------------------
    // Hàng rào lúc duyệt
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Ca có failure_code khác REFUND_POSTED_CONFLICT (ACCOUNT_CLOSED): 409 NOT_A_POSTED_CONFLICT ngay lúc ĐỀ XUẤT")
    void ca_khong_phai_posted_conflict_thi_khong_duoc_dao() {
        UUID txn = conflictCase("ACCOUNT_CLOSED");
        ledgerHasTopup(txn);
        bankHasRefund(txn, "SUCCEEDED");

        ResponseEntity<JsonNode> response = http.exchange(
                "/api/v1/admin/transactions/" + txn + "/resolve", HttpMethod.POST,
                new HttpEntity<>(Map.of("action", "REVERSE_LEDGER_ENTRY", "reason", "Thử đảo"),
                        identity(ops, "OPS")), JsonNode.class);

        // Chặn ở ĐỀ XUẤT, không phải ở duyệt: để một yêu cầu chắc chắn bị từ chối nằm chờ chỉ tổ
        // khiến hai người ký vào nó rồi mới biết. Hàng rào dưới khoá ở `reverseLedgerEntry` vẫn còn
        // cho ca saga đổi trạng thái giữa đề xuất và duyệt.
        assertRejected(response, HttpStatus.CONFLICT, "NOT_A_POSTED_CONFLICT");
        assertThat(journalCommands(txn))
                .as("đảo 'cho chắc' ở ca ledger đã từ chối là rút tiền khỏi ví mà không có bút toán gốc")
                .isEmpty();
        assertThat(txnRow(txn).get("failure_code")).isEqualTo("ACCOUNT_CLOSED");
        assertThat(requestCount()).as("không tạo yêu cầu nào để người khác phải duyệt").isZero();
    }

    @Test
    @DisplayName("Ca có type TRANSFER không đảo được dù failure_code trùng: 409 NOT_A_POSTED_CONFLICT")
    void chuyen_tien_khong_duoc_dao_bang_duong_nap() {
        UUID txn = conflictCase();
        ledgerHasTopup(txn);
        bankHasRefund(txn, "SUCCEEDED");
        UUID requestId = propose(txn, ops, "OPS");
        UUID nguon = wallets.add(TENANT, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
        jdbcTemplate.update("UPDATE transactions SET type = 'TRANSFER', from_wallet = ? WHERE id = ?", nguon, txn);

        ResponseEntity<JsonNode> decided = decide(requestId, admin);

        assertRejected(decided, HttpStatus.CONFLICT, "NOT_A_POSTED_CONFLICT");
        assertThat(journalCommands(txn)).isEmpty();
    }

    @Test
    @DisplayName("⭐ Đề xuất đảo LẦN HAI sau khi đã REVERSAL_SENT: bị từ chối, outbox vẫn chỉ MỘT lệnh")
    void de_xuat_dao_lan_hai_khong_sinh_lenh_thu_hai() {
        UUID txn = approvedReversal();
        assertThat(journalCommands(txn)).as("tiền đề: đã gửi một lệnh").hasSize(1);

        // Hàng rào chặn lần hai có thể đứng ở đề xuất hoặc ở duyệt — cả hai đều đáp ứng bất biến "tiền
        // chỉ đảo một lần". Hiện tại nó đứng ở lúc duyệt (failure_code đã là REVERSAL_SENT).
        ResponseEntity<JsonNode> second = proposeRaw(txn, ops, "OPS");
        ResponseEntity<JsonNode> rejected = second;
        if (second.getStatusCode() == HttpStatus.ACCEPTED) {
            UUID secondRequest = UUID.fromString(
                    second.getBody().path("data").path("resolutionRequestId").asText());
            rejected = decide(secondRequest, otherAdmin);
        }

        assertThat(rejected.getStatusCode().is4xxClientError()).isTrue();
        assertThat(journalCommands(txn))
                .as("⚠ hai lệnh đảo là hai lần rút tiền khỏi ví người dùng")
                .hasSize(1);
        assertThat(txnRow(txn).get("failure_code")).isEqualTo("REVERSAL_SENT");
    }

    @Test
    @DisplayName("⭐ 4-eyes: người đề xuất TỰ DUYỆT lệnh đảo → 403, không có lệnh nào vào outbox")
    void tu_duyet_lenh_dao_bi_tu_choi() {
        UUID txn = conflictCase();
        ledgerHasTopup(txn);
        bankHasRefund(txn, "SUCCEEDED");
        UUID requestId = propose(txn, admin, "ADMIN");

        ResponseEntity<JsonNode> response = decide(requestId, admin);

        assertRejected(response, HttpStatus.FORBIDDEN, "FOUR_EYES_VIOLATION");
        assertThat(journalCommands(txn)).isEmpty();
        assertThat(txnRow(txn).get("failure_code")).isEqualTo(CONFLICT);
        assertThat(txnRow(txn).get("resolved_at")).isNull();
    }

    // ------------------------------------------------------------------
    // Dữ liệu và DB
    // ------------------------------------------------------------------
    @Test
    @DisplayName("target_entry_id được chốt đúng bút toán TOPUP vào resolution_requests lúc đề xuất")
    void target_entry_id_duoc_ghi_dung() {
        UUID txn = conflictCase();
        UUID entryId = ledgerHasTopup(txn);
        bankHasRefund(txn, "SUCCEEDED");

        UUID requestId = propose(txn, ops, "OPS");

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT action, status, target_entry_id FROM resolution_requests WHERE id = ?", requestId);
        assertThat(row.get("action")).isEqualTo("REVERSE_LEDGER_ENTRY");
        assertThat(row.get("status")).isEqualTo("PENDING");
        assertThat(row.get("target_entry_id")).isEqualTo(entryId);
    }

    @Test
    @DisplayName("⭐ DB cưỡng chế ck_resolution_target_entry: không ghi target_entry_id cho MARK_REFUNDED_MANUALLY, và ngược lại")
    void db_chan_target_entry_sai_thao_tac() {
        UUID txn = conflictCase();

        assertThatThrownBy(() -> insertRequest(txn, "MARK_REFUNDED_MANUALLY", UUID.randomUUID()))
                .as("id bút toán gắn vào yêu cầu 'đã hoàn tay' là vô nghĩa, và về sau thành căn cứ đối soát sai")
                .hasMessageContaining("ck_resolution_target_entry");
        assertThatThrownBy(() -> insertRequest(txn, "REVERSE_LEDGER_ENTRY", null))
                .as("lệnh đảo không có bút toán đích thì không biết đảo cái gì")
                .hasMessageContaining("ck_resolution_target_entry");
        assertThat(requestCount()).isZero();

        // Đối chứng: hai tổ hợp ĐÚNG phải qua được — không thì hai assert trên đỏ vì lý do khác.
        insertRequest(txn, "REVERSE_LEDGER_ENTRY", UUID.randomUUID());
        assertThat(requestCount()).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Không đề xuất đảo được cho ca của tenant khác — cùng một lỗi với ca không tồn tại")
    void khong_de_xuat_dao_duoc_cho_tenant_khac() {
        UUID cuaHo = conflictCaseIn(OTHER_TENANT, CONFLICT);
        ledger.put(OTHER_TENANT, new TransactionEntries(cuaHo, List.of(topupEntry(UUID.randomUUID(), cuaHo))));
        bank.put(OTHER_TENANT, new ProviderContext(cuaHo, List.of(operation(EBankOperation.REFUND, "SUCCEEDED"))));

        ResponseEntity<JsonNode> response = proposeRaw(cuaHo, ops, "OPS");
        ResponseEntity<JsonNode> khongTonTai = proposeRaw(UUID.randomUUID(), ops, "OPS");

        assertThat(response.getStatusCode().is4xxClientError()).isTrue();
        assertThat(response.getStatusCode())
                .as("khác nhau giữa 'của người khác' và 'không tồn tại' là một máy dò id")
                .isEqualTo(khongTonTai.getStatusCode());
        assertThat(response.getBody().path("errorCode").asText())
                .isEqualTo(khongTonTai.getBody().path("errorCode").asText());
        assertThat(requestCount()).isZero();
    }

    @Test
    @DisplayName("⭐ Dù ledger/bank trả lời 'đủ điều kiện', ca của tenant khác vẫn 404 và không có yêu cầu")
    void ca_cua_tenant_khac_van_404_du_ngu_canh_ngoai_lot_qua() {
        UUID cuaHo = conflictCaseIn(OTHER_TENANT, CONFLICT);
        // Mô phỏng một service ngoài bỏ sót lọc tenant: nó trả bút toán và lần hoàn của ca này cho
        // người gọi thuộc TENANT. Lớp lọc tenant của chính transaction-service phải đứng một mình.
        ledger.put(TENANT, new TransactionEntries(cuaHo, List.of(topupEntry(UUID.randomUUID(), cuaHo))));
        bank.put(TENANT, new ProviderContext(cuaHo, List.of(operation(EBankOperation.REFUND, "SUCCEEDED"))));

        ResponseEntity<JsonNode> response = proposeRaw(cuaHo, ops, "OPS");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(requestCount()).isZero();
        assertThat(journalCommandsAll()).isEmpty();
    }

    // ------------------------------------------------------------------
    // Fixture
    // ------------------------------------------------------------------

    /** Ca đã đề xuất + duyệt đủ hai người: lệnh đảo đã vào outbox, ca ở REVERSAL_SENT. */
    private UUID approvedReversal() {
        UUID txn = conflictCase();
        ledgerHasTopup(txn);
        bankHasRefund(txn, "SUCCEEDED");
        ResponseEntity<JsonNode> decided = decide(propose(txn, ops, "OPS"), admin);
        assertThat(decided.getStatusCode()).as("tiền đề: duyệt thành công").isEqualTo(HttpStatus.OK);
        return txn;
    }

    private UUID conflictCase() {
        return conflictCase(CONFLICT);
    }

    private UUID conflictCase(String failureCode) {
        return conflictCaseIn(TENANT, failureCode);
    }

    /** Ca nạp tiền ở MANUAL_REVIEW, saga đứng ở BANK_REFUND — hình dạng thật của mọi ca REFUND_*. */
    private UUID conflictCaseIn(UUID tenant, String failureCode) {
        UUID id = UUID.randomUUID();
        UUID wallet = wallets.add(tenant, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
        jdbcTemplate.update("""
                INSERT INTO transactions (id, tenant_id, type, status, initiated_by, to_wallet,
                                          amount_minor, currency, failure_code, failure_reason,
                                          provider, provider_ref, created_at, updated_at)
                VALUES (?, ?, 'TOPUP', 'MANUAL_REVIEW', ?, ?, ?, 'VND', ?, ?, 'mock', ?, now(), now())
                """, id, tenant, UUID.randomUUID(), wallet, AMOUNT, failureCode,
                "Nguyên nhân: " + failureCode, "MOCK-" + id);
        jdbcTemplate.update("""
                INSERT INTO saga_state (transaction_id, tenant_id, current_step, step_status,
                                        attempt_count, command_event_id, deadline_at, updated_at)
                VALUES (?, ?, 'BANK_REFUND', 'MANUAL_REVIEW', 1, ?, now(), now())
                """, id, tenant, UUID.randomUUID());
        return id;
    }

    private UUID ledgerHasTopup(UUID txn) {
        UUID entryId = UUID.randomUUID();
        ledger.put(TENANT, new TransactionEntries(txn, List.of(topupEntry(entryId, txn))));
        return entryId;
    }

    private static JournalEntryView topupEntry(UUID entryId, UUID txn) {
        return new JournalEntryView(entryId, EJournalType.TOPUP, null, Instant.now(), List.of(
                new EntryLine(UUID.randomUUID(), "EXTERNAL_BANK", -AMOUNT),
                new EntryLine(UUID.randomUUID(), "WALLET-" + txn, AMOUNT)));
    }

    private void bankHasRefund(UUID txn, String refundStatus) {
        bank.put(TENANT, new ProviderContext(txn, List.of(
                operation(EBankOperation.CHARGE, "SUCCEEDED"),
                operation(EBankOperation.REFUND, refundStatus))));
    }

    private static ProviderOperationView operation(EBankOperation operation, String status) {
        return new ProviderOperationView(UUID.randomUUID(), operation, "mock", "MOCK-" + UUID.randomUUID(),
                AMOUNT, ECurrency.VND, status, null, Instant.now(), Instant.now(), null, "{\"code\":\"00\"}");
    }

    private void insertRequest(UUID txn, String action, UUID targetEntryId) {
        jdbcTemplate.update("""
                INSERT INTO resolution_requests (id, tenant_id, transaction_id, action, status, maker_id,
                                                 maker_role, reason, target_entry_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'PENDING', ?, 'OPS', 'thử', ?, now(), now())
                """, UUID.randomUUID(), TENANT, txn, action, UUID.randomUUID(), targetEntryId);
    }

    /** Câu trả lời của ledger cho lệnh đảo: hai vế đảo dấu với bút toán nạp. */
    private LedgerPostedEvent reversalPosted(UUID txn) {
        return new LedgerPostedEvent(txn, UUID.randomUUID(), EJournalType.REVERSAL, ECurrency.VND,
                List.of(new Leg(toWallet(txn).toString(), -AMOUNT), new Leg("EXTERNAL_BANK", AMOUNT)));
    }

    private static LedgerRejectedEvent reversalRejected(UUID txn, String code) {
        return new LedgerRejectedEvent(txn, EJournalType.REVERSAL, code,
                "Insufficient funds. Balance = 12, required = " + AMOUNT);
    }

    // ------------------------------------------------------------------
    // HTTP
    // ------------------------------------------------------------------
    private UUID propose(UUID txn, UUID actor, String role) {
        ResponseEntity<JsonNode> response = proposeRaw(txn, actor, role);
        assertThat(response.getStatusCode()).as("tiền đề: đề xuất được nhận").isEqualTo(HttpStatus.ACCEPTED);
        return UUID.fromString(response.getBody().path("data").path("resolutionRequestId").asText());
    }

    private ResponseEntity<JsonNode> proposeRaw(UUID txn, UUID actor, String role) {
        return http.exchange("/api/v1/admin/transactions/" + txn + "/resolve", HttpMethod.POST,
                new HttpEntity<>(Map.of("action", "REVERSE_LEDGER_ENTRY",
                        "reason", "Tiền đã hoàn qua cổng thanh toán, ví đang giữ khoản nạp thừa"),
                        identity(actor, role)), JsonNode.class);
    }

    private ResponseEntity<JsonNode> decide(UUID requestId, UUID actor) {
        return http.exchange("/api/v1/admin/transactions/resolutions/" + requestId + "/decision",
                HttpMethod.POST, new HttpEntity<>(Map.of("approve", true), identity(actor, "ADMIN")),
                JsonNode.class);
    }

    private static void assertRejected(ResponseEntity<JsonNode> response, HttpStatus status, String errorCode) {
        assertThat(response.getStatusCode()).isEqualTo(status);
        assertThat(response.getBody().path("errorCode").asText()).isEqualTo(errorCode);
    }

    private static HttpHeaders identity(UUID actor, String role) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(AuthHeaders.TENANT_ID, TENANT.toString());
        headers.set(AuthHeaders.USER_ID, actor.toString());
        headers.set(AuthHeaders.USER_EMAIL, role.toLowerCase() + "@walletflow.dev");
        headers.set(AuthHeaders.USER_ROLE, role);
        return headers;
    }

    // ------------------------------------------------------------------
    // Đọc trạng thái
    // ------------------------------------------------------------------
    private Map<String, Object> txnRow(UUID txn) {
        return jdbcTemplate.queryForMap("SELECT * FROM transactions WHERE id = ?", txn);
    }

    private UUID toWallet(UUID txn) {
        return (UUID) txnRow(txn).get("to_wallet");
    }

    private String sagaStepStatus(UUID txn) {
        return jdbcTemplate.queryForObject(
                "SELECT step_status FROM saga_state WHERE transaction_id = ?", String.class, txn);
    }

    private int requestCount() {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM resolution_requests", Integer.class);
    }

    private List<PostJournalCommand> journalCommands(UUID txn) {
        return jdbcTemplate.queryForList(
                        "SELECT payload FROM outbox_event WHERE topic = ? AND payload LIKE ? ORDER BY created_at",
                        String.class, WalletTopics.CMD_POST_JOURNAL, "%" + txn + "%").stream()
                .map(json -> envelopeReader.read(json, PostJournalCommand.class).payload()).toList();
    }

    private List<String> journalCommandsAll() {
        return jdbcTemplate.queryForList("SELECT payload FROM outbox_event WHERE topic = ?",
                String.class, WalletTopics.CMD_POST_JOURNAL);
    }

    private List<TransactionFailedEvent> failedEvents(UUID txn) {
        return jdbcTemplate.queryForList(
                        "SELECT payload FROM outbox_event WHERE topic = ? AND payload LIKE ?",
                        String.class, WalletTopics.EVT_TXN_FAILED, "%" + txn + "%").stream()
                .map(json -> envelopeReader.read(json, TransactionFailedEvent.class).payload()).toList();
    }

    /** 0 khi counter chưa từng được đăng ký — khác với "đã đăng ký và đang ở 0". */
    private double counter(String name) {
        Counter counter = meterRegistry.find(name).counter();
        return counter == null ? 0 : counter.count();
    }
}
