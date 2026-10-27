package com.walletflow.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.bank.ProviderContext;
import com.walletflow.common.bank.ProviderOperationView;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.ledger.EntryLine;
import com.walletflow.common.ledger.JournalEntryView;
import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.transaction.support.FakeCrossContextConfig;
import com.walletflow.transaction.support.FakeLedgerEntriesClient;
import com.walletflow.transaction.support.FakeProviderContextClient;
import com.walletflow.transaction.support.FakeWalletDirectory;
import com.walletflow.transaction.support.FakeWalletDirectoryConfig;
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
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chi tiết một ca kèm ngữ cảnh từ ledger và bank-adapter (roadmap 2.5 PR3, ADR-021).
 *
 * <p>Tính chất cốt lõi: <b>"service trả lời, chưa có gì" KHÁC "không hỏi được service"</b>. Một ô
 * trống vừa có thể nghĩa "ledger chưa ghi gì" (an toàn để hoàn tiền cho khách) vừa có thể nghĩa
 * "mạng hỏng, không biết ledger đã ghi chưa" (hoàn tiền có thể là chi hai lần). Hai cách đọc dẫn tới
 * hai quyết định ngược nhau về tiền, nên response phải phân biệt được bằng {@code null} so với rỗng
 * và bằng danh sách {@code unavailable}.
 *
 * <p>Thứ hai: một ô phụ chết không được làm hỏng màn hình chữa cháy — phần còn lại vẫn phải đầy đủ.
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
class AdminCrossContextIT {

    /** Hai tenant ngẫu nhiên, không dùng DEFAULT_TENANT — lý do ở {@code AdminReviewIT}. */
    private static final UUID TENANT = UUID.randomUUID();
    private static final UUID OTHER_TENANT = UUID.randomUUID();

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
    ObjectMapper objectMapper;

    private UUID operator;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE resolution_requests, saga_state, transactions, idempotency_keys, outbox_event, audit_events");
        wallets.reset();
        ledger.reset();
        bank.reset();
        operator = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Đủ nguồn
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Ledger có bút toán và bank có CHARGE: response chứa đủ cả hai, unavailable RỖNG")
    void du_ca_hai_nguon_thi_khong_co_gi_unavailable() {
        UUID txn = needsReview(TENANT);
        ledger.put(TENANT, entriesWithOneJournal(txn, 500_000));
        bank.put(TENANT, contextWithOneCharge(txn, 500_000));

        JsonNode data = ok(view(txn, TENANT)).path("data");

        assertThat(data.path("unavailable")).as("đủ cả hai nguồn thì không có gì để báo thiếu").isEmpty();
        JsonNode journal = data.path("ledger").path("entries").get(0);
        assertThat(data.path("ledger").path("entries")).hasSize(1);
        assertThat(journal.path("type").asText()).isEqualTo("TOPUP");
        assertThat(journal.path("lines")).hasSize(2);
        assertThat(journal.path("lines").get(0).path("accountRef").asText()).isEqualTo("EXTERNAL_BANK");
        assertThat(journal.path("lines").get(0).path("amountMinor").asLong()).isEqualTo(-500_000L);
        assertThat(journal.path("lines").get(1).path("amountMinor").asLong()).isEqualTo(500_000L);
        JsonNode charge = data.path("provider").path("operations").get(0);
        assertThat(data.path("provider").path("operations")).hasSize(1);
        assertThat(charge.path("operation").asText()).isEqualTo("CHARGE");
        assertThat(charge.path("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(charge.path("amountMinor").asLong()).isEqualTo(500_000L);
        assertThat(data.path("transaction").path("transactionId").asText()).isEqualTo(txn.toString());
        assertThat(data.path("saga").path("currentStep").asText()).isEqualTo("BANK_CHARGE");
    }

    // ------------------------------------------------------------------
    // Một nguồn chết — phần còn lại vẫn đủ
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Ledger chết: ledger=null, unavailable=[ledger], phần còn lại (transaction, saga, provider) VẪN đầy đủ")
    void ledger_chet_thi_man_hinh_van_dung_duoc() {
        UUID txn = needsReview(TENANT);
        ledger.put(TENANT, entriesWithOneJournal(txn, 500_000));
        bank.put(TENANT, contextWithOneCharge(txn, 500_000));
        ledger.goDown();

        ResponseEntity<JsonNode> response = view(txn, TENANT);

        assertThat(response.getStatusCode())
                .as("một ô phụ chết không được kéo cả màn hình chữa cháy xuống").isEqualTo(HttpStatus.OK);
        JsonNode data = response.getBody().path("data");
        assertThat(data.path("ledger").isNull()).as("null = KHÔNG BIẾT, không phải 'chưa có gì'").isTrue();
        assertThat(texts(data.path("unavailable"))).containsExactly("ledger");
        assertThat(data.path("transaction").path("transactionId").asText()).isEqualTo(txn.toString());
        assertThat(data.path("saga").path("currentStep").asText()).isEqualTo("BANK_CHARGE");
        assertThat(data.path("provider").path("operations")).hasSize(1);
    }

    @Test
    @DisplayName("⭐ Bank chết: provider=null, unavailable=[bank], phần còn lại (transaction, saga, ledger) VẪN đầy đủ")
    void bank_chet_thi_man_hinh_van_dung_duoc() {
        UUID txn = needsReview(TENANT);
        ledger.put(TENANT, entriesWithOneJournal(txn, 500_000));
        bank.put(TENANT, contextWithOneCharge(txn, 500_000));
        bank.goDown();

        ResponseEntity<JsonNode> response = view(txn, TENANT);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode data = response.getBody().path("data");
        assertThat(data.path("provider").isNull()).isTrue();
        assertThat(texts(data.path("unavailable"))).containsExactly("bank");
        assertThat(data.path("transaction").path("transactionId").asText()).isEqualTo(txn.toString());
        assertThat(data.path("saga").path("currentStep").asText()).isEqualTo("BANK_CHARGE");
        assertThat(data.path("ledger").path("entries")).hasSize(1);
    }

    @Test
    @DisplayName("⭐ Cả hai chết: unavailable có cả hai, endpoint vẫn 200 với transaction + saga")
    void ca_hai_chet_van_tra_giao_dich_va_saga() {
        UUID txn = needsReview(TENANT);
        ledger.goDown();
        bank.goDown();

        ResponseEntity<JsonNode> response = view(txn, TENANT);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode data = response.getBody().path("data");
        assertThat(texts(data.path("unavailable"))).containsExactlyInAnyOrder("ledger", "bank");
        assertThat(data.path("ledger").isNull()).isTrue();
        assertThat(data.path("provider").isNull()).isTrue();
        assertThat(data.path("transaction").path("transactionId").asText()).isEqualTo(txn.toString());
        assertThat(data.path("saga").path("currentStep").asText()).isEqualTo("BANK_CHARGE");
    }

    @Test
    @DisplayName("Nguồn chết rồi sống lại: lần xem sau thấy dữ liệu và unavailable rỗng — không dính cache")
    void nguon_song_lai_thi_lan_xem_sau_thay_du_lieu() {
        UUID txn = needsReview(TENANT);
        ledger.put(TENANT, entriesWithOneJournal(txn, 500_000));
        ledger.goDown();
        assertThat(texts(ok(view(txn, TENANT)).path("data").path("unavailable"))).containsExactly("ledger");

        ledger.goUp();

        JsonNode data = ok(view(txn, TENANT)).path("data");
        assertThat(data.path("unavailable")).isEmpty();
        assertThat(data.path("ledger").path("entries")).hasSize(1);
    }

    // ------------------------------------------------------------------
    // Rỗng KHÁC null
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Rỗng KHÁC null: ledger/bank trả lời 'chưa có gì' thì ledger.entries rỗng, KHÔNG null, và unavailable KHÔNG chứa nó")
    void rong_khac_null() {
        UUID txn = needsReview(TENANT);
        // Không put gì: fake trả lời "chưa có gì" như service thật khi chưa ghi sổ

        JsonNode data = ok(view(txn, TENANT)).path("data");

        assertThat(data.path("ledger").isNull())
                .as("⚠ null ở đây nghĩa là 'không biết' — người trực sẽ không dám hoàn tiền một ca "
                        + "mà ledger đã xác nhận CHƯA ghi gì")
                .isFalse();
        assertThat(data.path("ledger").path("entries").isArray()).isTrue();
        assertThat(data.path("ledger").path("entries")).isEmpty();
        assertThat(data.path("provider").isNull()).isFalse();
        assertThat(data.path("provider").path("operations").isArray()).isTrue();
        assertThat(data.path("provider").path("operations")).isEmpty();
        assertThat(data.path("unavailable"))
                .as("⚠ báo 'ledger' không hỏi được trong khi nó đã trả lời là nói dối người trực")
                .isEmpty();
    }

    // ------------------------------------------------------------------
    // Tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Cô lập tenant: dữ liệu put cho tenant A không hiện ra khi người trực tenant B xem ca của chính họ")
    void du_lieu_cua_tenant_khac_khong_hien_ra() {
        UUID caCuaB = needsReview(OTHER_TENANT);
        // Dữ liệu đặt dưới tenant A nhưng khoá theo id ca của B: nếu client/service bỏ tenant ra khỏi
        // khoá tra cứu, B sẽ thấy sổ và lần thu của A.
        ledger.put(TENANT, entriesWithOneJournal(caCuaB, 777_000));
        bank.put(TENANT, contextWithOneCharge(caCuaB, 777_000));

        JsonNode data = ok(view(caCuaB, OTHER_TENANT)).path("data");

        assertThat(data.path("ledger").path("entries"))
                .as("bút toán của tenant khác lọt vào màn hình người trực là lộ sổ của người lạ")
                .isEmpty();
        assertThat(data.path("provider").path("operations")).isEmpty();
        assertThat(data.path("unavailable")).isEmpty();

        // Đối chứng: đặt ĐÚNG tenant thì thấy — không thì test trên xanh vì lý do sai
        ledger.put(OTHER_TENANT, entriesWithOneJournal(caCuaB, 777_000));
        assertThat(ok(view(caCuaB, OTHER_TENANT)).path("data").path("ledger").path("entries")).hasSize(1);
    }

    // ------------------------------------------------------------------
    // Dấu vết
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Audit VIEW_TRANSACTION ghi lại unavailable: ledger chết thì dòng audit nói dữ liệu người trực thấy KHÔNG đầy đủ")
    void audit_ghi_lai_nguon_khong_hoi_duoc() throws Exception {
        UUID txnLedgerChet = needsReview(TENANT);
        UUID txnDayDu = needsReview(TENANT);
        ledger.put(TENANT, entriesWithOneJournal(txnDayDu, 1));

        ledger.goDown();
        ok(view(txnLedgerChet, TENANT));
        ledger.goUp();
        ok(view(txnDayDu, TENANT));

        JsonNode degraded = auditDetail(txnLedgerChet);
        assertThat(degraded.path("unavailable").isArray()).isTrue();
        assertThat(texts(degraded.path("unavailable")))
                .as("người kiểm sổ phải biết lúc đó người trực nhìn dữ liệu thiếu")
                .containsExactly("ledger");
        assertThat(degraded.path("status").asText()).isEqualTo("MANUAL_REVIEW");

        JsonNode full = auditDetail(txnDayDu);
        assertThat(full.path("unavailable").isArray()).isTrue();
        assertThat(full.path("unavailable")).as("đối chứng: đủ nguồn thì audit ghi mảng rỗng").isEmpty();
    }

    // ------------------------------------------------------------------
    // 404 không đi sang service khác
    // ------------------------------------------------------------------
    @Test
    @DisplayName("404 (không tồn tại / của tenant khác) vẫn 404 như cũ và KHÔNG gọi sang ledger hay bank")
    void khong_tim_thay_thi_404_va_khong_goi_service_khac() {
        UUID cuaTenantKhac = needsReview(OTHER_TENANT);
        UUID khongTonTai = UUID.randomUUID();

        ResponseEntity<JsonNode> khac = view(cuaTenantKhac, TENANT);
        ResponseEntity<JsonNode> khong = view(khongTonTai, TENANT);

        assertThat(khac.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(khong.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(khac.getBody().path("errorCode").asText())
                .as("cùng một lỗi cho cả hai ca — khác đi là thành máy dò id")
                .isEqualTo(khong.getBody().path("errorCode").asText());
        assertThat(ledger.calls())
                .as("hỏi ledger về một giao dịch không thuộc tenant là rò id sang service khác").isZero();
        assertThat(bank.calls()).isZero();
    }

    @Test
    @DisplayName("Đối chứng cho ca 404: ca tồn tại thì mỗi service bị hỏi đúng MỘT lần")
    void ca_ton_tai_thi_moi_service_duoc_hoi_mot_lan() {
        UUID txn = needsReview(TENANT);

        ok(view(txn, TENANT));

        assertThat(ledger.calls()).isEqualTo(1);
        assertThat(bank.calls()).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private JsonNode auditDetail(UUID txn) throws Exception {
        String json = jdbcTemplate.queryForObject(
                "SELECT detail::text FROM audit_events WHERE action = 'VIEW_TRANSACTION' AND target = ?",
                String.class, txn.toString());
        return objectMapper.readTree(json);
    }

    private static List<String> texts(JsonNode array) {
        List<String> out = new ArrayList<>();
        array.forEach(n -> out.add(n.asText()));
        return out;
    }

    private static TransactionEntries entriesWithOneJournal(UUID txn, long amount) {
        JournalEntryView entry = new JournalEntryView(UUID.randomUUID(), EJournalType.TOPUP, null,
                Instant.now(), List.of(
                        new EntryLine(UUID.randomUUID(), "EXTERNAL_BANK", -amount),
                        new EntryLine(UUID.randomUUID(), "WALLET-" + txn, amount)));
        return new TransactionEntries(txn, List.of(entry));
    }

    private static ProviderContext contextWithOneCharge(UUID txn, long amount) {
        ProviderOperationView charge = new ProviderOperationView(UUID.randomUUID(), EBankOperation.CHARGE,
                "mock", "MOCK-" + txn, amount, ECurrency.VND, "SUCCEEDED", null,
                Instant.now(), Instant.now(), null, "{\"code\":\"00\"}");
        return new ProviderContext(txn, List.of(charge));
    }

    /** Dựng thẳng bằng SQL, kèm saga — thứ đang kiểm là màn hình chi tiết, không phải đường dẫn tới ca. */
    private UUID needsReview(UUID tenant) {
        UUID id = UUID.randomUUID();
        UUID wallet = wallets.add(tenant, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
        jdbcTemplate.update("""
                INSERT INTO transactions (id, tenant_id, type, status, initiated_by, to_wallet, amount_minor,
                                          currency, failure_code, failure_reason, provider, provider_ref,
                                          created_at, updated_at)
                VALUES (?, ?, 'TOPUP', 'MANUAL_REVIEW', ?, ?, 500000, 'VND', 'BANK_NO_RESPONSE',
                        'Nguyên nhân vì sao', 'mock', ?, now(), now())
                """, id, tenant, UUID.randomUUID(), wallet, "MOCK-" + id);
        jdbcTemplate.update("""
                INSERT INTO saga_state (transaction_id, tenant_id, current_step, step_status, attempt_count,
                                        command_event_id, deadline_at, updated_at)
                VALUES (?, ?, 'BANK_CHARGE', 'MANUAL_REVIEW', 3, ?, now(), now())
                """, id, tenant, UUID.randomUUID());
        return id;
    }

    private ResponseEntity<JsonNode> view(UUID txn, UUID tenant) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(AuthHeaders.TENANT_ID, tenant.toString());
        headers.set(AuthHeaders.USER_ID, operator.toString());
        headers.set(AuthHeaders.USER_EMAIL, "ops@walletflow.dev");
        headers.set(AuthHeaders.USER_ROLE, "OPS");
        return http.exchange("/api/v1/admin/transactions/" + txn, HttpMethod.GET,
                new HttpEntity<>(headers), JsonNode.class);
    }

    private static JsonNode ok(ResponseEntity<JsonNode> response) {
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }
}
