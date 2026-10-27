package com.walletflow.ledger;

import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.ledger.EntryLine;
import com.walletflow.common.ledger.JournalEntryView;
import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.entity.JournalEntry;
import com.walletflow.ledger.services.AccountService;
import com.walletflow.ledger.services.LedgerService;
import com.walletflow.ledger.services.SystemAccountProvisioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code GET /internal/entries?transactionId=...} (roadmap 2.5 PR3) — đường đọc để người trực biết
 * ledger đã ghi gì cho một lệnh tiền.
 *
 * <p>Các tính chất, mỗi cái chặn một cách người trực bị dẫn đi sai:
 * <ol>
 *   <li><b>Đủ hai vế, đúng thứ tự</b> — thiếu một vế thì không thấy tiền đi từ đâu sang đâu.</li>
 *   <li><b>Chưa ghi sổ ≠ lỗi</b> — 200 + rỗng, không 404: "tiền chưa vào sổ" và "không hỏi được
 *       ledger" dẫn tới hai quyết định vận hành ngược nhau.</li>
 *   <li><b>Cô lập tenant</b> — id của tenant khác trả đúng câu trả lời như id không tồn tại.</li>
 *   <li><b>{@code entryId} là id thật</b> — REVERSAL ở PR4 dùng nó làm {@code referenceId}.</li>
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
                "management.tracing.enabled=false"
        })
@Testcontainers
class InternalEntriesIT {

    @LocalServerPort
    int port;

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
    AccountService accountService;
    @Autowired
    LedgerService ledgerService;
    @Autowired
    SystemAccountProvisioner provisioner;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE postings, journal_entries");
        jdbcTemplate.execute("DELETE FROM outbox_event");
        jdbcTemplate.execute("DELETE FROM processed_event");
        jdbcTemplate.execute("DELETE FROM accounts WHERE type = 'USER'");
    }

    // ------------------------------------------------------------------
    // 1 — Đủ hai vế, đúng thứ tự
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Lệnh chuyển tiền đã ghi sổ: đúng MỘT bút toán, ĐỦ hai vế, tổng bằng 0, vế nợ trước vế có")
    void lenh_da_ghi_so_tra_ve_mot_but_toan_du_hai_ve() {
        UUID tenant = newTenant();
        String from = openWallet(tenant);
        String to = openWallet(tenant);
        post(tenant, topUp(from, 100_000));
        UUID transactionId = UUID.randomUUID();
        JournalEntry posted = post(tenant, new PostJournalCommand(transactionId, EJournalType.TRANSFER, null,
                ECurrency.VND, List.of(new Leg(to, 30_000), new Leg(from, -30_000))));   // cố ý đưa vế có lên trước

        TransactionEntries result = entriesOf(tenant, transactionId);

        assertThat(result.transactionId()).isEqualTo(transactionId);
        assertThat(result.entries()).as("đúng một bút toán cho lệnh này").hasSize(1);
        JournalEntryView entry = result.entries().getFirst();
        assertThat(entry.type()).isEqualTo(EJournalType.TRANSFER);
        assertThat(entry.referenceId()).isNull();
        assertThat(entry.postedAt()).isNotNull();
        assertThat(entry.lines()).as("đủ cả hai vế, không gộp").hasSize(2);
        assertThat(entry.lines()).extracting(EntryLine::amountMinor)
                .as("vế ghi nợ (âm) đứng TRƯỚC vế ghi có (dương), bất kể thứ tự trong lệnh")
                .containsExactly(-30_000L, 30_000L);
        assertThat(entry.lines()).extracting(EntryLine::accountRef)
                .as("accountRef đọc được, không chỉ là UUID")
                .containsExactly(from, to);
        assertThat(entry.lines()).extracting(EntryLine::accountId)
                .doesNotContainNull()
                .doesNotHaveDuplicates();
        assertThat(entry.sumMinor()).as("bất biến vàng: tổng hai vế = 0").isZero();
        assertThat(entry.balanced()).isTrue();
        assertThat(entry.entryId()).isEqualTo(posted.getId());
    }

    // ------------------------------------------------------------------
    // 2 — Chưa ghi sổ ≠ lỗi
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ transactionId chưa có bút toán nào: 200 + danh sách RỖNG, KHÔNG phải 404")
    void chua_ghi_so_tra_ve_200_rong_khong_phai_404() {
        UUID tenant = newTenant();
        UUID neverPosted = UUID.randomUUID();

        RawResponse response = get(tenant, neverPosted);

        assertThat(response.status())
                .as("404 buộc người gọi coi 'chưa vào sổ' như 'không hỏi được ledger'")
                .isEqualTo(200);
        TransactionEntries body = response.body();
        assertThat(body).isNotNull();
        assertThat(body.transactionId()).isEqualTo(neverPosted);
        assertThat(body.entries()).isEmpty();
    }

    // ------------------------------------------------------------------
    // 3 — Cô lập tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Cô lập tenant: bút toán của tenant A không hiện khi hỏi bằng tenant B, và trả rỗng y như id lạ")
    void but_toan_cua_tenant_khac_khong_hien_ra() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        UUID transactionId = UUID.randomUUID();
        String walletA = openWallet(tenantA);
        JournalEntry entryA = post(tenantA, new PostJournalCommand(transactionId, EJournalType.TOPUP, null,
                ECurrency.VND, List.of(new Leg(BANK, -50_000), new Leg(walletA, 50_000))));

        RawResponse asOther = get(tenantB, transactionId);
        RawResponse asUnknownId = get(tenantB, UUID.randomUUID());

        assertThat(asOther.status())
                .as("không 404, không 403: khác biệt với id lạ sẽ lộ việc id này có tồn tại")
                .isEqualTo(200)
                .isEqualTo(asUnknownId.status());
        assertThat(asOther.body().entries()).isEmpty();
        assertThat(asOther.body().entries()).isEqualTo(asUnknownId.body().entries());
        assertThat(entriesOf(tenantA, transactionId).entries()).extracting(JournalEntryView::entryId)
                .as("chính tenant A vẫn thấy bút toán của mình")
                .containsExactly(entryA.getId());
    }

    @Test
    @DisplayName("Cùng transactionId ở hai tenant: mỗi tenant chỉ thấy bút toán và số tiền CỦA MÌNH")
    void cung_transaction_id_o_hai_tenant_khong_tron_so() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        UUID transactionId = UUID.randomUUID();
        String walletA = openWallet(tenantA);
        String walletB = openWallet(tenantB);
        JournalEntry entryA = post(tenantA, new PostJournalCommand(transactionId, EJournalType.TOPUP, null,
                ECurrency.VND, List.of(new Leg(BANK, -50_000), new Leg(walletA, 50_000))));
        JournalEntry entryB = post(tenantB, new PostJournalCommand(transactionId, EJournalType.TOPUP, null,
                ECurrency.VND, List.of(new Leg(BANK, -70_000), new Leg(walletB, 70_000))));

        TransactionEntries seenByA = entriesOf(tenantA, transactionId);
        TransactionEntries seenByB = entriesOf(tenantB, transactionId);

        assertThat(seenByA.entries()).extracting(JournalEntryView::entryId).containsExactly(entryA.getId());
        assertThat(seenByA.entries().getFirst().lines()).extracting(EntryLine::amountMinor)
                .containsExactly(-50_000L, 50_000L);
        assertThat(seenByB.entries()).extracting(JournalEntryView::entryId).containsExactly(entryB.getId());
        assertThat(seenByB.entries().getFirst().lines()).extracting(EntryLine::amountMinor)
                .containsExactly(-70_000L, 70_000L);
        assertThat(seenByB.entries().getFirst().lines()).extracting(EntryLine::accountRef)
                .as("tài khoản hệ thống là của RIÊNG từng tenant nhưng cùng accountRef; ví B không lẫn sang A")
                .containsExactly(BANK, walletB);
    }

    // ------------------------------------------------------------------
    // 4 — Thiếu header tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Thiếu header X-Tenant-Id: không trả dữ liệu nào (không đoán tenant mặc định)")
    void thieu_header_tenant_khong_tra_du_lieu() {
        UUID tenant = newTenant();
        UUID transactionId = UUID.randomUUID();
        String wallet = openWallet(tenant);
        post(tenant, new PostJournalCommand(transactionId, EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(BANK, -10_000), new Leg(wallet, 10_000))));

        RestClient client = RestClient.create("http://localhost:" + port);
        String body = client.get()
                .uri("/internal/entries?transactionId={id}", transactionId)
                .exchange((request, response) -> {
                    int status = response.getStatusCode().value();
                    String text = new String(response.getBody().readAllBytes());
                    return status + "|" + text;
                });

        String status = body.substring(0, body.indexOf('|'));
        String text = body.substring(body.indexOf('|') + 1);
        // Quan sát thực tế: TenantFilter cho request không header đi tiếp, TenantContext.require() ném
        // IllegalStateException (lỗi lập trình/đường vào sai, không phải lỗi khách) -> 500 INTERNAL_ERROR.
        assertThat(status).as("không bao giờ 200 khi không biết tenant").isEqualTo("500");
        assertThat(text).contains("INTERNAL_ERROR");
        assertThat(text)
                .as("500 không lộ message kỹ thuật và không kèm dữ liệu sổ")
                .doesNotContain("TenantContext")
                .doesNotContain("tenant trong luồng")
                .doesNotContain(transactionId.toString())
                .doesNotContain("EXTERNAL_BANK");
    }

    // ------------------------------------------------------------------
    // 5 — Bút toán gốc + REVERSAL
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Lệnh có cả bút toán gốc và REVERSAL: 2 bút toán theo thứ tự ghi sổ, REVERSAL trỏ về bút toán gốc")
    void co_reversal_tra_hai_but_toan_theo_thu_tu_ghi_so() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        post(tenant, topUp(wallet, 100_000));
        UUID transactionId = UUID.randomUUID();
        JournalEntry withdraw = post(tenant, new PostJournalCommand(transactionId, EJournalType.WITHDRAW, null,
                ECurrency.VND, List.of(new Leg(wallet, -40_000), new Leg(BANK, 40_000))));
        JournalEntry reversal = post(tenant, new PostJournalCommand(transactionId, EJournalType.REVERSAL,
                withdraw.getId(), ECurrency.VND, List.of(new Leg(wallet, 40_000), new Leg(BANK, -40_000))));

        TransactionEntries result = entriesOf(tenant, transactionId);

        assertThat(result.entries()).extracting(JournalEntryView::type)
                .as("theo thứ tự ghi sổ: gốc rồi đảo")
                .containsExactly(EJournalType.WITHDRAW, EJournalType.REVERSAL);
        JournalEntryView original = result.entries().get(0);
        JournalEntryView reverse = result.entries().get(1);
        assertThat(original.entryId()).isEqualTo(withdraw.getId());
        assertThat(original.referenceId()).isNull();
        assertThat(reverse.entryId()).isEqualTo(reversal.getId());
        assertThat(reverse.referenceId())
                .as("thấy ngay ca này đã được đảo rồi, khỏi đảo lần hai")
                .isEqualTo(withdraw.getId());
        assertThat(original.lines()).hasSize(2);
        assertThat(reverse.lines()).hasSize(2);
        assertThat(original.balanced()).isTrue();
        assertThat(reverse.balanced()).isTrue();
        assertThat(original.lines()).extracting(EntryLine::accountRef).containsExactly(wallet, BANK);
        assertThat(reverse.lines()).extracting(EntryLine::accountRef).containsExactly(BANK, wallet);
    }

    // ------------------------------------------------------------------
    // 6 — entryId khớp DB
    // ------------------------------------------------------------------
    @Test
    @DisplayName("entryId trả về khớp đúng journal_entries.id trong DB (REVERSAL ở PR4 dựa vào nó)")
    void entry_id_khop_journal_entries_id_trong_db() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        UUID transactionId = UUID.randomUUID();
        post(tenant, new PostJournalCommand(transactionId, EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(BANK, -25_000), new Leg(wallet, 25_000))));

        TransactionEntries result = entriesOf(tenant, transactionId);

        UUID idInDb = jdbcTemplate.queryForObject(
                "SELECT id FROM journal_entries WHERE tenant_id = ? AND transaction_id = ?",
                UUID.class, tenant, transactionId);
        assertThat(result.entries()).singleElement()
                .extracting(JournalEntryView::entryId)
                .isEqualTo(idInDb);
        // Mỗi vế cũng phải là posting thật của bút toán đó
        List<Long> postingsInDb = jdbcTemplate.queryForList(
                "SELECT amount_minor FROM postings WHERE tenant_id = ? AND entry_id = ? ORDER BY amount_minor",
                Long.class, tenant, idInDb);
        assertThat(result.entries().getFirst().lines()).extracting(EntryLine::amountMinor)
                .containsExactlyElementsOf(postingsInDb);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Phản hồi thô để assert được cả status lẫn body (RestClient mặc định ném khi 4xx). */
    private record RawResponse(int status, TransactionEntries body) {
    }

    private RawResponse get(UUID tenant, UUID transactionId) {
        return RestClient.create("http://localhost:" + port).get()
                .uri("/internal/entries?transactionId={id}", transactionId)
                .header(AuthHeaders.TENANT_ID, tenant.toString())
                .exchange((request, response) -> {
                    int status = response.getStatusCode().value();
                    TransactionEntries body = status == 200 ? response.bodyTo(TransactionEntries.class) : null;
                    return new RawResponse(status, body);
                });
    }

    private TransactionEntries entriesOf(UUID tenant, UUID transactionId) {
        RawResponse response = get(tenant, transactionId);
        assertThat(response.status()).isEqualTo(200);
        return response.body();
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
