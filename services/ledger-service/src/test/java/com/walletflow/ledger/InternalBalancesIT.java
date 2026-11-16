package com.walletflow.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.ledger.AccountBalance;
import com.walletflow.common.ledger.AccountBalancesRequest;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.services.AccountService;
import com.walletflow.ledger.services.LedgerService;
import com.walletflow.ledger.services.SystemAccountProvisioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code POST /internal/accounts/balances} (roadmap 3.1, ADR-023 §3) — đường đọc số dư theo lô mà
 * đối soát tầng 2 của wallet-service dựa vào.
 *
 * <p>Cái cần chứng minh không phải "trả được số dư" mà là các PHÂN BIỆT mà bên đối soát đọc ra kết
 * luận từ đó:
 * <ol>
 *   <li><b>0 ≠ vắng mặt</b> — tài khoản vừa mở, chưa có posting, phải hiện với số dư 0 ("khớp");
 *       ref ledger không có tài khoản thì KHÔNG hiện ("thiếu tài khoản" — một break). Nếu INNER JOIN
 *       làm tài khoản 0 đồng biến mất, đối soát sẽ kêu oan cho mọi ví mới.</li>
 *   <li><b>Cô lập tenant</b> — hỏi bằng tenant khác không thấy gì, y như ref lạ.</li>
 *   <li><b>Trần 500 ref</b> — hàng rào bộ nhớ với {@code errorCode} ổn định để người gọi chia lô.</li>
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
                "management.tracing.enabled=false",
                "walletflow.ledger.reconciliation.initial-delay=PT24H"
        })
@Testcontainers
class InternalBalancesIT {

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
    // 1 — Đúng số dư, nhiều tài khoản một lượt
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Nhiều ví một lượt: mỗi ví trả đúng SUM(postings) của nó, gồm cả ví đã rút và ví nhận chuyển")
    void tra_dung_so_du_cho_nhieu_tai_khoan_mot_lan() {
        UUID tenant = newTenant();
        String a = openWallet(tenant);
        String b = openWallet(tenant);
        String c = openWallet(tenant);
        post(tenant, topUp(a, 100_000));
        post(tenant, topUp(b, 50_000));
        post(tenant, new PostJournalCommand(UUID.randomUUID(), EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(a, -30_000), new Leg(c, 30_000))));
        post(tenant, new PostJournalCommand(UUID.randomUUID(), EJournalType.WITHDRAW, null, ECurrency.VND,
                List.of(new Leg(b, -20_000), new Leg(BANK, 20_000))));

        Response response = balances(tenant, ECurrency.VND, List.of(a, b, c));

        assertThat(response.status()).isEqualTo(200);
        assertThat(asMap(response.body()))
                .as("a: 100k-30k; b: 50k-20k; c: 30k — khớp SUM(postings) từng ví")
                .containsExactlyInAnyOrderEntriesOf(Map.of(a, 70_000L, b, 30_000L, c, 30_000L));
        for (String wallet : List.of(a, b, c)) {
            Long inDb = jdbcTemplate.queryForObject("""
                    SELECT COALESCE(SUM(p.amount_minor), 0) FROM postings p
                      JOIN accounts x ON x.id = p.account_id
                     WHERE x.tenant_id = ? AND x.external_ref = ?
                    """, Long.class, tenant, wallet);
            assertThat(asMap(response.body()).get(wallet)).isEqualTo(inDb);
        }
    }

    // ------------------------------------------------------------------
    // 2 — 0 không được biến mất
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Tài khoản vừa mở, chưa có bút toán: CÓ mặt trong kết quả với balanceMinor = 0")
    void tai_khoan_chua_co_but_toan_tra_ve_0_chu_khong_bien_mat() {
        UUID tenant = newTenant();
        String fresh = openWallet(tenant);
        String funded = openWallet(tenant);
        post(tenant, topUp(funded, 10_000));

        Response response = balances(tenant, ECurrency.VND, List.of(fresh, funded));

        assertThat(response.status()).isEqualTo(200);
        assertThat(asMap(response.body()))
                .as("INNER JOIN postings làm ví 0 đồng biến mất, và đối soát sẽ đọc thành "
                        + "'ledger không có tài khoản' — kêu oan cho mọi ví mới")
                .containsEntry(fresh, 0L)
                .containsEntry(funded, 10_000L)
                .hasSize(2);
    }

    // ------------------------------------------------------------------
    // 3 — Vắng mặt ≠ 0
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Ref không có tài khoản: VẮNG MẶT trong kết quả (khác 0), không làm hỏng cả lô")
    void ref_khong_co_tai_khoan_thi_khong_co_trong_ket_qua() {
        UUID tenant = newTenant();
        String known = openWallet(tenant);
        post(tenant, topUp(known, 5_000));
        String unknown = UUID.randomUUID().toString();

        Response response = balances(tenant, ECurrency.VND, List.of(known, unknown));

        assertThat(response.status())
                .as("một ví chưa mở xong không được làm hỏng cả lô 500 ví (không 404)")
                .isEqualTo(200);
        Map<String, Long> result = asMap(response.body());
        assertThat(result).containsEntry(known, 5_000L);
        assertThat(result)
                .as("vắng mặt = ledger CHƯA CÓ tài khoản — kết luận khác hẳn 'số dư 0'")
                .doesNotContainKey(unknown)
                .hasSize(1);
    }

    @Test
    @DisplayName("Cùng ref nhưng khác loại tiền: tài khoản (ref, USD) chưa tồn tại nên vắng mặt, không lẫn số dư VND")
    void ref_dung_nhung_khac_loai_tien_thi_vang_mat() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        post(tenant, topUp(wallet, 8_000));

        Response usd = balances(tenant, ECurrency.USD, List.of(wallet));

        assertThat(usd.status()).isEqualTo(200);
        assertThat(asMap(usd.body()))
                .as("tài khoản được định danh bởi (ref, currency)")
                .isEmpty();
        assertThat(asMap(balances(tenant, ECurrency.VND, List.of(wallet)).body()))
                .containsEntry(wallet, 8_000L);
    }

    // ------------------------------------------------------------------
    // 4 — Cô lập tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Hỏi bằng tenant khác: không thấy gì (y như ref lạ), tenant chủ sở hữu vẫn thấy đúng số dư")
    void hoi_bang_tenant_khac_thi_khong_thay_gi() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        String walletA = openWallet(tenantA);
        post(tenantA, topUp(walletA, 42_000));

        Response asOther = balances(tenantB, ECurrency.VND, List.of(walletA));
        Response asUnknown = balances(tenantB, ECurrency.VND, List.of(UUID.randomUUID().toString()));

        assertThat(asOther.status()).isEqualTo(200).isEqualTo(asUnknown.status());
        assertThat(asMap(asOther.body()))
                .as("biết ref của người khác không đủ để đọc số dư của họ; trả y như ref lạ để không dò được")
                .isEmpty();
        assertThat(asMap(asOther.body())).isEqualTo(asMap(asUnknown.body()));
        assertThat(asMap(balances(tenantA, ECurrency.VND, List.of(walletA)).body()))
                .containsEntry(walletA, 42_000L);
    }

    @Test
    @DisplayName("Hai tenant dùng CÙNG ref ví: mỗi tenant chỉ thấy số dư của riêng mình")
    void cung_ref_o_hai_tenant_khong_tron_so_du() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        String shared = UUID.randomUUID().toString();
        openWallet(tenantA, shared);
        openWallet(tenantB, shared);
        post(tenantA, topUp(shared, 60_000));
        post(tenantB, topUp(shared, 11_000));

        assertThat(asMap(balances(tenantA, ECurrency.VND, List.of(shared)).body()))
                .containsExactlyEntriesOf(Map.of(shared, 60_000L));
        assertThat(asMap(balances(tenantB, ECurrency.VND, List.of(shared)).body()))
                .as("LEFT JOIN postings mà thiếu lọc tenant sẽ cộng cả posting của tenant kia")
                .containsExactlyEntriesOf(Map.of(shared, 11_000L));
    }

    // ------------------------------------------------------------------
    // 5 — Trần số ref
    // ------------------------------------------------------------------
    @Test
    @DisplayName("501 ref bị từ chối: 400 + errorCode TOO_MANY_ACCOUNT_REFS; đúng 500 ref thì vẫn được")
    void qua_500_ref_thi_bi_tu_choi() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        post(tenant, topUp(wallet, 1_000));

        List<String> tooMany = refsIncluding(wallet, AccountBalancesRequest.MAX_REFS + 1);
        Response rejected = balances(tenant, ECurrency.VND, tooMany);

        assertThat(rejected.status()).isEqualTo(400);
        assertThat(rejected.body().path("errorCode").asText()).isEqualTo("TOO_MANY_ACCOUNT_REFS");

        List<String> atLimit = refsIncluding(wallet, AccountBalancesRequest.MAX_REFS);
        Response accepted = balances(tenant, ECurrency.VND, atLimit);
        assertThat(accepted.status()).as("đúng trần thì không bị chặn").isEqualTo(200);
        assertThat(asMap(accepted.body())).containsExactlyEntriesOf(Map.of(wallet, 1_000L));
    }

    @Test
    @DisplayName("Danh sách ref rỗng: 400, không trả 200 kèm mảng rỗng khiến người gọi tưởng 'không có gì lệch'")
    void danh_sach_ref_rong_thi_bi_tu_choi() {
        UUID tenant = newTenant();

        Response response = balances(tenant, ECurrency.VND, List.of());

        assertThat(response.status()).isEqualTo(400);
        assertThat(response.body().path("errorCode").asText()).isNotBlank();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private record Response(int status, JsonNode body) {
    }

    private Response balances(UUID tenant, ECurrency currency, List<String> refs) {
        return RestClient.create("http://localhost:" + port).post()
                .uri("/internal/accounts/balances")
                .header(AuthHeaders.TENANT_ID, tenant.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new AccountBalancesRequest(currency, refs))
                .exchange((request, response) ->
                        new Response(response.getStatusCode().value(), response.bodyTo(JsonNode.class)));
    }

    /** Đọc mảng JSON {@code [{accountRef, balanceMinor}]} thành map — qua record hợp đồng thật. */
    private static Map<String, Long> asMap(JsonNode body) {
        assertThat(body.isArray()).as("200 phải là một mảng, nhận: %s", body).isTrue();
        List<AccountBalance> rows = new ArrayList<>();
        body.forEach(node -> rows.add(
                new AccountBalance(node.path("accountRef").asText(), node.path("balanceMinor").asLong())));
        return rows.stream().collect(Collectors.toMap(AccountBalance::accountRef, AccountBalance::balanceMinor));
    }

    private static List<String> refsIncluding(String real, int total) {
        List<String> refs = new ArrayList<>();
        refs.add(real);
        while (refs.size() < total) {
            refs.add(UUID.randomUUID().toString());
        }
        return refs;
    }

    /** Tenant mới có bộ tài khoản hệ thống RIÊNG (ADR-010), nên mỗi test tự dựng tenant của mình. */
    private UUID newTenant() {
        UUID tenant = UUID.randomUUID();
        provisioner.provision(tenant);
        return tenant;
    }

    private String openWallet(UUID tenant) {
        return openWallet(tenant, UUID.randomUUID().toString());
    }

    private String openWallet(UUID tenant, String ref) {
        TenantContext.callWith(tenant,
                () -> accountService.openUserAccount(new OpenAccountCommand(ref, ECurrency.VND)));
        return ref;
    }

    private void post(UUID tenant, PostJournalCommand command) {
        TenantContext.callWith(tenant, () -> ledgerService.postJournal(command));
    }

    private static PostJournalCommand topUp(String wallet, long amountMinor) {
        return new PostJournalCommand(UUID.randomUUID(), EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(BANK, -amountMinor), new Leg(wallet, amountMinor)));
    }
}
