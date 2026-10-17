package com.walletflow.wallet;

import com.fasterxml.jackson.databind.JsonNode;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.services.PendingWalletReconciler;
import com.walletflow.wallet.services.WalletProjector;
import com.walletflow.wallet.support.Events;
import com.walletflow.wallet.support.FakeLedger;
import com.walletflow.wallet.support.FakeLedgerConfig;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * API của wallet-service qua HTTP thật, với header danh tính như gateway gắn.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eureka.client.enabled=false",
                "spring.cloud.config.enabled=false",
                "outbox.relay.delay-ms=3600000",
                "spring.kafka.listener.auto-startup=false",
                "walletflow.kafka.declare-topics=false",
                // Job gửi lại được gọi tay trong test, không để lịch chạy chen vào
                "walletflow.wallet.pending-resend.interval-ms=3600000",
                "management.tracing.enabled=false"
        })
@Import(FakeLedgerConfig.class)
@Testcontainers
class WalletApiIT {

    private static final UUID TENANT = TenantContext.DEFAULT_TENANT;

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
    TestRestTemplate http;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    WalletProjector projector;
    @Autowired
    PendingWalletReconciler reconciler;
    @Autowired
    FakeLedger ledger;
    @Autowired
    EnvelopeReader envelopeReader;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE wallet_entries, wallets, outbox_event");
        ledger.reset();
    }

    // ------------------------------------------------------------------
    // Mở ví
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Mở ví: ví PENDING + đúng MỘT lệnh OpenAccount trong outbox; gọi lại trả ví cũ, không gửi lệnh mới")
    void mo_vi_idempotent_va_gui_lenh_qua_outbox() {
        UUID user = UUID.randomUUID();

        ResponseEntity<JsonNode> first = openWallet(TENANT, user);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String walletId = first.getBody().get("data").get("id").asText();
        assertThat(first.getBody().get("data").get("status").asText()).isEqualTo("PENDING");

        ResponseEntity<JsonNode> again = openWallet(TENANT, user);
        assertThat(again.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(again.getBody().get("data").get("id").asText()).isEqualTo(walletId);

        List<EventEnvelope<OpenAccountCommand>> commands = openCommands();
        assertThat(commands).as("lệnh mở tài khoản đi ra cùng transaction với hàng ví").hasSize(1);
        assertThat(commands.get(0).payload().accountRef()).isEqualTo(walletId);
        assertThat(commands.get(0).tenantId()).isEqualTo(TENANT);
    }

    @Test
    @DisplayName("10 request mở cùng một ví song song: một ví, một lệnh, không request nào lỗi")
    void mo_vi_song_song() throws Exception {
        UUID user = UUID.randomUUID();
        List<Callable<HttpStatus>> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.add(() -> HttpStatus.valueOf(openWallet(TENANT, user).getStatusCode().value()));
        }

        ExecutorService pool = Executors.newFixedThreadPool(10);
        List<HttpStatus> statuses = new ArrayList<>();
        try {
            for (Future<HttpStatus> result : pool.invokeAll(tasks)) {
                statuses.add(result.get());
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(statuses).as("không ai vỡ unique key thành 500").doesNotContain(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(statuses).filteredOn(HttpStatus.CREATED::equals).hasSize(1);
        assertThat(count("SELECT count(*) FROM wallets")).isEqualTo(1);
        assertThat(openCommands()).hasSize(1);
    }

    @Test
    @DisplayName("Thiếu X-User-Id (không đi qua gateway): 401")
    void thieu_danh_tinh_bi_tu_choi() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(AuthHeaders.TENANT_ID, TENANT.toString());
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<JsonNode> response = http.exchange("/api/v1/wallets", HttpMethod.POST,
                new HttpEntity<>(Map.of("currency", "VND"), headers), JsonNode.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("Cùng userId ở tenant khác không thấy ví của tenant này")
    void khong_thay_vi_cua_tenant_khac() {
        UUID user = UUID.randomUUID();
        openWallet(TENANT, user);

        assertThat(get("/api/v1/wallets/me", TENANT, user).getBody().get("data")).hasSize(1);
        assertThat(get("/api/v1/wallets/me", UUID.randomUUID(), user).getBody().get("data")).isEmpty();
    }

    // ------------------------------------------------------------------
    // ⭐ Lịch sử theo cursor
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Phân trang cursor: đủ, không lặp, và không bị xô lệch khi có giao dịch mới chen vào giữa")
    void phan_trang_cursor_on_dinh() {
        UUID user = UUID.randomUUID();
        UUID wallet = activeWallet(user);
        for (int i = 1; i <= 25; i++) {
            apply(Events.topUp(wallet, i));
        }

        JsonNode page1 = history(user, null, 10);
        // Giao dịch mới đến giữa hai lần gọi — với offset, trang 2 sẽ lặp lại hàng cuối của trang 1
        apply(Events.topUp(wallet, 1_000));
        JsonNode page2 = history(user, page1.get("nextCursor").asText(), 10);
        JsonNode page3 = history(user, page2.get("nextCursor").asText(), 10);

        List<Long> amounts = new ArrayList<>();
        for (JsonNode page : List.of(page1, page2, page3)) {
            page.get("items").forEach(item -> amounts.add(item.get("amountMinor").asLong()));
        }

        assertThat(page1.get("items")).hasSize(10);
        assertThat(page2.get("items")).hasSize(10);
        assertThat(page3.get("items")).hasSize(5);
        assertThat(page3.get("nextCursor").isNull()).as("hết trang").isTrue();
        Set<Long> unique = new HashSet<>(amounts);
        assertThat(unique).as("không lặp, không sót").hasSize(25);
        assertThat(amounts.get(0)).as("mới nhất trước").isEqualTo(25);
    }

    @Test
    @DisplayName("Cursor không hợp lệ: 400")
    void cursor_sai_bi_tu_choi() {
        UUID user = UUID.randomUUID();
        activeWallet(user);

        ResponseEntity<JsonNode> response = get(
                "/api/v1/wallets/me/transactions?currency=VND&cursor=abc", TENANT, user);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().get("errorCode").asText()).isEqualTo("INVALID_CURSOR");
    }

    // ------------------------------------------------------------------
    // Quản trị
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Rebuild: USER bị 403, ADMIN dựng lại được số dư từ sổ cái")
    void rebuild_chi_admin() {
        UUID user = UUID.randomUUID();
        UUID wallet = activeWallet(user);
        jdbcTemplate.update("UPDATE wallets SET balance_minor = 123 WHERE id = ?", wallet);
        ledger.declare(TENANT, wallet, EAccountStatus.ACTIVE, 0, List.of());

        assertThat(rebuildAs("USER", wallet).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(count("SELECT balance_minor FROM wallets")).as("USER không đụng được gì").isEqualTo(123);

        ResponseEntity<JsonNode> admin = rebuildAs("ADMIN", wallet);
        assertThat(admin.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(admin.getBody().get("data").get("balanceAfter").asLong()).isZero();
    }

    @Test
    @DisplayName("Ví kẹt PENDING quá lâu được gửi lại lệnh mở tài khoản; ví đã ACTIVE thì không")
    void gui_lai_lenh_cho_vi_ket_pending() {
        UUID stuck = UUID.fromString(openWallet(TENANT, UUID.randomUUID())
                .getBody().get("data").get("id").asText());
        activeWallet(UUID.randomUUID());
        jdbcTemplate.update("UPDATE wallets SET created_at = now() - interval '10 minutes'");
        jdbcTemplate.execute("DELETE FROM outbox_event");

        int resent = reconciler.resendStale();

        assertThat(resent).isEqualTo(1);
        List<EventEnvelope<OpenAccountCommand>> commands = openCommands();
        assertThat(commands).hasSize(1);
        assertThat(commands.get(0).payload().accountRef()).isEqualTo(stuck.toString());
        assertThat(commands.get(0).tenantId())
                .as("job xuyên tenant vẫn gắn đúng tenant của ví vào envelope")
                .isEqualTo(TENANT);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private ResponseEntity<JsonNode> openWallet(UUID tenant, UUID user) {
        HttpHeaders headers = identity(tenant, user, "USER");
        headers.setContentType(MediaType.APPLICATION_JSON);
        return http.exchange("/api/v1/wallets", HttpMethod.POST,
                new HttpEntity<>(Map.of("currency", "VND"), headers), JsonNode.class);
    }

    private UUID activeWallet(UUID user) {
        UUID wallet = UUID.fromString(openWallet(TENANT, user).getBody().get("data").get("id").asText());
        TenantContext.runWith(TENANT,
                () -> projector.applyAccountOpened(Events.opened(wallet, EAccountStatus.ACTIVE, 0)));
        return wallet;
    }

    private void apply(com.walletflow.common.event.LedgerPostedEvent event) {
        TenantContext.runWith(TENANT, () -> projector.applyPosted(event, Instant.now()));
    }

    private JsonNode history(UUID user, String cursor, int limit) {
        String url = "/api/v1/wallets/me/transactions?currency=VND&limit=" + limit
                + (cursor == null ? "" : "&cursor=" + cursor);
        ResponseEntity<JsonNode> response = get(url, TENANT, user);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody().get("data");
    }

    private ResponseEntity<JsonNode> get(String url, UUID tenant, UUID user) {
        return http.exchange(url, HttpMethod.GET, new HttpEntity<>(identity(tenant, user, "USER")), JsonNode.class);
    }

    private ResponseEntity<JsonNode> rebuildAs(String role, UUID wallet) {
        return http.exchange("/api/v1/admin/wallets/" + wallet + "/rebuild", HttpMethod.POST,
                new HttpEntity<>(identity(TENANT, UUID.randomUUID(), role)), JsonNode.class);
    }

    private static HttpHeaders identity(UUID tenant, UUID user, String role) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(AuthHeaders.TENANT_ID, tenant.toString());
        headers.set(AuthHeaders.USER_ID, user.toString());
        headers.set(AuthHeaders.USER_ROLE, role);
        return headers;
    }

    private List<EventEnvelope<OpenAccountCommand>> openCommands() {
        return jdbcTemplate.queryForList(
                        "SELECT payload FROM outbox_event WHERE topic = ? ORDER BY created_at",
                        String.class, WalletTopics.CMD_OPEN_ACCOUNT)
                .stream()
                .peek(json -> EventSchemas.assertValid(WalletTopics.CMD_OPEN_ACCOUNT, json))
                .map(json -> envelopeReader.read(json, OpenAccountCommand.class))
                .toList();
    }

    private long count(String sql) {
        Long value = jdbcTemplate.queryForObject(sql, Long.class);
        return value == null ? 0 : value;
    }
}
