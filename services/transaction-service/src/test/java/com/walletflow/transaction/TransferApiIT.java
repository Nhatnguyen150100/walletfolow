package com.walletflow.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.transaction.support.FakeWalletDirectory;
import com.walletflow.transaction.support.FakeWalletDirectoryConfig;
import com.walletflow.transaction.support.TransferApi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Nhận lệnh chuyển tiền: lệnh ghi sổ đi ra đúng hình dạng, và mọi lệnh không hợp lệ bị chặn TRƯỚC khi
 * có bất cứ thứ gì được ghi.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eureka.client.enabled=false",
                "spring.cloud.config.enabled=false",
                "outbox.relay.delay-ms=3600000",
                "spring.kafka.listener.auto-startup=false",
                "walletflow.kafka.declare-topics=false",
                "walletflow.transaction.saga-deadline=PT2M",
                "management.tracing.enabled=false"
        })
@Import(FakeWalletDirectoryConfig.class)
@Testcontainers
class TransferApiIT {

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
    TestRestTemplate http;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    FakeWalletDirectory wallets;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    EnvelopeReader envelopeReader;

    private UUID alice;
    private UUID aliceWallet;
    private UUID bobWallet;

    @BeforeEach
    void reset() {
        // Không tự gửi lại khi nhận 503 (httpclient5 mặc định làm vậy kể cả với POST) — xem IdempotencyIT
        http.getRestTemplate().setRequestFactory(new JdkClientHttpRequestFactory());
        jdbcTemplate.execute("TRUNCATE resolution_requests, saga_state, transactions, idempotency_keys, outbox_event");
        wallets.reset();
        alice = UUID.randomUUID();
        aliceWallet = wallets.add(TENANT, alice, ECurrency.VND, EWalletStatus.ACTIVE);
        bobWallet = wallets.add(TENANT, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
    }

    // ------------------------------------------------------------------
    // Lệnh hợp lệ
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Lệnh hợp lệ: 202, lệnh ghi sổ đúng schema/vế/partition key, saga bắt đầu với hạn chót")
    void lenh_hop_le_di_ra_dung_hinh_dang() throws Exception {
        Map<String, Object> body = TransferApi.body(aliceWallet, bobWallet, 75_000);
        body.put("note", "tiền cơm");

        ResponseEntity<String> response = TransferApi.transfer(http, TENANT, alice, "ok-1", body);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        JsonNode data = objectMapper.readTree(response.getBody()).get("data");
        UUID transactionId = UUID.fromString(data.get("transactionId").asText());
        assertThat(data.get("status").asText()).isEqualTo("PROCESSING");
        assertThat(response.getHeaders().getLocation()).hasToString("/api/v1/transactions/" + transactionId);

        Map<String, Object> outbox = jdbcTemplate.queryForMap(
                "SELECT msg_key, payload, event_id FROM outbox_event WHERE topic = ?", WalletTopics.CMD_POST_JOURNAL);
        String payload = (String) outbox.get("payload");
        EventSchemas.assertValid(WalletTopics.CMD_POST_JOURNAL, payload);

        EventEnvelope<PostJournalCommand> envelope = envelopeReader.read(payload, PostJournalCommand.class);
        PostJournalCommand command = envelope.payload();
        assertThat(command.transactionId())
                .as("transactionId là khoá idempotency của ledger — phải là id của giao dịch")
                .isEqualTo(transactionId);
        assertThat(command.type()).isEqualTo(EJournalType.TRANSFER);
        assertThat(command.legs()).containsExactly(
                new Leg(aliceWallet.toString(), -75_000), new Leg(bobWallet.toString(), 75_000));
        assertThat(envelope.tenantId()).isEqualTo(TENANT);
        assertThat(outbox.get("msg_key"))
                .as("partition theo ví bị ghi nợ: mọi lệnh của một ví đi đúng thứ tự")
                .isEqualTo(aliceWallet.toString());

        Map<String, Object> saga = jdbcTemplate.queryForMap(
                "SELECT current_step, step_status, attempt_count, command_event_id, deadline_at "
                        + "FROM saga_state WHERE transaction_id = ?", transactionId);
        assertThat(saga.get("current_step")).isEqualTo("POST_JOURNAL");
        assertThat(saga.get("step_status")).isEqualTo("SENT");
        assertThat(saga.get("command_event_id"))
                .as("saga nhớ eventId đã gửi — lần gửi lại (1.2b) phải dùng eventId KHÁC")
                .isEqualTo(outbox.get("event_id"));
        Instant deadline = ((java.sql.Timestamp) saga.get("deadline_at")).toInstant();
        assertThat(deadline).isCloseTo(Instant.now().plus(Duration.ofMinutes(2)), within(Duration.ofSeconds(30)));
    }

    @Test
    @DisplayName("GET giao dịch: người tạo xem được; người khác và tenant khác nhận 404")
    void xem_giao_dich_chi_nguoi_tao() throws Exception {
        ResponseEntity<String> created = TransferApi.transfer(http, TENANT, alice, "ok-2",
                TransferApi.body(aliceWallet, bobWallet, 1_000));
        String txnId = objectMapper.readTree(created.getBody()).get("data").get("transactionId").asText();

        ResponseEntity<JsonNode> mine = TransferApi.get(http, TENANT, alice, txnId);
        assertThat(mine.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(mine.getBody().get("data").get("status").asText()).isEqualTo("PROCESSING");
        assertThat(mine.getBody().get("data").get("amountMinor").asLong()).isEqualTo(1_000);

        assertThat(TransferApi.get(http, TENANT, UUID.randomUUID(), txnId).getStatusCode())
                .as("biết id giao dịch của người khác không đủ để xem nó")
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(TransferApi.get(http, UUID.randomUUID(), alice, txnId).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    // ------------------------------------------------------------------
    // Lệnh không hợp lệ: chặn trước khi ghi bất cứ thứ gì
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Ví nguồn của người khác và ví không tồn tại trả CÙNG một lỗi 404 — không dò được wallet id")
    void vi_nguon_khong_phai_cua_minh() {
        UUID stranger = UUID.randomUUID();

        ResponseEntity<String> notMine = TransferApi.transfer(http, TENANT, stranger, "bad-1",
                TransferApi.body(aliceWallet, bobWallet, 1_000));
        ResponseEntity<String> ghost = TransferApi.transfer(http, TENANT, stranger, "bad-2",
                TransferApi.body(UUID.randomUUID(), bobWallet, 1_000));

        assertThat(notMine.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(errorCode(notMine)).isEqualTo(errorCode(ghost));
        assertNothingAccepted();
    }

    @Test
    @DisplayName("Các lệnh vô lý bị từ chối với mã ổn định, không có gì được ghi")
    void lenh_vo_ly_bi_tu_choi() {
        UUID usdWallet = wallets.add(TENANT, UUID.randomUUID(), ECurrency.USD, EWalletStatus.ACTIVE);
        UUID pendingWallet = wallets.add(TENANT, UUID.randomUUID(), ECurrency.VND, EWalletStatus.PENDING);

        assertError(TransferApi.body(aliceWallet, aliceWallet, 1_000), HttpStatus.BAD_REQUEST, "SAME_WALLET");
        assertError(TransferApi.body(aliceWallet, UUID.randomUUID(), 1_000),
                HttpStatus.UNPROCESSABLE_ENTITY, "RECIPIENT_NOT_FOUND");
        assertError(TransferApi.body(aliceWallet, usdWallet, 1_000),
                HttpStatus.UNPROCESSABLE_ENTITY, "CURRENCY_MISMATCH");
        assertError(TransferApi.body(aliceWallet, pendingWallet, 1_000),
                HttpStatus.UNPROCESSABLE_ENTITY, "RECIPIENT_UNAVAILABLE");
        assertError(TransferApi.body(aliceWallet, bobWallet, 0), HttpStatus.BAD_REQUEST, "VALIDATION_ERROR");
        assertError(TransferApi.body(aliceWallet, bobWallet, -5), HttpStatus.BAD_REQUEST, "VALIDATION_ERROR");

        Map<String, Object> badCurrency = TransferApi.body(aliceWallet, bobWallet, 1_000);
        badCurrency.put("currency", "XYZ");
        assertError(badCurrency, HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST");

        assertNothingAccepted();
    }

    @Test
    @DisplayName("Ví nguồn bị đóng băng: 422; ví NHẬN bị đóng băng vẫn nhận được (ADR-011)")
    void dong_bang_chan_gui_khong_chan_nhan() {
        wallets.setStatus(TENANT, aliceWallet, EWalletStatus.FROZEN);
        assertError(TransferApi.body(aliceWallet, bobWallet, 1_000),
                HttpStatus.UNPROCESSABLE_ENTITY, "WALLET_NOT_ACTIVE");

        wallets.setStatus(TENANT, aliceWallet, EWalletStatus.ACTIVE);
        wallets.setStatus(TENANT, bobWallet, EWalletStatus.FROZEN);
        assertThat(TransferApi.transfer(http, TENANT, alice, "to-frozen",
                TransferApi.body(aliceWallet, bobWallet, 1_000)).getStatusCode())
                .isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    @DisplayName("wallet-service sập: 503 và KHÔNG giữ key — gửi lại cùng key sau đó được nhận bình thường")
    void wallet_service_sap_thi_503_va_gui_lai_duoc() {
        Map<String, Object> body = TransferApi.body(aliceWallet, bobWallet, 1_000);
        wallets.goDown();

        ResponseEntity<String> down = TransferApi.transfer(http, TENANT, alice, "retry-me", body);
        assertThat(down.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(errorCode(down)).isEqualTo("WALLET_SERVICE_UNAVAILABLE");
        assertNothingAccepted();

        wallets.reset();
        UUID aliceAgain = wallets.add(TENANT, alice, ECurrency.VND, EWalletStatus.ACTIVE);
        UUID bobAgain = wallets.add(TENANT, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
        ResponseEntity<String> retried = TransferApi.transfer(http, TENANT, alice, "retry-me",
                TransferApi.body(aliceAgain, bobAgain, 1_000));
        assertThat(retried.getStatusCode())
                .as("lỗi 503 không được 'ghim' key — nếu không, client không bao giờ gửi lại được")
                .isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    @DisplayName("Lỗi theo RFC 7807: application/problem+json, có type/title/status/detail/instance/errorCode")
    void loi_theo_rfc7807() throws Exception {
        ResponseEntity<String> response = TransferApi.transfer(http, TENANT, alice, "rfc",
                TransferApi.body(aliceWallet, aliceWallet, 1_000));

        assertThat(response.getHeaders().getContentType().toString()).startsWith("application/problem+json");
        JsonNode problem = objectMapper.readTree(response.getBody());
        assertThat(problem.get("type").asText()).isEqualTo("https://walletflow.dev/problems/same-wallet");
        assertThat(problem.get("status").asInt()).isEqualTo(400);
        assertThat(problem.get("instance").asText()).isEqualTo("/api/v1/transactions/transfer");
        assertThat(problem.has("title")).isTrue();
        assertThat(problem.has("detail")).isTrue();
    }

    @Test
    @DisplayName("Thiếu X-User-Id (không đi qua gateway): 401")
    void thieu_danh_tinh() {
        ResponseEntity<String> response = TransferApi.transfer(http, TENANT, null, "no-user",
                TransferApi.body(aliceWallet, bobWallet, 1_000));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private void assertError(Map<String, Object> body, HttpStatus status, String code) {
        ResponseEntity<String> response =
                TransferApi.transfer(http, TENANT, alice, "k-" + UUID.randomUUID(), body);
        assertThat(response.getStatusCode()).as(code).isEqualTo(status);
        assertThat(errorCode(response)).isEqualTo(code);
    }

    private String errorCode(ResponseEntity<String> response) {
        try {
            return objectMapper.readTree(response.getBody()).get("errorCode").asText();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void assertNothingAccepted() {
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM idempotency_keys", Long.class))
                .as("không key nào bị 'ghim' bởi một lệnh không được nhận").isZero();
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM transactions", Long.class)).isZero();
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM outbox_event", Long.class)).isZero();
    }
}
