package com.walletflow.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ⭐ "Gửi lại cùng Idempotency-Key thì không trừ tiền lần hai" (roadmap 1.2, DoD Phase 1).
 *
 * <p>Thước đo duy nhất có ý nghĩa: <b>số lệnh ghi sổ đi ra outbox</b>. Đó là thứ biến thành bút toán
 * ở ledger. Một câu trả lời 202 trông đúng mà outbox có hai lệnh nghĩa là tiền bị trừ hai lần.
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
@Import(FakeWalletDirectoryConfig.class)
@Testcontainers
class IdempotencyIT {

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

    private UUID alice;
    private UUID aliceWallet;
    private UUID bobWallet;

    @BeforeEach
    void reset() {
        // Client test KHÔNG được tự gửi lại. httpclient5 (có trên classpath qua Eureka) là mặc định
        // của TestRestTemplate, và nó tự gửi lại request khi nhận 503 — KỂ CẢ POST. Lần gửi lại đó gặp
        // key đã có câu trả lời và được replay, nên một bug "trả 503 cho lệnh đã được nhận" bị che mất
        // hoàn toàn. Đã xảy ra thật: mutation-test bỏ phần sửa mà test vẫn xanh.
        http.getRestTemplate().setRequestFactory(new JdkClientHttpRequestFactory());
        jdbcTemplate.execute("TRUNCATE saga_state, transactions, idempotency_keys, outbox_event");
        wallets.reset();
        alice = UUID.randomUUID();
        aliceWallet = wallets.add(TENANT, alice, ECurrency.VND, EWalletStatus.ACTIVE);
        bobWallet = wallets.add(TENANT, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
    }

    @Test
    @DisplayName("Gửi lại cùng key: cùng câu trả lời TỪNG BYTE, đúng MỘT lệnh ghi sổ")
    void gui_lai_tra_ve_dung_cau_tra_loi_cu() {
        Map<String, Object> body = TransferApi.body(aliceWallet, bobWallet, 50_000);

        ResponseEntity<String> first = TransferApi.transfer(http, TENANT, alice, "key-1", body);
        ResponseEntity<String> again = TransferApi.transfer(http, TENANT, alice, "key-1", body);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(again.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(again.getBody())
                .as("kể cả acceptedAt/timestamp: lần gửi lại nhận ĐÚNG câu trả lời cũ, không phải câu mới giống thế")
                .isEqualTo(first.getBody());
        assertThat(first.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("false");
        assertThat(again.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(again.getHeaders().getLocation()).isEqualTo(first.getHeaders().getLocation());

        assertThat(postJournalCommands()).as("đúng một lệnh ghi sổ = tiền chỉ bị trừ một lần").isEqualTo(1);
        assertThat(count("transactions")).isEqualTo(1);
        assertThat(count("saga_state")).isEqualTo(1);
    }

    @Test
    @DisplayName("⭐ Nạp tiền gửi lại cùng key: cùng câu trả lời TỪNG BYTE, đúng MỘT lệnh thu tiền")
    void gui_lai_lenh_nap_khong_tao_phien_thu_thu_hai() {
        Map<String, Object> body = TransferApi.topupBody(aliceWallet, 500_000);

        ResponseEntity<String> first = TransferApi.topup(http, TENANT, alice, "topup-1", body);
        ResponseEntity<String> again = TransferApi.topup(http, TENANT, alice, "topup-1", body);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(again.getBody()).isEqualTo(first.getBody());
        assertThat(again.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(commands(WalletTopics.CMD_BANK_CHARGE))
                .as("lệnh thu thứ hai là một lần người dùng bị trừ tiền thứ hai ở ngân hàng")
                .hasSize(1);
        assertThat(commands(WalletTopics.CMD_POST_JOURNAL))
                .as("nhận lệnh nạp KHÔNG được ghi sổ — chưa ai trả đồng nào")
                .isEmpty();
    }

    @Test
    @DisplayName("Cùng key dùng cho lệnh chuyển rồi lệnh nạp: 422, không lặng lẽ phát lại câu trả lời cũ")
    void cung_key_cho_hai_loai_lenh_bi_tu_choi() {
        TransferApi.transfer(http, TENANT, alice, "k-chung", TransferApi.body(aliceWallet, bobWallet, 50_000));

        ResponseEntity<String> topup =
                TransferApi.topup(http, TENANT, alice, "k-chung", TransferApi.topupBody(aliceWallet, 50_000));

        assertThat(topup.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(commands(WalletTopics.CMD_BANK_CHARGE)).isEmpty();
    }

    @Test
    @DisplayName("⭐ 20 request cùng key ĐỒNG THỜI: đúng một lệnh ghi sổ, mọi request nhận cùng transactionId")
    void gui_trung_song_song_chi_mot_lenh() throws Exception {
        Map<String, Object> body = TransferApi.body(aliceWallet, bobWallet, 10_000);
        int threads = 20;
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<ResponseEntity<String>>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                start.await();   // thả tất cả cùng lúc để thực sự đua nhau
                return TransferApi.transfer(http, TENANT, alice, "concurrent-key", body);
            });
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<ResponseEntity<String>> responses = new ArrayList<>();
        try {
            List<Future<ResponseEntity<String>>> futures = new ArrayList<>();
            for (Callable<ResponseEntity<String>> task : tasks) {
                futures.add(pool.submit(task));
            }
            start.countDown();
            for (Future<ResponseEntity<String>> future : futures) {
                responses.add(future.get());
            }
        } finally {
            pool.shutdownNow();
        }

        Set<String> transactionIds = new HashSet<>();
        for (ResponseEntity<String> response : responses) {
            assertThat(response.getStatusCode())
                    .as("không request nào vỡ unique key thành 500 hay nhận 409")
                    .isEqualTo(HttpStatus.ACCEPTED);
            transactionIds.add(data(response).get("transactionId").asText());
        }
        assertThat(transactionIds).as("mọi request trỏ về CÙNG một giao dịch").hasSize(1);
        assertThat(postJournalCommands()).isEqualTo(1);
        assertThat(count("transactions")).isEqualTo(1);
    }

    @Test
    @DisplayName("Cùng key nhưng KHÁC nội dung: 422 — bug phía client phải được báo to, không lặng lẽ trả lệnh cũ")
    void cung_key_khac_noi_dung_bi_tu_choi() {
        TransferApi.transfer(http, TENANT, alice, "key-2", TransferApi.body(aliceWallet, bobWallet, 50_000));

        ResponseEntity<String> reused = TransferApi.transfer(http, TENANT, alice, "key-2",
                TransferApi.body(aliceWallet, bobWallet, 70_000));

        assertThat(reused.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(problem(reused).get("errorCode").asText()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        assertThat(postJournalCommands()).as("lệnh 70 000 KHÔNG được nhận").isEqualTo(1);
    }

    @Test
    @DisplayName("Key được tính THEO NGƯỜI DÙNG: hai người trùng chuỗi key là hai lệnh độc lập")
    void key_tinh_theo_nguoi_dung() {
        UUID carol = UUID.randomUUID();
        UUID carolWallet = wallets.add(TENANT, carol, ECurrency.VND, EWalletStatus.ACTIVE);

        ResponseEntity<String> aliceResponse = TransferApi.transfer(http, TENANT, alice, "shared-key",
                TransferApi.body(aliceWallet, bobWallet, 1_000));
        ResponseEntity<String> carolResponse = TransferApi.transfer(http, TENANT, carol, "shared-key",
                TransferApi.body(carolWallet, bobWallet, 2_000));

        assertThat(carolResponse.getStatusCode())
                .as("key theo (tenant, key) thì Carol nhận 422/404 chỉ vì Alice tình cờ dùng cùng chuỗi")
                .isEqualTo(HttpStatus.ACCEPTED);
        assertThat(data(carolResponse).get("transactionId").asText())
                .isNotEqualTo(data(aliceResponse).get("transactionId").asText());
        assertThat(postJournalCommands()).isEqualTo(2);
    }

    @Test
    @DisplayName("Key quá hạn được coi như key mới")
    void key_qua_han_la_key_moi() {
        Map<String, Object> body = TransferApi.body(aliceWallet, bobWallet, 5_000);
        ResponseEntity<String> first = TransferApi.transfer(http, TENANT, alice, "old-key", body);
        jdbcTemplate.update("UPDATE idempotency_keys SET expires_at = now() - interval '1 second'");

        ResponseEntity<String> afterExpiry = TransferApi.transfer(http, TENANT, alice, "old-key", body);

        assertThat(afterExpiry.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(afterExpiry.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("false");
        assertThat(data(afterExpiry).get("transactionId").asText())
                .isNotEqualTo(data(first).get("transactionId").asText());
        assertThat(postJournalCommands()).isEqualTo(2);
    }

    @Test
    @DisplayName("Gửi lại KHÔNG kiểm tra lại ví: lệnh đã nhận vẫn là đã nhận, kể cả khi ví vừa bị đóng băng")
    void gui_lai_khong_kiem_tra_lai() {
        Map<String, Object> body = TransferApi.body(aliceWallet, bobWallet, 5_000);
        ResponseEntity<String> first = TransferApi.transfer(http, TENANT, alice, "key-3", body);
        int lookupsAfterFirst = wallets.lookups();

        wallets.setStatus(TENANT, aliceWallet, EWalletStatus.FROZEN);
        ResponseEntity<String> again = TransferApi.transfer(http, TENANT, alice, "key-3", body);

        assertThat(again.getStatusCode())
                .as("trả lỗi ở đây khiến client tưởng lệnh thất bại trong khi tiền có thể đang đi")
                .isEqualTo(HttpStatus.ACCEPTED);
        assertThat(again.getBody()).isEqualTo(first.getBody());
        assertThat(wallets.lookups()).as("không hỏi wallet-service lần nào nữa").isEqualTo(lookupsAfterFirst);
    }

    @Test
    @DisplayName("⭐ Bản trùng được nhận trong lúc request đang kiểm tra ví, rồi kiểm tra lỗi: trả 202 của bản kia, KHÔNG trả lỗi")
    void kiem_tra_loi_nhung_ban_trung_da_duoc_nhan() {
        Map<String, Object> body = TransferApi.body(aliceWallet, bobWallet, 30_000);
        AtomicReference<ResponseEntity<String>> winner = new AtomicReference<>();

        // Đúng lúc request B bắt đầu kiểm tra ví: request A (cùng key) đi trọn và được nhận, rồi
        // wallet-service sập với B — kịch bản thật: client timeout vì wallet-service chậm, gửi lại.
        wallets.beforeNextLookup(() -> {
            winner.set(TransferApi.transfer(http, TENANT, alice, "race-key", body));
            wallets.goDown();
        });

        ResponseEntity<String> late = TransferApi.transfer(http, TENANT, alice, "race-key", body);

        assertThat(winner.get().getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(late.getStatusCode())
                .as("trả 503 ở đây: client tin lệnh thất bại, gửi lại với key MỚI -> trừ tiền hai lần")
                .isEqualTo(HttpStatus.ACCEPTED);
        assertThat(late.getBody()).isEqualTo(winner.get().getBody());
        assertThat(late.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(postJournalCommands()).isEqualTo(1);
    }

    @Test
    @DisplayName("Thiếu hoặc sai định dạng Idempotency-Key: 400, không nhận lệnh")
    void thieu_key_bi_tu_choi() {
        Map<String, Object> body = TransferApi.body(aliceWallet, bobWallet, 5_000);

        ResponseEntity<String> missing = TransferApi.transfer(http, TENANT, alice, null, body);
        ResponseEntity<String> tooLong = TransferApi.transfer(http, TENANT, alice, "k".repeat(65), body);
        ResponseEntity<String> badChars = TransferApi.transfer(http, TENANT, alice, "has space", body);

        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(problem(missing).get("errorCode").asText()).isEqualTo("IDEMPOTENCY_KEY_REQUIRED");
        assertThat(problem(tooLong).get("errorCode").asText()).isEqualTo("IDEMPOTENCY_KEY_INVALID");
        assertThat(problem(badChars).get("errorCode").asText()).isEqualTo("IDEMPOTENCY_KEY_INVALID");
        assertThat(postJournalCommands()).isZero();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private JsonNode data(ResponseEntity<String> response) {
        try {
            return objectMapper.readTree(response.getBody()).get("data");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private JsonNode problem(ResponseEntity<String> response) {
        try {
            return objectMapper.readTree(response.getBody());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private List<String> commands(String topic) {
        return jdbcTemplate.queryForList("SELECT payload FROM outbox_event WHERE topic = ?", String.class, topic);
    }

    private long postJournalCommands() {
        Long value = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE topic = 'txn.command.post-journal.v1'", Long.class);
        return value == null ? 0 : value;
    }

    private long count(String table) {
        Long value = jdbcTemplate.queryForObject("SELECT count(*) FROM " + table, Long.class);
        return value == null ? 0 : value;
    }
}
