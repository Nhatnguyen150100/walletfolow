package com.walletflow.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.reconciliation.ReconciliationBreakView;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.transaction.client.BreakClient.CloseOutcome;
import com.walletflow.transaction.client.EBreakSource;
import com.walletflow.transaction.support.FakeBreakClient;
import com.walletflow.transaction.support.FakeBreakClient.CloseCall;
import com.walletflow.transaction.support.FakeCrossContextConfig;
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
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Màn hình chỗ lệch đối soát của người trực (roadmap 3.3, ADR-025). Ba tính chất, theo thứ tự quan
 * trọng:
 *
 * <ol>
 *   <li><b>"Không có chỗ lệch" KHÁC "không hỏi được".</b> Chỉ câu đầu cho phép người trực đi ngủ.
 *       Một nguồn chết mà hàng đợi trả rỗng là nói dối họ (ADR-021 B1, ADR-025 §1).</li>
 *   <li><b>Audit THỪA chứ không THIẾU.</b> Việc đóng nằm ở DB của service khác nên không có
 *       transaction nào bao được cả hai; chiều hỏng được chọn là "ghi ý định trước, đóng sau"
 *       (ADR-025 §2). Mọi lần thử đóng — kể cả lần thất bại — đều phải có dòng ý định.</li>
 *   <li><b>Vai.</b> AUDITOR chỉ đọc; OPS đóng được mà không cần người thứ hai (ADR-025 §3).</li>
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
// FakeCrossContextConfig: nối FakeBreakClient thay HttpBreakClient, không thì client thật gọi
// localhost:8083/8082/8085 và mỗi test chờ hết timeout.
@Import({FakeWalletDirectoryConfig.class, FakeCrossContextConfig.class})
@Testcontainers
class AdminBreakIT {

    /** Hai tenant ngẫu nhiên, không dùng DEFAULT_TENANT — lý do ở {@code AdminReviewIT}. */
    private static final UUID TENANT = UUID.randomUUID();
    private static final UUID OTHER_TENANT = UUID.randomUUID();
    private static final String QUEUE = "/api/v1/admin/reconciliation/breaks";

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
    FakeBreakClient breaks;
    @Autowired
    ObjectMapper objectMapper;

    private UUID operator;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE resolution_requests, saga_state, transactions, idempotency_keys, outbox_event, audit_events");
        breaks.reset();
        operator = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Hàng đợi
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Gom đủ ba nguồn: sources đủ 3 ô, complete=true, totalOpen đúng tổng")
    void gom_du_ba_nguon_va_dem_dung() {
        UUID l1 = openBreak(EBreakSource.LEDGER, TENANT);
        UUID l2 = openBreak(EBreakSource.LEDGER, TENANT);
        UUID w1 = openBreak(EBreakSource.WALLET, TENANT);
        UUID b1 = openBreak(EBreakSource.BANK, TENANT);
        UUID b2 = openBreak(EBreakSource.BANK, TENANT);
        UUID b3 = openBreak(EBreakSource.BANK, TENANT);

        JsonNode data = ok(get(QUEUE, TENANT, operator, "OPS")).path("data");

        assertThat(data.path("sources")).as("LUÔN đủ ba ô").hasSize(3);
        assertThat(data.path("complete").asBoolean()).isTrue();
        assertThat(data.path("totalOpen").asInt()).isEqualTo(6);
        assertThat(texts(data.path("sources"), "source"))
                .containsExactlyInAnyOrder("ledger", "wallet", "bank");
        assertThat(ids(cell(data, "ledger"))).containsExactlyInAnyOrder(l1.toString(), l2.toString());
        assertThat(ids(cell(data, "wallet"))).containsExactly(w1.toString());
        assertThat(ids(cell(data, "bank")))
                .containsExactlyInAnyOrder(b1.toString(), b2.toString(), b3.toString());
        assertThat(data.path("sources")).allSatisfy(source ->
                assertThat(source.path("available").asBoolean()).isTrue());
    }

    @Test
    @DisplayName("⭐ Một nguồn chết: ô đó available=false + breaks rỗng, complete=false, totalOpen chỉ đếm nguồn còn lại — KHÔNG báo rỗng")
    void mot_nguon_chet_thi_bao_khong_hoi_duoc_chu_khong_bao_rong() {
        openBreak(EBreakSource.LEDGER, TENANT);
        openBreak(EBreakSource.LEDGER, TENANT);
        openBreak(EBreakSource.WALLET, TENANT);
        openBreak(EBreakSource.BANK, TENANT);   // có thật, nhưng bank đang chết nên không thấy được
        breaks.goDown(EBreakSource.BANK);

        ResponseEntity<JsonNode> response = get(QUEUE, TENANT, operator, "ADMIN");

        assertThat(response.getStatusCode())
                .as("một nguồn chết không được kéo cả màn hình chữa cháy xuống").isEqualTo(HttpStatus.OK);
        JsonNode data = response.getBody().path("data");
        assertThat(data.path("sources")).as("vẫn đủ ba ô").hasSize(3);
        JsonNode bank = cell(data, "bank");
        assertThat(bank.path("available").asBoolean())
                .as("⚠ available=true với danh sách rỗng là 'không có gì' — người trực sẽ đi ngủ "
                        + "trong khi bank đang có chỗ lệch")
                .isFalse();
        assertThat(bank.path("breaks")).isEmpty();
        assertThat(data.path("complete").asBoolean()).isFalse();
        assertThat(data.path("totalOpen").asInt())
                .as("chỉ đếm hai nguồn hỏi được").isEqualTo(3);
        assertThat(cell(data, "ledger").path("available").asBoolean()).isTrue();
        assertThat(cell(data, "wallet").path("available").asBoolean()).isTrue();
    }

    @Test
    @DisplayName("Nguồn rỗng thật khác nguồn chết: available=true, breaks rỗng, complete vẫn true")
    void nguon_rong_that_khac_nguon_chet() {
        openBreak(EBreakSource.LEDGER, TENANT);

        JsonNode data = ok(get(QUEUE, TENANT, operator, "OPS")).path("data");

        JsonNode wallet = cell(data, "wallet");
        assertThat(wallet.path("available").asBoolean()).as("đã trả lời 'không có gì'").isTrue();
        assertThat(wallet.path("breaks")).isEmpty();
        assertThat(data.path("complete").asBoolean()).isTrue();
        assertThat(data.path("totalOpen").asInt()).isEqualTo(1);
    }

    @Test
    @DisplayName("Nguồn chết rồi sống lại: lần mở sau complete=true và thấy chỗ lệch — không dính trạng thái cũ")
    void nguon_song_lai_thi_lan_mo_sau_thay_du_lieu() {
        UUID b = openBreak(EBreakSource.BANK, TENANT);
        breaks.goDown(EBreakSource.BANK);
        assertThat(ok(get(QUEUE, TENANT, operator, "OPS")).path("data").path("complete").asBoolean()).isFalse();

        breaks.goUp(EBreakSource.BANK);

        JsonNode data = ok(get(QUEUE, TENANT, operator, "OPS")).path("data");
        assertThat(data.path("complete").asBoolean()).isTrue();
        assertThat(ids(cell(data, "bank"))).containsExactly(b.toString());
    }

    @Test
    @DisplayName("⭐ Hàng đợi chỉ thấy chỗ lệch của tenant mình — header tenant đổi thì tập đổi theo")
    void hang_doi_co_lap_tenant() {
        UUID cuaToi = openBreak(EBreakSource.LEDGER, TENANT);
        UUID cuaHo = openBreak(EBreakSource.LEDGER, OTHER_TENANT);

        assertThat(ids(cell(ok(get(QUEUE, TENANT, operator, "ADMIN")).path("data"), "ledger")))
                .containsExactly(cuaToi.toString());
        assertThat(ids(cell(ok(get(QUEUE, OTHER_TENANT, operator, "ADMIN")).path("data"), "ledger")))
                .containsExactly(cuaHo.toString());
    }

    @Test
    @DisplayName("⭐ Đọc hàng đợi có ghi audit LIST_RECONCILIATION_BREAKS, kèm vai và việc hàng đợi có đủ hay không")
    void doc_hang_doi_co_ghi_audit() throws Exception {
        openBreak(EBreakSource.LEDGER, TENANT);
        breaks.goDown(EBreakSource.WALLET);

        ok(get(QUEUE, TENANT, operator, "AUDITOR"));

        List<Map<String, Object>> audits = jdbcTemplate.queryForList(
                "SELECT action, actor_id, actor_role, tenant_id, detail::text AS detail FROM audit_events");
        assertThat(audits).hasSize(1);
        Map<String, Object> row = audits.get(0);
        assertThat(row.get("action")).isEqualTo("LIST_RECONCILIATION_BREAKS");
        assertThat(row.get("actor_id")).isEqualTo(operator);
        assertThat(row.get("actor_role")).isEqualTo("AUDITOR");
        assertThat(row.get("tenant_id")).isEqualTo(TENANT);
        JsonNode detail = objectMapper.readTree((String) row.get("detail"));
        assertThat(detail.path("complete").asBoolean())
                .as("người kiểm sổ phải biết lúc đó người trực nhìn dữ liệu thiếu").isFalse();
        assertThat(detail.path("totalOpen").asInt()).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Đóng break
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đóng thành công: 200, audit đủ cặp ý định → kết quả cùng target, service sở hữu nhận đúng người đóng + lý do")
    void dong_thanh_cong_ghi_du_hai_dong_audit() throws Exception {
        UUID breakId = openBreak(EBreakSource.BANK, TENANT);
        String target = "bank:" + breakId;

        ResponseEntity<JsonNode> response = close("bank", breakId, TENANT, operator, "OPS",
                Map.of("reason", "Hoàn tay qua portal VNPay, đã đối chiếu"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Map<String, Object>> audits = jdbcTemplate.queryForList(
                "SELECT action, target, actor_id, actor_role, detail::text AS detail "
                        + "FROM audit_events ORDER BY created_at");
        assertThat(audits).extracting(row -> row.get("action"))
                .as("ý định ghi TRƯỚC, kết quả SAU — chiều hỏng của ADR-025 §2")
                .containsExactly("BREAK_CLOSE_REQUESTED", "BREAK_CLOSED");
        assertThat(audits).extracting(row -> row.get("target")).containsOnly(target);
        assertThat(audits).allSatisfy(row -> {
            assertThat(row.get("actor_id")).isEqualTo(operator);
            assertThat(row.get("actor_role")).isEqualTo("OPS");
        });
        assertThat(objectMapper.readTree((String) audits.get(0).get("detail")).path("reason").asText())
                .as("lý do nằm trong dòng ý định — đó là thứ người kiểm toán cần đọc")
                .isEqualTo("Hoàn tay qua portal VNPay, đã đối chiếu");
        assertThat(breaks.closeCalls()).containsExactly(new CloseCall(
                EBreakSource.BANK, breakId, operator, "Hoàn tay qua portal VNPay, đã đối chiếu"));
    }

    @Test
    @DisplayName("⭐ Nguồn trả 'không tìm thấy': 404 nhưng VẪN còn dấu vết ý định + một dòng FAILED — audit THỪA chứ không THIẾU")
    void nguon_tra_khong_tim_thay_thi_404_va_van_co_dau_vet_y_dinh() {
        UUID breakId = UUID.randomUUID();
        breaks.respondToClose(CloseOutcome.NOT_FOUND);

        ResponseEntity<JsonNode> response = close("ledger", breakId, TENANT, operator, "ADMIN",
                Map.of("reason", "thử đóng id không có"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody().path("errorCode").asText()).isNotBlank();
        assertThat(auditActions())
                .as("⚠ thiếu dòng ý định = một lần thử đóng không ai biết; thừa một dòng thì đọc được và sửa được bằng mắt")
                .containsExactly("BREAK_CLOSE_REQUESTED", "BREAK_CLOSE_FAILED");
        assertThat(jdbcTemplate.queryForList("SELECT DISTINCT target FROM audit_events", String.class))
                .containsExactly("ledger:" + breakId);
        assertThat(breaks.closeCalls()).hasSize(1);
    }

    @Test
    @DisplayName("⭐ Nguồn không hỏi được: 503 BREAK_SOURCE_UNAVAILABLE, vẫn có ý định + FAILED — KHÔNG coi là đã đóng")
    void nguon_khong_hoi_duoc_thi_503() {
        UUID breakId = UUID.randomUUID();
        breaks.respondToClose(CloseOutcome.UNAVAILABLE);

        ResponseEntity<JsonNode> response = close("wallet", breakId, TENANT, operator, "OPS",
                Map.of("reason", "đóng khi wallet chết"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().path("errorCode").asText()).isEqualTo("BREAK_SOURCE_UNAVAILABLE");

        // Không assert SỐ LƯỢNG dòng audit ở ca này: `HttpURLConnection` sau lưng TestRestTemplate tự
        // gửi lại POST khi gặp 503 (`sun.net.http.retryPost` mặc định true), nên lời gọi tới được
        // service HAI lần và sinh hai cặp dòng. Đó là hành vi của client trong test, không phải của
        // service — và nó minh hoạ đúng điều ADR-025 §2 đã chọn: audit THỪA thì đọc được, audit
        // THIẾU thì không khôi phục được.
        //
        // Tính chất thật sự cần chứng minh, không phụ thuộc client gửi lại mấy lần:
        List<String> actions = auditActions();
        assertThat(actions)
                .as("đường không-hỏi-được KHÔNG BAO GIỜ được ghi là đã đóng")
                .doesNotContain("BREAK_CLOSED")
                .containsOnly("BREAK_CLOSE_REQUESTED", "BREAK_CLOSE_FAILED");
        assertThat(java.util.Collections.frequency(actions, "BREAK_CLOSE_REQUESTED"))
                .as("mỗi lần thử đóng để lại ĐÚNG một cặp ý định + kết quả")
                .isEqualTo(java.util.Collections.frequency(actions, "BREAK_CLOSE_FAILED"))
                .isEqualTo(breaks.closeCalls().size());
        assertThat(actions.getFirst()).isEqualTo("BREAK_CLOSE_REQUESTED");
    }

    @Test
    @DisplayName("⭐ Thiếu lý do: 400 và KHÔNG gọi sang service sở hữu, KHÔNG có dòng ý định — validate chặn trước hết")
    void thieu_ly_do_thi_400_va_KHONG_goi_sang_service_so_huu() {
        UUID breakId = openBreak(EBreakSource.LEDGER, TENANT);

        ResponseEntity<JsonNode> rong = close("ledger", breakId, TENANT, operator, "OPS", Map.of("reason", ""));
        ResponseEntity<JsonNode> khoangTrang = close("ledger", breakId, TENANT, operator, "OPS", Map.of("reason", "   "));
        ResponseEntity<JsonNode> khongCoKhoa = close("ledger", breakId, TENANT, operator, "OPS", Map.of());

        assertThat(rong.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(rong.getBody().path("errorCode").asText()).isEqualTo("VALIDATION_ERROR");
        assertThat(khoangTrang.getStatusCode()).as("toàn khoảng trắng không phải một lý do")
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(khongCoKhoa.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(breaks.closeCalls())
                .as("⚠ break bị đóng ở service sở hữu mà không có lý do là đúng thứ ADR-025 §3 cấm")
                .isEmpty();
        assertThat(auditActions()).isEmpty();
    }

    @Test
    @DisplayName("Lý do quá 1000 ký tự: 400, không gọi sang service sở hữu")
    void ly_do_qua_dai_thi_400() {
        ResponseEntity<JsonNode> response = close("ledger", UUID.randomUUID(), TENANT, operator, "OPS",
                Map.of("reason", "x".repeat(1001)));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(breaks.closeCalls()).isEmpty();
    }

    @Test
    @DisplayName("Slug nguồn lạ: 400 UNKNOWN_BREAK_SOURCE (không phải 500), không gọi đi đâu")
    void slug_nguon_la_thi_400() {
        ResponseEntity<JsonNode> response = close("khong-ton-tai", UUID.randomUUID(), TENANT, operator, "OPS",
                Map.of("reason", "thử"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().path("errorCode").asText()).isEqualTo("UNKNOWN_BREAK_SOURCE");
        assertThat(breaks.closeCalls())
                .as("slug do người dùng gõ chọn đích của lời gọi mạng — tập đích phải đóng").isEmpty();
    }

    // ------------------------------------------------------------------
    // Vai
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ AUDITOR đọc được hàng đợi nhưng KHÔNG đóng được: 403, có ADMIN_ACCESS_DENIED, không gọi sang service sở hữu")
    void auditor_doc_duoc_hang_doi_nhung_khong_dong_duoc() {
        UUID breakId = openBreak(EBreakSource.LEDGER, TENANT);

        assertThat(get(QUEUE, TENANT, operator, "AUDITOR").getStatusCode()).isEqualTo(HttpStatus.OK);
        ResponseEntity<JsonNode> response = close("ledger", breakId, TENANT, operator, "AUDITOR",
                Map.of("reason", "kiểm toán viên cố đóng"));

        assertThat(response.getStatusCode())
                .as("gateway chặn theo tiền tố, KHÔNG theo method — chốt chặn là service").isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody().path("errorCode").asText()).isEqualTo("FORBIDDEN");
        assertThat(breaks.closeCalls())
                .as("một vai kiểm toán mà sửa được chính thứ mình kiểm toán thì mất ý nghĩa").isEmpty();
        assertThat(auditActions())
                .containsExactly("LIST_RECONCILIATION_BREAKS", "ADMIN_ACCESS_DENIED");
        assertThat(auditActions()).doesNotContain("BREAK_CLOSE_REQUESTED");
    }

    @Test
    @DisplayName("Khách hàng (USER) không đóng được và cũng không đọc được hàng đợi: 403, không gọi đi đâu")
    void khach_hang_khong_vao_duoc() {
        UUID breakId = openBreak(EBreakSource.BANK, TENANT);

        ResponseEntity<JsonNode> doc = get(QUEUE, TENANT, operator, "USER");
        ResponseEntity<JsonNode> dong = close("bank", breakId, TENANT, operator, "USER", Map.of("reason", "thử"));

        assertThat(doc.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(doc.getBody().has("data")).as("không rò dòng nào của hàng đợi").isFalse();
        assertThat(dong.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(breaks.closeCalls()).isEmpty();
    }

    @Test
    @DisplayName("OPS đóng được — khác /resolve vốn đòi ADMIN duyệt: đóng break không tạo chuyển động tiền nên không cần 4-eyes")
    void ops_dong_duoc() {
        UUID breakId = openBreak(EBreakSource.WALLET, TENANT);

        ResponseEntity<JsonNode> response = close("wallet", breakId, TENANT, operator, "OPS",
                Map.of("reason", "Đã rebuild read model, nguyên nhân là bản deploy cũ"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(breaks.closeCalls()).hasSize(1);
        assertThat(auditActions()).containsExactly("BREAK_CLOSE_REQUESTED", "BREAK_CLOSED");
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM resolution_requests", Integer.class))
                .as("không đi đường 4-eyes").isZero();
    }

    @Test
    @DisplayName("Thiếu header X-User-Id (gọi thẳng service, bỏ qua gateway): 401 cho cả đọc lẫn đóng, không gọi đi đâu")
    void thieu_dinh_danh_thi_401() {
        assertThat(get(QUEUE, TENANT, null, "ADMIN").getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(close("ledger", UUID.randomUUID(), TENANT, null, "ADMIN", Map.of("reason", "thử")).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(breaks.closeCalls()).isEmpty();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private UUID openBreak(EBreakSource source, UUID tenant) {
        UUID id = UUID.randomUUID();
        breaks.put(source, new ReconciliationBreakView(id, tenant, "LEDGER_IMBALANCE", "subject-" + id,
                ECurrency.VND, 0L, 1_000L, 1_000L, false, 1, "{}", Instant.now(), Instant.now()));
        return id;
    }

    private List<String> auditActions() {
        return jdbcTemplate.queryForList("SELECT action FROM audit_events ORDER BY created_at", String.class);
    }

    private HttpHeaders identity(UUID tenant, UUID actor, String role) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(AuthHeaders.TENANT_ID, tenant.toString());
        if (actor != null) {
            headers.set(AuthHeaders.USER_ID, actor.toString());
            headers.set(AuthHeaders.USER_EMAIL, "ops@walletflow.dev");
        }
        headers.set(AuthHeaders.USER_ROLE, role);
        return headers;
    }

    private ResponseEntity<JsonNode> get(String path, UUID tenant, UUID actor, String role) {
        return http.exchange(path, HttpMethod.GET, new HttpEntity<>(identity(tenant, actor, role)), JsonNode.class);
    }

    private ResponseEntity<JsonNode> close(String source, UUID breakId, UUID tenant, UUID actor, String role,
                                           Map<String, String> body) {
        return http.exchange(QUEUE + "/" + source + "/" + breakId + "/close", HttpMethod.POST,
                new HttpEntity<>(body, identity(tenant, actor, role)), JsonNode.class);
    }

    private static JsonNode ok(ResponseEntity<JsonNode> response) {
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    /** Ô của một nguồn trong hàng đợi, tìm theo slug. */
    private static JsonNode cell(JsonNode data, String slug) {
        for (JsonNode source : data.path("sources")) {
            if (slug.equals(source.path("source").asText())) {
                return source;
            }
        }
        throw new AssertionError("Hàng đợi thiếu ô " + slug);
    }

    private static List<String> ids(JsonNode cell) {
        return texts(cell.path("breaks"), "id");
    }

    private static List<String> texts(JsonNode array, String field) {
        List<String> out = new ArrayList<>();
        array.forEach(n -> out.add(n.path(field).asText()));
        return out;
    }
}
