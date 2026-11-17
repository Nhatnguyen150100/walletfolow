package com.walletflow.wallet;

import com.fasterxml.jackson.databind.JsonNode;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.reconciliation.ReconciliationBreakView;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.wallet.services.BreakAdminService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code GET/POST /internal/reconciliation/breaks} của read model ví (roadmap 3.3, ADR-025).
 *
 * <p>Việc đóng break không chuyển một đồng nào, nên cái cần chứng minh không phải "tiền đúng" mà là
 * ba tính chất về QUYỀN và DẤU VẾT:
 * <ol>
 *   <li><b>Cô lập tenant ở đường GHI</b> (ADR-025 §4). {@code close()} từng chỉ lọc theo {@code id};
 *       "UUID không đoán được" chưa bao giờ là mô hình bảo mật của ADR-010. Tenant sai phải ra 404 và
 *       hàng phải còn NGUYÊN — kể cả {@code closed_by}.</li>
 *   <li><b>Dấu vết đủ và không bị ghi đè</b> — ai đóng (từ header, không từ body), vì sao, lúc nào; gọi
 *       lại không được cướp tên người đóng lần đầu.</li>
 *   <li><b>Lý do bắt buộc</b> (§3) — {@code ck_recon_break_closed} ở DB chưa đòi {@code closed_note},
 *       nên tầng app là chốt duy nhất.</li>
 * </ol>
 *
 * <p>Break được chèn thẳng bằng SQL thay vì chạy cả job đối soát: nhanh hơn và tất định, và job không
 * phải là đối tượng kiểm ở đây (xem {@code WalletReconciliationIT}). Đường này chỉ đọc/ghi DB nên không cần
 * Kafka.
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
                // @Scheduled không được chen vào: test tự dựng dữ liệu break
                "walletflow.wallet.reconciliation.initial-delay=PT24H",
                "walletflow.wallet.pending-resend.enabled=false"
        })
@Testcontainers
class InternalBreakAdminIT {

    private static final String BREAKS = "/internal/reconciliation/breaks";
    private static final String TYPE = "PROJECTION_SELF_MISMATCH";

    @LocalServerPort
    int port;

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
    JdbcTemplate jdbcTemplate;

    UUID runId;
    UUID tenantA;
    UUID tenantB;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE reconciliation_breaks, reconciliation_runs");
        runId = UUID.randomUUID();
        // run_id có FK tới reconciliation_runs, nên cần một hàng run thật
        jdbcTemplate.update("""
                INSERT INTO reconciliation_runs (id, kind, window_start, status, finished_at)
                VALUES (?, 'WALLET_PROJECTION', now(), 'SUCCEEDED', now())
                """, runId);
        tenantA = UUID.randomUUID();
        tenantB = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // 1 — Hàng đợi: chỉ OPEN, cũ nhất trước, map đủ trường
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Hàng đợi chỉ có break OPEN, cũ nhất (first_seen_at) trước, và mọi trường map đúng")
    void tra_ve_break_dang_mo_cu_nhat_truoc() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        // Chèn lộn thứ tự để thứ tự trả về không thể là thứ tự chèn
        UUID newest = insertBreak(tenantA, "NEWEST", 0, 300, now.minus(1, ChronoUnit.HOURS));
        UUID oldest = insertOpen(tenantA, "OLDEST", 0, 100, now.minus(3, ChronoUnit.DAYS),
                now.minus(1, ChronoUnit.MINUTES), 7, true, "lệch từ lâu");
        UUID middle = insertBreak(tenantA, "MIDDLE", 0, 200, now.minus(2, ChronoUnit.HOURS));
        UUID closed = insertBreak(tenantA, "DONE", 0, 999, now.minus(10, ChronoUnit.DAYS));
        closeInDb(closed);

        List<ReconciliationBreakView> queue = openBreaks(tenantA, "");

        assertThat(queue).extracting(ReconciliationBreakView::id)
                .as("chỉ OPEN, cũ nhất trước; break CLOSED không được lọt vào dù first_seen_at cũ nhất")
                .containsExactly(oldest, middle, newest);
        ReconciliationBreakView first = queue.getFirst();
        assertThat(first.tenantId()).isEqualTo(tenantA);
        assertThat(first.type()).isEqualTo(TYPE);
        assertThat(first.subject()).isEqualTo("OLDEST");
        assertThat(first.currency()).isEqualTo(ECurrency.VND);
        assertThat(first.expectedMinor()).isZero();
        assertThat(first.actualMinor()).isEqualTo(100);
        assertThat(first.deltaMinor()).as("delta = actual - expected").isEqualTo(100);
        assertThat(first.timesSeen()).isEqualTo(7);
        assertThat(first.autoRepaired()).isTrue();
        assertThat(first.detail()).isEqualTo("lệch từ lâu");
        assertThat(first.firstSeenAt()).as("lệch từ bao giờ").isEqualTo(now.minus(3, ChronoUnit.DAYS));
        assertThat(first.detectedAt()).as("lần thấy gần nhất, khác first_seen_at")
                .isEqualTo(now.minus(1, ChronoUnit.MINUTES));
    }

    // ------------------------------------------------------------------
    // 2 — Cô lập tenant ở đường đọc
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Break của tenant B không xuất hiện khi hỏi bằng tenant A (và ngược lại)")
    void khong_thay_break_cua_tenant_khac() {
        UUID ofA = insertBreak(tenantA, "A", 0, 10, Instant.now());
        UUID ofB = insertBreak(tenantB, "B", 0, 20, Instant.now());

        assertThat(openBreaks(tenantA, "")).extracting(ReconciliationBreakView::id).containsExactly(ofA);
        assertThat(openBreaks(tenantB, "")).extracting(ReconciliationBreakView::id).containsExactly(ofB);
        assertThat(openBreaks(UUID.randomUUID(), ""))
                .as("tenant không có break nào thì hàng đợi rỗng, không phải lỗi")
                .isEmpty();
    }

    // ------------------------------------------------------------------
    // 3 — Đóng: dấu vết đầy đủ
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đóng break: 204, hàng thành CLOSED với đúng người đóng (từ header), lý do và thời điểm")
    void dong_break_thi_ghi_du_dau_vet() {
        UUID id = insertBreak(tenantA, "VND", 0, 500, Instant.now());
        UUID operator = UUID.randomUUID();

        Response response = close(tenantA, operator, id, "đã đối chiếu với sao kê, đảo bút toán tay");

        assertThat(response.status()).isEqualTo(204);
        Map<String, Object> row = row(id);
        assertThat(row.get("status")).isEqualTo("CLOSED");
        assertThat(row.get("closed_by")).as("người đóng lấy từ X-User-Id").isEqualTo(operator);
        assertThat(row.get("closed_note")).isEqualTo("đã đối chiếu với sao kê, đảo bút toán tay");
        assertThat(row.get("closed_at")).isNotNull();
        assertThat(openBreaks(tenantA, ""))
                .as("đóng rồi thì rời hàng đợi")
                .isEmpty();
    }

    // ------------------------------------------------------------------
    // 4 — ⭐ Tenant sai ở đường GHI (ADR-025 §4)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đóng bằng tenant khác: 404 và hàng vẫn OPEN nguyên vẹn (closed_by, closed_at, closed_note đều NULL)")
    void dong_bang_tenant_khac_thi_khong_doi_duoc_gi() {
        UUID id = insertBreak(tenantA, "VND", 0, 500, Instant.now());
        UUID attacker = UUID.randomUUID();

        Response asOther = close(tenantB, attacker, id, "thử đóng break của người khác");
        Response asUnknown = close(tenantB, attacker, UUID.randomUUID(), "id không tồn tại");

        assertThat(asOther.status())
                .as("biết id break của tenant khác không đủ để đóng nó")
                .isEqualTo(404);
        assertThat(asOther.status())
                .as("'của người khác' và 'không tồn tại' trả CÙNG một lỗi — chống dò id")
                .isEqualTo(asUnknown.status());
        assertThat(asOther.body().path("errorCode").asText())
                .isEqualTo(asUnknown.body().path("errorCode").asText());
        Map<String, Object> row = row(id);
        assertThat(row.get("status")).isEqualTo("OPEN");
        assertThat(row.get("closed_by")).isNull();
        assertThat(row.get("closed_at")).isNull();
        assertThat(row.get("closed_note")).isNull();
        assertThat(openBreaks(tenantA, "")).extracting(ReconciliationBreakView::id)
                .as("tenant chủ sở hữu vẫn thấy break trong hàng đợi")
                .containsExactly(id);
    }

    // ------------------------------------------------------------------
    // 5 — Đóng lại không ghi đè
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Đóng lại lần hai: 404, closed_by/closed_note/closed_at của lần đầu KHÔNG đổi")
    void dong_lai_lan_hai_khong_ghi_de_nguoi_dong_dau() {
        UUID id = insertBreak(tenantA, "VND", 0, 500, Instant.now());
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        assertThat(close(tenantA, first, id, "lý do lần đầu").status()).isEqualTo(204);
        Map<String, Object> afterFirst = row(id);

        Response again = close(tenantA, second, id, "người thứ hai cố ghi đè");

        assertThat(again.status()).as("không còn break ĐANG MỞ nào khớp").isEqualTo(404);
        Map<String, Object> row = row(id);
        assertThat(row.get("closed_by")).isEqualTo(first);
        assertThat(row.get("closed_note")).isEqualTo("lý do lần đầu");
        assertThat(row.get("closed_at")).isEqualTo(afterFirst.get("closed_at"));
    }

    // ------------------------------------------------------------------
    // 6 — Lý do bắt buộc
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Thiếu lý do (rỗng, toàn khoảng trắng, hoặc không có note): 400 MISSING_CLOSE_REASON, hàng vẫn OPEN")
    void thieu_ly_do_thi_bi_tu_choi() {
        UUID id = insertBreak(tenantA, "VND", 0, 500, Instant.now());
        UUID operator = UUID.randomUUID();

        for (Object body : List.of(Map.of("note", ""), Map.of("note", "   "), Map.of("note", " \t\n "),
                Map.of())) {
            Response response = closeWithBody(tenantA, operator, id, body);

            assertThat(response.status()).as("body %s", body).isEqualTo(400);
            assertThat(response.body().path("errorCode").asText()).as("body %s", body)
                    .isEqualTo("MISSING_CLOSE_REASON");
        }
        Map<String, Object> row = row(id);
        assertThat(row.get("status")).isEqualTo("OPEN");
        assertThat(row.get("closed_by")).isNull();
        assertThat(row.get("closed_note")).isNull();
    }

    @Test
    @DisplayName("Lý do dài 1001 ký tự: 400 CLOSE_REASON_TOO_LONG, hàng vẫn OPEN; đúng 1000 ký tự thì được")
    void ly_do_qua_dai_thi_bi_tu_choi() {
        UUID id = insertBreak(tenantA, "VND", 0, 500, Instant.now());
        UUID operator = UUID.randomUUID();

        Response tooLong = close(tenantA, operator, id, "x".repeat(1001));

        assertThat(tooLong.status()).isEqualTo(400);
        assertThat(tooLong.body().path("errorCode").asText()).isEqualTo("CLOSE_REASON_TOO_LONG");
        assertThat(row(id).get("status")).isEqualTo("OPEN");

        Response atLimit = close(tenantA, operator, id, "x".repeat(1000));

        assertThat(atLimit.status()).as("đúng trần thì không bị chặn").isEqualTo(204);
        assertThat(row(id).get("status")).isEqualTo("CLOSED");
    }

    // ------------------------------------------------------------------
    // 7 — Trần limit
    // ------------------------------------------------------------------
    @Test
    @DisplayName("limit bị kẹp trong [1, 200]: 0 và số âm lên 1, 9999 xuống 200, không nổ; mặc định 50")
    void limit_bi_kep_trong_khoang() {
        Instant base = Instant.now().minus(30, ChronoUnit.DAYS).truncatedTo(ChronoUnit.MICROS);
        List<UUID> byAge = new ArrayList<>();
        for (int i = 0; i < BreakAdminService.MAX_LIMIT + 5; i++) {
            byAge.add(insertBreak(tenantA, "S" + i, 0, i + 1, base.plusSeconds(i)));
        }

        List<ReconciliationBreakView> huge = openBreaks(tenantA, "?limit=9999");
        List<ReconciliationBreakView> zero = openBreaks(tenantA, "?limit=0");
        List<ReconciliationBreakView> negative = openBreaks(tenantA, "?limit=-5");
        List<ReconciliationBreakView> byDefault = openBreaks(tenantA, "");

        assertThat(BreakAdminService.MAX_LIMIT).isEqualTo(200);
        assertThat(huge).as("trần bộ nhớ của một lần đọc").hasSize(BreakAdminService.MAX_LIMIT);
        assertThat(huge).extracting(ReconciliationBreakView::id)
                .as("kẹp rồi vẫn là 200 break CŨ NHẤT")
                .containsExactlyElementsOf(byAge.subList(0, BreakAdminService.MAX_LIMIT));
        assertThat(zero).hasSize(1);
        assertThat(zero.getFirst().id()).as("limit 0 → 1, và là break cũ nhất").isEqualTo(byAge.getFirst());
        assertThat(negative).hasSize(1);
        assertThat(byDefault).hasSize(50);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private record Response(int status, JsonNode body) {
    }

    private List<ReconciliationBreakView> openBreaks(UUID tenant, String query) {
        List<ReconciliationBreakView> body = client().get()
                .uri(BREAKS + query)
                .header(AuthHeaders.TENANT_ID, tenant.toString())
                .retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
        assertThat(body).as("GET %s phải trả 200 kèm một mảng", BREAKS + query).isNotNull();
        return body;
    }

    private Response close(UUID tenant, UUID operator, UUID breakId, String note) {
        return closeWithBody(tenant, operator, breakId, Map.of("note", note));
    }

    private Response closeWithBody(UUID tenant, UUID operator, UUID breakId, Object body) {
        return client().post()
                .uri(BREAKS + "/{id}/close", breakId)
                .header(AuthHeaders.TENANT_ID, tenant.toString())
                .header(AuthHeaders.USER_ID, operator.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .exchange((request, response) ->
                        new Response(response.getStatusCode().value(), response.bodyTo(JsonNode.class)));
    }

    private RestClient client() {
        return RestClient.create("http://localhost:" + port);
    }

    private Map<String, Object> row(UUID id) {
        return jdbcTemplate.queryForMap("SELECT * FROM reconciliation_breaks WHERE id = ?", id);
    }

    private UUID insertBreak(UUID tenant, String subject, long expected, long actual, Instant firstSeenAt) {
        return insertOpen(tenant, subject, expected, actual, firstSeenAt, firstSeenAt, 1, false, null);
    }

    /** Chèn một break OPEN; {@code delta_minor = actual - expected} vì {@code ck_recon_break_delta}. */
    private UUID insertOpen(UUID tenant, String subject, long expected, long actual, Instant firstSeenAt,
                            Instant detectedAt, int timesSeen, boolean autoRepaired, String detail) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                        INSERT INTO reconciliation_breaks
                            (id, run_id, tenant_id, type, subject, currency, expected_minor, actual_minor,
                             delta_minor, status, auto_repaired, times_seen, detail, first_seen_at, detected_at)
                        VALUES (?, ?, ?, ?, ?, 'VND', ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?)
                        """,
                id, runId, tenant, TYPE, subject, expected, actual, actual - expected, autoRepaired, timesSeen,
                detail, Timestamp.from(firstSeenAt), Timestamp.from(detectedAt));
        return id;
    }

    /** Đóng bằng SQL — dựng sẵn một break đã có người kết luận, không đi qua API đang được kiểm. */
    private void closeInDb(UUID id) {
        jdbcTemplate.update("""
                UPDATE reconciliation_breaks
                   SET status = 'CLOSED', closed_at = now(), closed_by = gen_random_uuid(), closed_note = 'cũ'
                 WHERE id = ?
                """, id);
    }
}
