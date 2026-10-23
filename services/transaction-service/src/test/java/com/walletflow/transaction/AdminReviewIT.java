package com.walletflow.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.transaction.repositories.TransactionRepository;
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
import org.springframework.data.jpa.repository.Query;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Khu vực vận hành (roadmap 2.5 PR1, ADR-021). Bốn tính chất, theo thứ tự quan trọng:
 *
 * <ol>
 *   <li><b>Chỉ vai vận hành vào được.</b> Khách hàng đi lạc vào đây phải nhận 403, không phải một
 *       danh sách giao dịch của người khác.</li>
 *   <li><b>Tenant vẫn là ranh giới.</b> Người vận hành của tenant này không thấy ca của tenant kia —
 *       vai vận hành nới quyền trong NỘI BỘ một tenant, không phải xuyên tenant (ADR-010).</li>
 *   <li><b>Không thao tác nào không để lại dấu vết</b>, kể cả thao tác đọc — và dấu vết đó không
 *       sửa được.</li>
 *   <li>Hàng đợi trả đúng tập ca cần người, cũ nhất trước, kèm đủ ngữ cảnh để phân loại.</li>
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
@Import(FakeWalletDirectoryConfig.class)
@Testcontainers
class AdminReviewIT {

    /**
     * KHÔNG dùng {@code TenantContext.DEFAULT_TENANT}: đó đúng là giá trị mà một code hỏng sẽ rơi
     * vào (quên lọc tenant, hay {@code TenantFilter} bỏ qua header), nên test sẽ vẫn xanh trong khi
     * bất biến tenant đã mất. Hai tenant ngẫu nhiên thì tenant chỉ có thể đến TỪ HEADER.
     */
    private static final UUID TENANT = UUID.randomUUID();
    private static final UUID OTHER_TENANT = UUID.randomUUID();
    private static final String QUEUE = "/api/v1/admin/transactions/manual-review";

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

    private UUID operator;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE saga_state, transactions, idempotency_keys, outbox_event, audit_events");
        wallets.reset();
        operator = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Ai vào được
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Khách hàng (USER) gọi khu vực vận hành: 403, KHÔNG trả dữ liệu của ai cả")
    void khach_hang_khong_vao_duoc() {
        needsReview(TENANT, "ACCOUNT_CLOSED", 500_000);

        ResponseEntity<JsonNode> response = call(QUEUE, TENANT, operator, "USER");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody().path("errorCode").asText()).isEqualTo("FORBIDDEN");
        assertThat(response.getBody().has("data"))
                .as("⚠ rò một dòng nào của hàng đợi ở đây là lộ giao dịch của khách hàng khác")
                .isFalse();
    }

    @Test
    @DisplayName("Thiếu header danh tính (gọi thẳng service, không qua gateway): 401")
    void thieu_danh_tinh_thi_401() {
        assertThat(call(QUEUE, TENANT, null, "ADMIN").getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("Cả ba vai vận hành đều XEM được — AUDITOR cũng vậy, vai chỉ-đọc vẫn phải đọc được")
    void ba_vai_van_hanh_deu_xem_duoc() {
        needsReview(TENANT, "ACCOUNT_CLOSED", 500_000);

        for (String role : List.of("ADMIN", "OPS", "AUDITOR")) {
            ResponseEntity<JsonNode> response = call(QUEUE, TENANT, operator, role);
            assertThat(response.getStatusCode()).as(role).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody().path("data")).as(role).hasSize(1);
        }
    }

    // ------------------------------------------------------------------
    // Tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Người vận hành KHÔNG thấy ca của tenant khác — vai nới quyền trong tenant, không xuyên tenant")
    void khong_thay_ca_cua_tenant_khac() {
        UUID mine = needsReview(TENANT, "ACCOUNT_CLOSED", 500_000);
        UUID theirs = needsReview(OTHER_TENANT, "ACCOUNT_CLOSED", 900_000);

        JsonNode data = ok(call(QUEUE, TENANT, operator, "ADMIN")).path("data");

        assertThat(ids(data)).containsExactly(mine.toString()).doesNotContain(theirs.toString());
        assertThat(call("/api/v1/admin/transactions/" + theirs, TENANT, operator, "ADMIN").getStatusCode())
                .as("đọc thẳng theo id cũng không qua được — và 404 chứ không 403, để không thành máy dò id")
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("⭐ Tenant đến TỪ HEADER: cùng một người gọi, đổi header thì thấy đúng ca của tenant đó")
    void tenant_den_tu_header_khong_phai_tu_dau_khac() {
        UUID a = needsReview(TENANT, "ACCOUNT_CLOSED", 500_000);
        UUID b = needsReview(OTHER_TENANT, "BANK_NO_RESPONSE", 900_000);

        // CÙNG operator, chỉ khác X-Tenant-Id. Nếu tenant bị lấy từ bất kỳ đâu khác (hằng mặc định,
        // người dùng, cấu hình) thì một trong hai lời gọi sẽ trả sai tập — đây là thứ mà việc dùng
        // DEFAULT_TENANT trong test che mất.
        assertThat(ids(ok(call(QUEUE, TENANT, operator, "ADMIN")).path("data")))
                .containsExactly(a.toString());
        assertThat(ids(ok(call(QUEUE, OTHER_TENANT, operator, "ADMIN")).path("data")))
                .containsExactly(b.toString());
    }

    @Test
    @DisplayName("Thiếu X-Tenant-Id: KHÔNG trả dữ liệu của tenant nào cả")
    void thieu_tenant_thi_khong_tra_du_lieu() {
        needsReview(TENANT, "ACCOUNT_CLOSED", 500_000);

        HttpHeaders headers = new HttpHeaders();
        headers.set(AuthHeaders.USER_ID, operator.toString());
        headers.set(AuthHeaders.USER_ROLE, "ADMIN");
        ResponseEntity<JsonNode> response =
                http.exchange(QUEUE, HttpMethod.GET, new HttpEntity<>(headers), JsonNode.class);

        assertThat(response.getStatusCode().is2xxSuccessful())
                .as("không có tenant thì không có sổ nào để mở — đoán một tenant là mở sổ của người lạ")
                .isFalse();
    }

    // ------------------------------------------------------------------
    // Hàng đợi
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Hàng đợi: đúng tập trạng thái cần người, CŨ NHẤT TRƯỚC, kèm nguyên nhân gốc")
    void hang_doi_dung_tap_va_dung_thu_tu() {
        UUID cu = needsReview(TENANT, "BANK_NO_RESPONSE", 100_000);
        UUID moi = needsReview(TENANT, "ACCOUNT_CLOSED", 200_000);
        UUID dangHoan = refunding(TENANT, "ACCOUNT_CLOSED", 300_000);
        jdbcTemplate.update("UPDATE transactions SET created_at = now() - interval '2 hours' WHERE id = ?", cu);
        jdbcTemplate.update("UPDATE transactions SET created_at = now() - interval '1 hour' WHERE id = ?", moi);
        completed(TENANT, 400_000);

        JsonNode data = ok(call(QUEUE, TENANT, operator, "OPS")).path("data");

        assertThat(ids(data))
                .as("cũ nhất trước: ở đây mỗi phút là một phút khách mất tiền mà không có gì. "
                        + "COMPLETED không được lọt vào — người trực sẽ mất thời gian cho ca không cần ai")
                .containsExactly(cu.toString(), moi.toString(), dangHoan.toString());
        JsonNode first = data.get(0);
        assertThat(first.path("failureCode").asText()).isEqualTo("BANK_NO_RESPONSE");
        assertThat(first.path("failureReason").asText())
                .as("mã bị ghi đè qua các bước, nên nguyên nhân GỐC chỉ còn ở phần lời — thiếu nó là "
                        + "người trực phân loại sai mục runbook")
                .contains("vì sao");
        assertThat(first.path("amountMinor").asLong()).isEqualTo(100_000L);
        assertThat(first.path("stuckForSeconds").asLong()).isNotNegative();
    }

    @Test
    @DisplayName("Chi tiết một ca kèm trạng thái saga — đủ để phân loại mà không phải mở psql")
    void chi_tiet_kem_trang_thai_saga() {
        UUID txn = needsReview(TENANT, "REFUND_NO_RESPONSE", 700_000);
        jdbcTemplate.update("""
                INSERT INTO saga_state (transaction_id, tenant_id, current_step, step_status, attempt_count,
                                        command_event_id, deadline_at, updated_at)
                VALUES (?, ?, 'BANK_REFUND', 'MANUAL_REVIEW', 3, ?, now(), now())
                """, txn, TENANT, UUID.randomUUID());

        JsonNode data = ok(call("/api/v1/admin/transactions/" + txn, TENANT, operator, "OPS")).path("data");

        assertThat(data.path("transaction").path("transactionId").asText()).isEqualTo(txn.toString());
        assertThat(data.path("saga").path("currentStep").asText()).isEqualTo("BANK_REFUND");
        assertThat(data.path("saga").path("attemptCount").asInt()).isEqualTo(3);
    }

    @Test
    @DisplayName("limit bị kẹp vào trần — một client hỏng không kéo được cả bảng về")
    void limit_bi_kep_vao_tran() {
        for (int i = 0; i < 3; i++) {
            needsReview(TENANT, "ACCOUNT_CLOSED", 10_000 + i);
        }

        assertThat(ok(call(QUEUE + "?limit=1", TENANT, operator, "ADMIN")).path("data")).hasSize(1);
        assertThat(ok(call(QUEUE + "?limit=0", TENANT, operator, "ADMIN")).path("data"))
                .as("0 và số âm bị kẹp lên 1, không phải trả rỗng hay nổ").hasSize(1);
        assertThat(ok(call(QUEUE + "?limit=99999", TENANT, operator, "ADMIN")).path("data")).hasSize(3);
    }

    @Test
    @DisplayName("⭐ Hàng đợi DÙNG ĐƯỢC index riêng phần — không quét toàn bảng transactions")
    void hang_doi_dung_duoc_index_rieng_phan() throws Exception {
        needsReview(TENANT, "ACCOUNT_CLOSED", 500_000);
        // Hai thứ phải ép, nếu không test sẽ XANH BẬY:
        //   * `force_generic_plan` — đây mới là ca thật. Ở kế hoạch *custom*, Postgres thế tham số
        //     bằng hằng rồi mới lập kế hoạch, nên index riêng phần vẫn dùng được và một câu lệnh
        //     dùng tham số bind trông vẫn "ổn". Chỉ ở kế hoạch *generic* — thứ pgjdbc tự chuyển sang
        //     sau vài lần gọi trên cùng connection — nó mới lộ ra là KHÔNG DÙNG ĐƯỢC.
        //   * `enable_seqscan = off` — bảng test quá nhỏ để index thắng về chi phí. Tắt rồi hỏi:
        //     còn ĐƯỜNG nào khác không? Index không dùng được thì Seq Scan vẫn là lựa chọn duy nhất.
        // Câu lệnh lấy TỪ CHÍNH repository, không chép tay: chép tay thì ai đổi @Query về dạng tham
        // số bind sẽ làm production quay lại quét bảng trong khi test vẫn xanh.
        String sql = TransactionRepository.class
                .getMethod("findNeedingReview", UUID.class, int.class)
                .getAnnotation(Query.class).value()
                .replace(":tenantId", "$1")
                .replace(":limit", "50");

        // Cả khối trên MỘT connection: `SET`, `PREPARE` và `RESET` là trạng thái của session, nên
        // chúng rơi vào các connection khác nhau thì test vừa sai vừa để lại `enable_seqscan = off`
        // trên một connection của pool cho test sau.
        String plan = jdbcTemplate.execute((ConnectionCallback<String>) con -> {
            try (Statement st = con.createStatement()) {
                // force_generic_plan mới là ca thật: ở kế hoạch *custom*, Postgres thế tham số bằng
                // hằng rồi mới lập kế hoạch, nên index riêng phần vẫn dùng được và một câu lệnh dùng
                // tham số bind trông vẫn "ổn". Chỉ kế hoạch *generic* — thứ pgjdbc tự chuyển sang sau
                // vài lần gọi — mới lộ ra là KHÔNG DÙNG ĐƯỢC. enable_seqscan=off vì bảng test quá nhỏ
                // để index thắng về chi phí: tắt rồi hỏi "còn ĐƯỜNG nào khác không".
                st.execute("SET plan_cache_mode = force_generic_plan");
                st.execute("SET enable_seqscan = off");
                try {
                    st.execute("PREPARE hang_doi(uuid) AS " + sql);
                    StringBuilder out = new StringBuilder();
                    try (ResultSet rs = st.executeQuery("EXPLAIN EXECUTE hang_doi('" + TENANT + "')")) {
                        while (rs.next()) {
                            out.append(rs.getString(1)).append('\n');
                        }
                    }
                    return out.toString();
                } finally {
                    st.execute("DEALLOCATE ALL");
                    st.execute("RESET enable_seqscan");
                    st.execute("RESET plan_cache_mode");
                }
            }
        });

        assertThat(plan)
                .as("mất index ở đây là quét mọi giao dịch của tenant trên đúng đường người trực dùng "
                        + "lúc đang có sự cố — và nó hỏng ÂM THẦM, chỉ chậm dần khi bảng lớn")
                .contains("idx_transactions_needs_review");
    }

    // ------------------------------------------------------------------
    // Dấu vết
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Mọi thao tác để lại dấu vết — kể cả ĐỌC, kèm vai LÚC ĐÓ")
    void moi_thao_tac_deu_de_lai_dau_vet() {
        UUID txn = needsReview(TENANT, "ACCOUNT_CLOSED", 500_000);

        ok(call(QUEUE, TENANT, operator, "OPS"));
        ok(call("/api/v1/admin/transactions/" + txn, TENANT, operator, "AUDITOR"));

        List<Map<String, Object>> audits = jdbcTemplate.queryForList(
                "SELECT * FROM audit_events ORDER BY created_at");
        assertThat(audits).hasSize(2);
        assertThat(audits.get(0).get("action")).isEqualTo("LIST_MANUAL_REVIEW");
        assertThat(audits.get(0).get("actor_role")).isEqualTo("OPS");
        assertThat(audits.get(1).get("action")).isEqualTo("VIEW_TRANSACTION");
        assertThat(audits.get(1).get("target")).isEqualTo(txn.toString());
        assertThat(audits.get(1).get("actor_role"))
                .as("vai đổi theo thời gian — đọc lại từ users về sau là đọc một sự thật khác")
                .isEqualTo("AUDITOR");
        assertThat(audits).allSatisfy(row -> {
            assertThat(row.get("tenant_id")).isEqualTo(TENANT);
            assertThat(row.get("actor_id")).isEqualTo(operator);
        });
    }

    @Test
    @DisplayName("⭐ Thao tác bị TỪ CHỐI cũng để lại dấu vết — chuỗi 403/404 là hình dạng của token bị đánh cắp")
    void thao_tac_bi_tu_choi_cung_de_lai_dau_vet() {
        UUID cuaTenantKhac = needsReview(OTHER_TENANT, "ACCOUNT_CLOSED", 900_000);

        call(QUEUE, TENANT, operator, "USER");                                  // 403 ở cửa
        call("/api/v1/admin/transactions/" + cuaTenantKhac, TENANT, operator, "OPS");  // 404 dò id

        List<Map<String, Object>> audits = jdbcTemplate.queryForList(
                "SELECT action, target, actor_role FROM audit_events ORDER BY created_at");
        assertThat(audits).extracting(row -> row.get("action"))
                .as("lần bị từ chối còn đáng ghi hơn lần thành công — gateway chỉ log.warn, mà log "
                        + "không có tenant_id, không bất biến, và bị xoay vòng")
                .containsExactly("ADMIN_ACCESS_DENIED", "VIEW_TRANSACTION_DENIED");
        assertThat(audits.get(0).get("actor_role")).isEqualTo("USER");
        assertThat(audits.get(1).get("target"))
                .as("ghi id ĐƯỢC HỎI, không phải id tìm thấy — chính nó mới cho thấy người này đang dò gì")
                .isEqualTo(cuaTenantKhac.toString());
    }

    @Test
    @DisplayName("⭐ Dấu vết KHÔNG sửa được — một audit log sửa được thì không trả lời được câu hỏi nó sinh ra để trả lời")
    void dau_vet_khong_sua_duoc() {
        ok(call(QUEUE, TENANT, operator, "ADMIN"));

        // SQL trực tiếp = đường đi tắt duy nhất còn lại sau khi entity đã bỏ setter và repository
        // đã bỏ delete(). Trigger phải chặn được cả đường này (cùng khuôn LedgerServiceIT).
        assertThatThrownBy(() -> jdbcTemplate.update("UPDATE audit_events SET action = 'KHAC'"))
                .hasMessageContaining("APPEND-ONLY");
        assertThatThrownBy(() -> jdbcTemplate.update("DELETE FROM audit_events"))
                .hasMessageContaining("APPEND-ONLY");
    }

    @Test
    @DisplayName("Audit hỏng không chặn việc đang làm — người trực vẫn nhìn được hàng đợi lúc có sự cố")
    void audit_hong_khong_chan_viec_dang_lam() {
        needsReview(TENANT, "ACCOUNT_CLOSED", 500_000);
        // Ép mọi INSERT audit nổ, giống lúc bảng bị khoá hoặc hết chỗ
        jdbcTemplate.execute("""
                CREATE OR REPLACE FUNCTION txn_fail_audit_insert() RETURNS trigger AS
                $$ BEGIN RAISE EXCEPTION 'audit chết'; END; $$ LANGUAGE plpgsql
                """);
        jdbcTemplate.execute("""
                CREATE TRIGGER trg_test_fail_audit BEFORE INSERT ON audit_events
                FOR EACH ROW EXECUTE FUNCTION txn_fail_audit_insert()
                """);
        try {
            ResponseEntity<JsonNode> response = call(QUEUE, TENANT, operator, "OPS");

            assertThat(response.getStatusCode())
                    .as("mất dấu vết của một lần XEM là chuyện đáng log, không phải lý do để người "
                            + "trực không nhìn được hàng đợi (thao tác đụng tiền ở PR2 thì ngược lại)")
                    .isEqualTo(HttpStatus.OK);
            assertThat(response.getBody().path("data")).hasSize(1);
        } finally {
            jdbcTemplate.execute("DROP TRIGGER trg_test_fail_audit ON audit_events");
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private UUID needsReview(UUID tenant, String failureCode, long amount) {
        return insert(tenant, "MANUAL_REVIEW", failureCode, amount);
    }

    private UUID refunding(UUID tenant, String failureCode, long amount) {
        return insert(tenant, "REFUNDING", failureCode, amount);
    }

    private UUID completed(UUID tenant, long amount) {
        return insert(tenant, "COMPLETED", null, amount);
    }

    /**
     * Dựng thẳng bằng SQL: thứ đang kiểm là hàng đợi đọc được gì, không phải đường nào dẫn một giao
     * dịch tới đó — những đường ấy đã có test riêng ở {@code TopupCompensationIT}/{@code StuckSagaIT}.
     */
    private UUID insert(UUID tenant, String status, String failureCode, long amount) {
        UUID id = UUID.randomUUID();
        UUID wallet = wallets.add(tenant, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
        jdbcTemplate.update("""
                INSERT INTO transactions (id, tenant_id, type, status, initiated_by, to_wallet, amount_minor,
                                          currency, failure_code, failure_reason, provider, provider_ref,
                                          created_at, updated_at)
                VALUES (?, ?, 'TOPUP', ?, ?, ?, ?, 'VND', ?, ?, 'mock', ?, now(), now())
                """, id, tenant, status, UUID.randomUUID(), wallet, amount, failureCode,
                failureCode == null ? null : "Nguyên nhân vì sao: " + failureCode, "MOCK-" + id);
        return id;
    }

    private ResponseEntity<JsonNode> call(String path, UUID tenant, UUID actor, String role) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(AuthHeaders.TENANT_ID, tenant.toString());
        if (actor != null) {
            headers.set(AuthHeaders.USER_ID, actor.toString());
            headers.set(AuthHeaders.USER_EMAIL, "ops@walletflow.dev");
        }
        headers.set(AuthHeaders.USER_ROLE, role);
        return http.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), JsonNode.class);
    }

    private static JsonNode ok(ResponseEntity<JsonNode> response) {
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    private static List<String> ids(JsonNode data) {
        return data.findValuesAsText("transactionId");
    }
}
