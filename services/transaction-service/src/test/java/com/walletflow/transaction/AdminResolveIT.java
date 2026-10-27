package com.walletflow.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.transaction.services.AuditWriter;
import com.walletflow.transaction.services.Operator;
import com.walletflow.transaction.services.SagaService;
import com.walletflow.transaction.support.FakeWalletDirectory;
import com.walletflow.transaction.support.FakeWalletDirectoryConfig;
import com.walletflow.transaction.support.SagaFixture;
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
import org.springframework.transaction.IllegalTransactionStateException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Xử lý ca cần can thiệp với 4-eyes (roadmap 2.5 PR2, GAP-A4, ADR-021).
 *
 * <p>Năm tính chất, theo thứ tự quan trọng:
 * <ol>
 *   <li><b>Không ai một mình đụng tiền được.</b> Người duyệt phải khác người đề xuất, và chỉ
 *       {@code ADMIN} mới duyệt được.</li>
 *   <li><b>Không thao tác đụng tiền nào không để lại dấu vết.</b> Audit hỏng ⇒ tiền KHÔNG được ghi
 *       (ngược hẳn với đường đọc ở PR1, nơi audit hỏng không chặn gì).</li>
 *   <li><b>Chốt rồi thì ledger ghi sổ muộn KHÔNG biến ca thành COMPLETED</b> — nếu không, người dùng
 *       vừa được hoàn tay vừa được cộng ví.</li>
 *   <li>Không chốt được một ca saga đã tự xử lý xong trong lúc người vận hành đang gõ.</li>
 *   <li>Vai: {@code AUDITOR} không đề xuất được (gateway chặn theo tiền tố chứ không theo method).</li>
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
class AdminResolveIT {

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
    SagaService sagaService;
    @Autowired
    AuditWriter auditWriter;
    @Autowired
    MeterRegistry meterRegistry;

    private UUID ops;
    private UUID admin;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE resolution_requests, saga_state, transactions, "
                + "idempotency_keys, outbox_event, audit_events CASCADE");
        wallets.reset();
        ops = UUID.randomUUID();
        admin = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // 4-eyes
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đường đủ hai người: OPS đề xuất, ADMIN khác duyệt → ca đóng FAILED, có resolved_at")
    void hai_nguoi_thi_chot_duoc_ca() {
        UUID txn = needsReview("ACCOUNT_CLOSED");

        UUID requestId = propose(txn, ops, "OPS", "Ví đóng vĩnh viễn, đã hoàn qua portal", "VNP-RF-9");
        ResponseEntity<JsonNode> decided = decide(requestId, admin, "ADMIN", true, null);

        assertThat(decided.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> row = txnRow(txn);
        assertThat(row.get("status")).isEqualTo("FAILED");
        assertThat(row.get("failure_code")).isEqualTo("REFUNDED_MANUALLY");
        assertThat((String) row.get("failure_reason")).contains("VNP-RF-9");
        assertThat(row.get("resolved_at"))
                .as("dấu bền: ledger ghi sổ sau thời điểm này là xung đột, không phải 'ledger thắng'")
                .isNotNull();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM resolution_requests WHERE id = ?", String.class, requestId))
                .isEqualTo("APPROVED");
    }

    @Test
    @DisplayName("⭐ Người đề xuất TỰ DUYỆT yêu cầu của mình: 403, tiền không đụng tới")
    void tu_duyet_bi_tu_choi() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, admin, "ADMIN", "Tự duyệt thử", null);

        ResponseEntity<JsonNode> response = decide(requestId, admin, "ADMIN", true, null);

        assertThat(response.getStatusCode())
                .as("4-eyes mà một người làm trọn được thì nó chỉ là một thủ tục, không phải kiểm soát")
                .isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody().path("errorCode").asText()).isEqualTo("FOUR_EYES_VIOLATION");
        assertThat(txnRow(txn).get("status")).isEqualTo("MANUAL_REVIEW");
        assertThat(txnRow(txn).get("resolved_at")).isNull();
    }

    @Test
    @DisplayName("⭐ `resolved_at` là VĨNH VIỄN — một câu SQL tay không gỡ được hàng rào tiền")
    void resolved_at_khong_go_duoc_bang_sql_tay() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        decide(propose(txn, ops, "OPS", "Hoàn tay", null), admin, "ADMIN", true, null);

        // Gỡ dấu này là mở lại đúng cửa hoàn-tiền-hai-lần mà nó sinh ra để đóng. "Sửa bằng SQL tay,
        // không để lại dấu vết" là vấn đề mở đầu của ADR-021 — chốt chặn phải ở DB.
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE transactions SET resolved_at = NULL WHERE id = ?", txn))
                .hasMessageContaining("VĨNH VIỄN");
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE transactions SET resolved_at = now() - interval '1 day' WHERE id = ?", txn))
                .hasMessageContaining("VĨNH VIỄN");
        assertThat(txnRow(txn).get("resolved_at")).isNotNull();
    }

    @Test
    @DisplayName("⭐ DB cũng cưỡng chế 4-eyes — không chỉ tầng app")
    void bon_mat_duoc_cuong_che_o_db() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay", null);

        // Đi vòng qua mọi kiểm tra của Java: chốt chặn cuối phải là ràng buộc ở DB.
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE resolution_requests SET checker_id = maker_id, status = 'APPROVED', "
                        + "decided_at = now() WHERE id = ?", requestId))
                .hasMessageContaining("ck_resolution_four_eyes");
    }

    @Test
    @DisplayName("OPS KHÔNG duyệt được thao tác đụng tiền — nó mở được ca, không tự quyết chuyện tiền")
    void ops_khong_duyet_duoc() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, admin, "ADMIN", "Hoàn tay", null);

        assertThat(decide(requestId, ops, "OPS", true, null).getStatusCode())
                .isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(txnRow(txn).get("status")).isEqualTo("MANUAL_REVIEW");
    }

    @Test
    @DisplayName("⭐ AUDITOR không đề xuất được — gateway chặn theo tiền tố, KHÔNG theo method")
    void auditor_khong_de_xuat_duoc() {
        UUID txn = needsReview("ACCOUNT_CLOSED");

        ResponseEntity<JsonNode> response = http.exchange(
                "/api/v1/admin/transactions/" + txn + "/resolve", HttpMethod.POST,
                new HttpEntity<>(Map.of("action", "MARK_REFUNDED_MANUALLY", "reason", "thử"),
                        identity(ops, "AUDITOR")), JsonNode.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM resolution_requests",
                Integer.class)).isZero();
        assertThat(jdbcTemplate.queryForList("SELECT action FROM audit_events", String.class))
                .as("lần bị từ chối vẫn để lại dấu vết").containsExactly("ADMIN_ACCESS_DENIED");
    }

    @Test
    @DisplayName("Mỗi giao dịch tối đa MỘT yêu cầu đang chờ — không thì người thứ ba duyệt nhầm cái nào cũng được")
    void chi_mot_yeu_cau_dang_cho() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        propose(txn, ops, "OPS", "Lần một", null);

        ResponseEntity<JsonNode> second = http.exchange(
                "/api/v1/admin/transactions/" + txn + "/resolve", HttpMethod.POST,
                new HttpEntity<>(Map.of("action", "MARK_REFUNDED_MANUALLY", "reason", "Lần hai"),
                        identity(admin, "ADMIN")), JsonNode.class);

        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(second.getBody().path("errorCode").asText()).isEqualTo("RESOLUTION_ALREADY_PENDING");
    }

    // ------------------------------------------------------------------
    // Tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Không đề xuất được cho ca của tenant khác — vai vận hành nới quyền TRONG tenant")
    void khong_de_xuat_duoc_cho_tenant_khac() {
        UUID cuaHo = needsReviewIn(OTHER_TENANT, "ACCOUNT_CLOSED");

        ResponseEntity<JsonNode> response = http.exchange(
                "/api/v1/admin/transactions/" + cuaHo + "/resolve", HttpMethod.POST,
                new HttpEntity<>(Map.of("action", "MARK_REFUNDED_MANUALLY", "reason", "thử"),
                        identity(ops, "OPS")), JsonNode.class);

        assertThat(response.getStatusCode())
                .as("404 chứ không 403 — khác nhau là cho người ngoài một máy dò id")
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM resolution_requests",
                Integer.class)).isZero();
    }

    @Test
    @DisplayName("⭐ Không duyệt được yêu cầu của tenant khác — hàng của họ không đổi")
    void khong_duyet_duoc_yeu_cau_cua_tenant_khac() {
        UUID cuaHo = needsReviewIn(OTHER_TENANT, "ACCOUNT_CLOSED");
        UUID requestId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO resolution_requests (id, tenant_id, transaction_id, action, status,
                                                 maker_id, maker_role, reason, created_at, updated_at)
                VALUES (?, ?, ?, 'MARK_REFUNDED_MANUALLY', 'PENDING', ?, 'OPS', 'của tenant khác',
                        now(), now())
                """, requestId, OTHER_TENANT, cuaHo, UUID.randomUUID());

        ResponseEntity<JsonNode> response = decide(requestId, admin, "ADMIN", true, null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM resolution_requests WHERE id = ?", String.class, requestId))
                .as("⚠ ADMIN của tenant này chốt được ca của tenant kia là thủng ranh giới cứng ADR-010")
                .isEqualTo("PENDING");
        assertThat(jdbcTemplate.queryForObject("SELECT status FROM transactions WHERE id = ?",
                String.class, cuaHo)).isEqualTo("MANUAL_REVIEW");
    }

    // ------------------------------------------------------------------
    // Tiền và dấu vết cùng một transaction
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Audit hỏng ⇒ TIỀN KHÔNG ĐƯỢC GHI — ngược hẳn đường đọc, và đó là chủ ý")
    void audit_hong_thi_tien_khong_duoc_ghi() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay", null);
        jdbcTemplate.execute("""
                CREATE OR REPLACE FUNCTION txn_fail_audit_insert() RETURNS trigger AS
                $$ BEGIN RAISE EXCEPTION 'audit chết'; END; $$ LANGUAGE plpgsql
                """);
        jdbcTemplate.execute("""
                CREATE TRIGGER trg_test_fail_audit BEFORE INSERT ON audit_events
                FOR EACH ROW EXECUTE FUNCTION txn_fail_audit_insert()
                """);
        try {
            ResponseEntity<JsonNode> response = decide(requestId, admin, "ADMIN", true, null);

            assertThat(response.getStatusCode().is2xxSuccessful()).isFalse();
            assertThat(txnRow(txn).get("status"))
                    .as("'không có thao tác đụng tiền nào không để lại audit' là BẤT BIẾN, không phải "
                            + "nỗ lực — ghi được tiền mà không ghi được dấu vết thì phải rollback cả hai")
                    .isEqualTo("MANUAL_REVIEW");
            assertThat(txnRow(txn).get("resolved_at")).isNull();
            assertThat(jdbcTemplate.queryForObject(
                    "SELECT status FROM resolution_requests WHERE id = ?", String.class, requestId))
                    .as("quyết định cũng phải quay lại — nó nằm cùng transaction")
                    .isEqualTo("PENDING");
        } finally {
            jdbcTemplate.execute("DROP TRIGGER trg_test_fail_audit ON audit_events");
        }
    }

    @Test
    @DisplayName("⭐ Đường audit của thao tác đụng tiền là MANDATORY — không tự mở transaction riêng")
    void audit_dung_tien_khong_tu_mo_transaction_rieng() {
        Operator actor = new Operator(admin, "admin@walletflow.dev", "ADMIN", "127.0.0.1");

        // Gọi NGOÀI mọi transaction. MANDATORY ném; REQUIRES_NEW thì lặng lẽ mở một transaction
        // riêng và commit — và đó mới là chiều nguy hiểm: dấu vết commit độc lập rồi TIỀN rollback,
        // để lại một dòng audit nói "đã duyệt và thực thi" cho việc chưa từng xảy ra. Sổ kiểm toán
        // nói dối còn tệ hơn sổ kiểm toán thiếu.
        //
        // (Chiều kia — audit hỏng thì tiền rollback — đúng với CẢ HAI propagation, nên
        // `audit_hong_thi_tien_khong_duoc_ghi` một mình không ghim được thiết kế này.)
        assertThatThrownBy(() -> TenantContext.runWith(TENANT,
                () -> auditWriter.writeInCurrentTransaction(actor, "THU", null, "{}")))
                .isInstanceOf(IllegalTransactionStateException.class);

        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM audit_events", Integer.class))
                .isZero();
    }

    @Test
    @DisplayName("Dấu vết ghi đủ cả hai nửa của 4-eyes: ai đề xuất, ai duyệt, vai gì")
    void dau_vet_ghi_du_hai_nua() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, ops, "OPS", "Ví đóng vĩnh viễn", "VNP-RF-1");
        decide(requestId, admin, "ADMIN", true, "Đã đối chiếu portal");

        List<Map<String, Object>> audits = jdbcTemplate.queryForList(
                "SELECT action, actor_id, actor_role, target FROM audit_events ORDER BY created_at");
        assertThat(audits).extracting(r -> r.get("action"))
                .containsExactly("RESOLUTION_PROPOSED", "RESOLUTION_APPROVED");
        assertThat(audits.get(0).get("actor_id")).isEqualTo(ops);
        assertThat(audits.get(0).get("actor_role")).isEqualTo("OPS");
        assertThat(audits.get(1).get("actor_id")).isEqualTo(admin);
        assertThat(audits).allSatisfy(r -> assertThat(r.get("target")).isEqualTo(txn.toString()));
    }

    // ------------------------------------------------------------------
    // Đua với saga
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Chốt xong rồi LedgerPosted tới muộn: KHÔNG thành COMPLETED, ví không được cộng")
    void chot_roi_thi_ledger_posted_muon_khong_lam_completed() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay", "VNP-RF-2");
        decide(requestId, admin, "ADMIN", true, null);

        // Lời từ chối TRƯỚC ledger V6 không có hàng rào rejected_topups, nên một lệnh ghi sổ gửi lại
        // vẫn ghi được sau khi ví được mở lại. Đây là ca thật, không phải giả định.
        TenantContext.runWith(TENANT, () -> sagaService.onPosted(
                SagaFixture.topupPosted(accepted(txn))));

        assertThat(txnRow(txn).get("status"))
                .as("COMPLETED ở đây là ví được cộng TRONG KHI tiền đã hoàn tay — người dùng nhận hai "
                        + "lần, và giao dịch trông như một lần nạp thành công nên không ai nhìn lại")
                .isEqualTo("FAILED");
        assertThat(jdbcTemplate.queryForList(
                "SELECT payload FROM outbox_event WHERE topic LIKE '%completed%'", String.class))
                .isEmpty();
    }

    @Test
    @DisplayName("⭐ Chốt một ca ĐANG Ở ĐƯỜNG HOÀN TIỀN rồi LedgerPosted tới muộn: ca vẫn ĐÓNG, không bị mở lại")
    void chot_roi_thi_ledger_posted_muon_khong_mo_lai_ca_dang_hoan() {
        // Khác test trên ở ĐÚNG MỘT điểm: saga đứng ở BANK_REFUND thay vì POST_JOURNAL — hình dạng
        // thật của mọi ca REFUND_* (ADR-020). Chính cột đó làm `onPosted` rẽ sang nhánh
        // `postedWhileRefunding` và thoát ra TRƯỚC khi chạm hàng rào `alreadyResolved`.
        UUID txn = needsReviewRefunding("REFUND_DECLINED");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay qua portal", "VNP-RF-9");
        decide(requestId, admin, "ADMIN", true, null);
        assertThat(txnRow(txn).get("status")).as("tiền đề: ca đã được chốt").isEqualTo("FAILED");
        double conflictsBefore = counter("walletflow.topup.result_conflict");
        double reviewsBefore = counter("walletflow.topup.manual_review");

        TenantContext.runWith(TENANT, () -> sagaService.onPosted(
                SagaFixture.topupPosted(accepted(txn))));

        assertThat(txnRow(txn).get("status"))
                .as("ADR-021 A5: saga KHÔNG BAO GIỜ ghi tiếp vào một ca người vận hành đã chốt, bất kể "
                        + "event nào tới và ca đang ở trạng thái nào. Mở lại ca này đưa nó về hàng đợi "
                        + "như thể chưa ai xử lý — người trực kế tiếp sẽ hoàn tay LẦN HAI số tiền đã hoàn")
                .isEqualTo("FAILED");
        assertThat(txnRow(txn).get("failure_code"))
                .as("mã phải giữ nguyên kết cục người vận hành đã ghi. Bị đổi thành "
                        + "REFUND_POSTED_CONFLICT nghĩa là lần chốt đã bị xoá khỏi lịch sử của ca")
                .isEqualTo("REFUNDED_MANUALLY");
        assertThat(txnRow(txn).get("resolved_at")).isNotNull();
        // Kiểm cả hàng saga: một bản sửa nửa vời (chặn ghi `transactions`, vẫn ghi `saga_state`) để
        // lại hai hàng nói hai chuyện khác nhau — đúng thứ sinh ra SAGA_STATE_DESYNC về sau.
        assertThat(jdbcTemplate.queryForObject(
                "SELECT step_status FROM saga_state WHERE transaction_id = ?", String.class, txn))
                .isEqualTo("FAILED");
        // Tín hiệu DUY NHẤT còn lại cho ca này sau khi hàng rào chặn: ca không quay vào hàng đợi nữa,
        // nên nếu counter này cũng im thì sự cố "ví được cộng VÀ tiền đã hoàn tay" trôi qua không ai thấy.
        assertThat(counter("walletflow.topup.result_conflict") - conflictsBefore).isEqualTo(1);
        assertThat(counter("walletflow.topup.manual_review") - reviewsBefore)
                .as("ca đã chốt KHÔNG được tính là một ca cần người xử lý lần nữa")
                .isZero();
    }

    @Test
    @DisplayName("⭐ Chốt xong rồi lệnh hoàn TỰ ĐỘNG mới xong: không ghi đè ca, nhưng phải KÊU — tiền đã về hai lần")
    void chot_roi_thi_hoan_tu_dong_xong_muon_van_keu() {
        // Ca thật: REFUND_NO_RESPONSE (job quét bỏ cuộc) → người vận hành hoàn tay qua portal → rồi
        // lệnh hoàn tự động vẫn đang chạy ở bank-adapter hoàn xong thật. Người dùng nhận tiền HAI LẦN.
        UUID txn = needsReviewRefunding("REFUND_NO_RESPONSE");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay qua portal", "VNP-RF-10");
        decide(requestId, admin, "ADMIN", true, null);
        double conflictsBefore = counter("walletflow.topup.result_conflict");

        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(
                SagaFixture.refundCompleted(accepted(txn))));

        assertThat(txnRow(txn).get("status")).isEqualTo("FAILED");
        assertThat(counter("walletflow.topup.result_conflict") - conflictsBefore)
                .as("trước khi có hàng rào, ca này rơi vào nhánh default của onRefundCompletedLate "
                        + "(trạng thái đã là FAILED chứ không còn MANUAL_REVIEW) và chỉ để lại log.debug")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("Chốt xong rồi provider từ chối lệnh hoàn: không ghi đè ca đã đóng, vẫn để lại dấu")
    void chot_roi_thi_loi_tu_choi_hoan_toi_muon_khong_ghi_de() {
        UUID txn = needsReviewRefunding("REFUND_NO_RESPONSE");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay qua portal", "VNP-RF-11");
        decide(requestId, admin, "ADMIN", true, null);
        double conflictsBefore = counter("walletflow.topup.result_conflict");

        TenantContext.runWith(TENANT, () -> sagaService.onBankFailed(
                SagaFixture.refundFailed(accepted(txn), "REFUND_UNSUPPORTED")));

        assertThat(txnRow(txn).get("failure_code")).isEqualTo("REFUNDED_MANUALLY");
        assertThat(txnRow(txn).get("status")).isEqualTo("FAILED");
        assertThat(counter("walletflow.topup.result_conflict") - conflictsBefore).isEqualTo(1);
    }

    @Test
    @DisplayName("⭐ Chốt xong rồi BankCompleted(CHARGE) lặp: KHÔNG gửi lệnh hoàn lần hai")
    void chot_roi_thi_bank_completed_lap_khong_hoan_lan_hai() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay", "VNP-RF-3");
        decide(requestId, admin, "ADMIN", true, null);

        // Bản lặp của lời xác nhận thu tiền: outbox at-least-once, consumer rebalance, hay một DLT
        // vừa được giải phóng. Trước 2.5 PR2 ca này nằm ở MANUAL_REVIEW và bị bỏ qua an toàn; việc
        // chốt đưa nó sang FAILED, tức là vào đúng nhánh `startRefund` — nếu không có hàng rào
        // `resolved_at` thì ta hoàn lần thứ HAI số tiền người vận hành vừa hoàn tay qua portal.
        TenantContext.runWith(TENANT, () -> sagaService.onBankCompleted(
                SagaFixture.bankCompleted(accepted(txn))));

        assertThat(jdbcTemplate.queryForList(
                "SELECT payload FROM outbox_event WHERE topic = ?", String.class,
                WalletTopics.CMD_BANK_REFUND))
                .as("⚠ một lệnh hoàn ở đây là mất tiền thật — người dùng đã được hoàn qua portal rồi")
                .isEmpty();
        Map<String, Object> row = txnRow(txn);
        assertThat(row.get("status")).isEqualTo("FAILED");
        assertThat(row.get("failure_code"))
                .as("và không được xoá mất dấu vết duy nhất nói rằng tiền đã hoàn tay")
                .isEqualTo("REFUNDED_MANUALLY");
    }

    @Test
    @DisplayName("Chốt xong rồi LedgerRejected lặp: không đụng gì tới ca đã đóng")
    void chot_roi_thi_ledger_rejected_lap_khong_dung_gi() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay", null);
        decide(requestId, admin, "ADMIN", true, null);

        TenantContext.runWith(TENANT, () -> sagaService.onRejected(
                SagaFixture.topupRejected(accepted(txn), "ACCOUNT_CLOSED")));

        assertThat(txnRow(txn).get("failure_code")).isEqualTo("REFUNDED_MANUALLY");
    }

    @Test
    @DisplayName("⭐ Ca đã rời MANUAL_REVIEW giữa lúc đề xuất và lúc duyệt: 409, không ghi đè")
    void ca_da_dong_thi_khong_chot_duoc() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay", null);
        // Saga tự xử lý xong trong lúc người vận hành đang gõ
        jdbcTemplate.update("UPDATE transactions SET status = 'COMPLETED' WHERE id = ?", txn);

        ResponseEntity<JsonNode> response = decide(requestId, admin, "ADMIN", true, null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().path("errorCode").asText()).isEqualTo("CASE_NO_LONGER_OPEN");
        assertThat(txnRow(txn).get("status"))
                .as("⚠ ghi đè COMPLETED thành FAILED là nói với người dùng họ không mất tiền trong khi "
                        + "ví vừa được cộng")
                .isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("Đề xuất cho một ca không ở diện chờ người: 409 ngay, không tạo yêu cầu")
    void de_xuat_cho_ca_khong_mo_bi_tu_choi() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        jdbcTemplate.update("UPDATE transactions SET status = 'COMPLETED' WHERE id = ?", txn);

        ResponseEntity<JsonNode> response = http.exchange(
                "/api/v1/admin/transactions/" + txn + "/resolve", HttpMethod.POST,
                new HttpEntity<>(Map.of("action", "MARK_REFUNDED_MANUALLY", "reason", "thử"),
                        identity(ops, "OPS")), JsonNode.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM resolution_requests",
                Integer.class)).isZero();
    }

    @Test
    @DisplayName("Từ chối yêu cầu: ca quay về hàng đợi nguyên vẹn, có dấu vết")
    void tu_choi_thi_ca_quay_ve_hang_doi() {
        UUID txn = needsReview("ACCOUNT_CLOSED");
        UUID requestId = propose(txn, ops, "OPS", "Hoàn tay", null);

        assertThat(decide(requestId, admin, "ADMIN", false, "Chưa thấy bằng chứng ở portal")
                .getStatusCode()).isEqualTo(HttpStatus.OK);

        assertThat(txnRow(txn).get("status")).isEqualTo("MANUAL_REVIEW");
        assertThat(txnRow(txn).get("resolved_at")).isNull();
        assertThat(jdbcTemplate.queryForList("SELECT action FROM audit_events", String.class))
                .containsExactly("RESOLUTION_PROPOSED", "RESOLUTION_REJECTED");
    }

    @Test
    @DisplayName("Thiếu lý do: 400 — một thao tác đụng tiền không giải thích được thì không nên xảy ra")
    void thieu_ly_do_bi_tu_choi() {
        UUID txn = needsReview("ACCOUNT_CLOSED");

        ResponseEntity<JsonNode> response = http.exchange(
                "/api/v1/admin/transactions/" + txn + "/resolve", HttpMethod.POST,
                new HttpEntity<>(Map.of("action", "MARK_REFUNDED_MANUALLY", "reason", "  "),
                        identity(ops, "OPS")), JsonNode.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private UUID needsReview(String failureCode) {
        return needsReviewIn(TENANT, failureCode);
    }

    private UUID needsReviewIn(UUID tenant, String failureCode) {
        return needsReviewIn(tenant, failureCode, "POST_JOURNAL");
    }

    /**
     * Ca đã vào ĐƯỜNG HOÀN TIỀN: saga đứng ở {@code BANK_REFUND}, không phải {@code POST_JOURNAL}.
     *
     * <p>Đây là hình dạng thật của mọi ca {@code REFUND_*} (ADR-020): lệnh hoàn đã được gửi rồi mới
     * hỏng. Phân biệt với {@code POST_JOURNAL} là quan trọng vì {@code SagaService.onPosted} rẽ
     * nhánh theo đúng cột này — {@code refundStarted()} đọc {@code current_step}.
     */
    private UUID needsReviewRefunding(String failureCode) {
        return needsReviewIn(TENANT, failureCode, "BANK_REFUND");
    }

    private UUID needsReviewIn(UUID tenant, String failureCode, String currentStep) {
        UUID id = UUID.randomUUID();
        UUID wallet = wallets.add(tenant, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
        jdbcTemplate.update("""
                INSERT INTO transactions (id, tenant_id, type, status, initiated_by, to_wallet,
                                          amount_minor, currency, failure_code, failure_reason,
                                          provider, provider_ref, created_at, updated_at)
                VALUES (?, ?, 'TOPUP', 'MANUAL_REVIEW', ?, ?, 500000, 'VND', ?, ?, 'mock', ?, now(), now())
                """, id, tenant, UUID.randomUUID(), wallet, failureCode,
                "Nguyên nhân: " + failureCode, "MOCK-" + id);
        jdbcTemplate.update("""
                INSERT INTO saga_state (transaction_id, tenant_id, current_step, step_status,
                                        attempt_count, command_event_id, deadline_at, updated_at)
                VALUES (?, ?, ?, 'MANUAL_REVIEW', 1, ?, now(), now())
                """, id, tenant, currentStep, UUID.randomUUID());
        return id;
    }

    /** Dựng {@code Accepted} tối thiểu để tái dùng fixture sự kiện của saga. */
    private SagaFixture.Accepted accepted(UUID txn) {
        Map<String, Object> row = txnRow(txn);
        return new SagaFixture.Accepted(TENANT, txn, null, (UUID) row.get("to_wallet"),
                ((Number) row.get("amount_minor")).longValue());
    }

    private UUID propose(UUID txn, UUID actor, String role, String reason, String evidence) {
        Map<String, Object> body = evidence == null
                ? Map.of("action", "MARK_REFUNDED_MANUALLY", "reason", reason)
                : Map.of("action", "MARK_REFUNDED_MANUALLY", "reason", reason, "evidence", evidence);
        ResponseEntity<JsonNode> response = http.exchange(
                "/api/v1/admin/transactions/" + txn + "/resolve", HttpMethod.POST,
                new HttpEntity<>(body, identity(actor, role)), JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return UUID.fromString(response.getBody().path("data").path("resolutionRequestId").asText());
    }

    private ResponseEntity<JsonNode> decide(UUID requestId, UUID actor, String role,
                                            boolean approve, String note) {
        Map<String, Object> body = note == null
                ? Map.of("approve", approve) : Map.of("approve", approve, "note", note);
        return http.exchange("/api/v1/admin/transactions/resolutions/" + requestId + "/decision",
                HttpMethod.POST, new HttpEntity<>(body, identity(actor, role)), JsonNode.class);
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

    private Map<String, Object> txnRow(UUID txn) {
        return jdbcTemplate.queryForMap("SELECT * FROM transactions WHERE id = ?", txn);
    }

    /** 0 khi counter chưa từng được đăng ký — khác với "đã đăng ký và đang ở 0". */
    private double counter(String name) {
        Counter counter = meterRegistry.find(name).counter();
        return counter == null ? 0 : counter.count();
    }
}
