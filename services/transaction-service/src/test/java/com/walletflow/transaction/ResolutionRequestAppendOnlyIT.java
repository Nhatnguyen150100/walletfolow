package com.walletflow.transaction;

import com.walletflow.common.money.ECurrency;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.transaction.support.FakeWalletDirectory;
import com.walletflow.transaction.support.FakeWalletDirectoryConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Migration V8: {@code resolution_requests} là append-only ở tầng DB (ADR-021 A7, ADR-023 §8).
 *
 * <p>Vì sao test này đi thẳng qua {@link JdbcTemplate} thay vì qua {@code /resolve} và {@code /decision}:
 * hàng rào phải chịu được <b>một câu SQL gõ tay</b>, không chỉ chặn được đường đi qua app. Đi qua app
 * thì tầng Java đã tự chặn trước (không ai gọi APPROVED → PENDING cả), test sẽ xanh dù DB không có
 * hàng rào nào. Phần "app vẫn chạy được trên hàng rào mới" do {@code AdminResolveIT} và
 * {@code AdminReversalIT} chứng minh; ở đây chỉ có ca {@link #van_quyet_duoc_mot_lan_tu_pending()}
 * bảo vệ đúng đường đi đó ở mức SQL.
 *
 * <p>Hai nửa bắt buộc đi cặp với nhau:
 * <ul>
 *   <li><b>Cấm</b>: sửa lại quyết định đã chốt, xoá yêu cầu, sửa nội dung đề xuất khi còn PENDING,
 *       chốt mà không ghi vai người duyệt, điền sẵn người duyệt khi còn PENDING.</li>
 *   <li><b>Cho phép</b>: quyết đúng một lần từ PENDING. Một trigger chặn nhầm đường đúng làm hỏng
 *       {@code POST /decision} ngoài production — nên "vẫn quyết được" quan trọng không kém "không
 *       sửa được".</li>
 * </ul>
 *
 * <p>Mỗi ca cấm không chỉ kiểm có lỗi: nó kiểm lỗi <i>đến từ DB</i> (thông điệp trigger hoặc tên
 * constraint) và kiểm hàng KHÔNG đổi một byte nào sau đó — một câu lệnh bị chặn nửa chừng mà vẫn để lại
 * dấu vết thì cũng là hàng rào hỏng.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
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
class ResolutionRequestAppendOnlyIT {

    private static final UUID TENANT = UUID.randomUUID();

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
    JdbcTemplate jdbcTemplate;
    @Autowired
    FakeWalletDirectory wallets;

    private UUID maker;
    private UUID checker;

    @BeforeEach
    void reset() {
        // TRUNCATE không kích hoạt trigger hàng (BEFORE UPDATE OR DELETE) nên dọn dữ liệu được.
        jdbcTemplate.execute("TRUNCATE resolution_requests, saga_state, transactions, "
                + "idempotency_keys, outbox_event, audit_events CASCADE");
        wallets.reset();
        maker = UUID.randomUUID();
        checker = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Các ca CẤM
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Quyết định đã chốt thì không đổi ngược được: APPROVED → PENDING bằng SQL tay bị chặn (lỗ hổng A7)")
    void quyet_dinh_da_chot_thi_khong_doi_nguoc_duoc() {
        UUID txn = giaoDich();
        UUID approved = yeuCauDaQuyet(txn, "APPROVED");
        Map<String, Object> truoc = hang(approved);

        // Chính xác câu lệnh mà A7 mô tả. Hai chiều đóng cùng một lúc: để checker_* nguyên thì CHECK
        // ck_resolution_decided chặn, nhưng người gõ tay sẽ xoá luôn checker_* — nên phải là trigger
        // chặn, không phải CHECK.
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE resolution_requests SET status = 'PENDING' WHERE id = ?", approved))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("APPEND-ONLY");
        assertThatThrownBy(() -> jdbcTemplate.update("""
                UPDATE resolution_requests
                   SET status = 'PENDING', checker_id = NULL, checker_role = NULL,
                       checker_email = NULL, decided_at = NULL
                 WHERE id = ?
                """, approved))
                .as("gỡ cả checker_* để qua CHECK: chỉ còn trigger đứng giữa")
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("APPEND-ONLY")
                .hasMessageContaining("vĩnh viễn");

        assertThat(trangThai(approved)).as("quyết định đã chốt phải còn nguyên").isEqualTo("APPROVED");
        assertThat(hang(approved)).isEqualTo(truoc);
    }

    @Test
    @DisplayName("Đã chốt thì đóng băng hẳn: không đổi APPROVED sang REJECTED, không sửa người duyệt, REJECTED cũng không mở lại được")
    void da_chot_thi_dong_bang_moi_cot() {
        UUID txn = giaoDich();
        UUID approved = yeuCauDaQuyet(txn, "APPROVED");
        UUID txn2 = giaoDich();
        UUID rejected = yeuCauDaQuyet(txn2, "REJECTED");
        Map<String, Object> truocApproved = hang(approved);
        Map<String, Object> truocRejected = hang(rejected);

        // Đổi kết luận "duyệt" thành "từ chối" (hoặc ngược lại) cũng là viết lại lịch sử, không chỉ
        // riêng chiều về PENDING.
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE resolution_requests SET status = 'REJECTED' WHERE id = ?", approved))
                .isInstanceOf(DataAccessException.class).hasMessageContaining("APPEND-ONLY");
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE resolution_requests SET checker_role = 'OPS' WHERE id = ?", approved))
                .isInstanceOf(DataAccessException.class).hasMessageContaining("APPEND-ONLY");
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE resolution_requests SET decided_at = now() WHERE id = ?", approved))
                .isInstanceOf(DataAccessException.class).hasMessageContaining("APPEND-ONLY");
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE resolution_requests SET reason = 'viết lại lý do' WHERE id = ?", approved))
                .isInstanceOf(DataAccessException.class).hasMessageContaining("APPEND-ONLY");
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE resolution_requests SET updated_at = now() WHERE id = ?", approved))
                .as("ngay cả siêu dữ liệu: đã chốt là không một câu UPDATE nào lọt qua")
                .isInstanceOf(DataAccessException.class).hasMessageContaining("APPEND-ONLY");
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE resolution_requests SET status = 'APPROVED' WHERE id = ?", rejected))
                .isInstanceOf(DataAccessException.class).hasMessageContaining("APPEND-ONLY");

        assertThat(hang(approved)).isEqualTo(truocApproved);
        assertThat(hang(rejected)).isEqualTo(truocRejected);
    }

    @Test
    @DisplayName("⭐ Không xoá được yêu cầu — dù đang PENDING hay đã chốt, xoá một yêu cầu đụng tiền là xoá bằng chứng")
    void khong_xoa_duoc_yeu_cau() {
        UUID pending = yeuCauPending(giaoDich(), "MARK_REFUNDED_MANUALLY", null);
        UUID approved = yeuCauDaQuyet(giaoDich(), "APPROVED");

        for (UUID id : List.of(pending, approved)) {
            assertThatThrownBy(() -> jdbcTemplate.update(
                    "DELETE FROM resolution_requests WHERE id = ?", id))
                    .isInstanceOf(DataAccessException.class)
                    .hasMessageContaining("APPEND-ONLY")
                    .hasMessageContaining("không xoá được");
        }

        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM resolution_requests WHERE id IN (?, ?)", Integer.class,
                pending, approved)).as("cả hai hàng vẫn còn").isEqualTo(2);
    }

    @Test
    @DisplayName("⭐ Còn PENDING thì không sửa được nội dung đề xuất: lý do, người đề xuất, bút toán đích... (người duyệt ký vào đúng thứ đã đọc)")
    void khong_sua_duoc_noi_dung_de_xuat_khi_con_pending() {
        UUID mark = yeuCauPending(giaoDich(), "MARK_REFUNDED_MANUALLY", null);
        UUID entry = UUID.randomUUID();
        UUID reverse = yeuCauPending(giaoDich(), "REVERSE_LEDGER_ENTRY", entry);
        UUID txnKhac = giaoDich();

        // Mỗi phần tử một cột trong danh sách bất biến của trigger. Thiếu một cột ở trigger ⇒ đúng một
        // dòng ở đây đỏ.
        record Sua(String cot, UUID hang, String set, Object... args) {
        }
        List<Sua> cacSua = List.of(
                new Sua("reason", mark, "reason = 'khác'"),
                new Sua("evidence", mark, "evidence = 'VNP-RF-SUA'"),
                new Sua("maker_id", mark, "maker_id = ?", UUID.randomUUID()),
                new Sua("maker_email", mark, "maker_email = 'nguoi.khac@walletflow.dev'"),
                new Sua("maker_role", mark, "maker_role = 'ADMIN'"),
                new Sua("tenant_id", mark, "tenant_id = ?", UUID.randomUUID()),
                new Sua("transaction_id", mark, "transaction_id = ?", txnKhac),
                new Sua("created_at", mark, "created_at = now() + interval '1 day'"),
                new Sua("target_entry_id", reverse, "target_entry_id = ?", UUID.randomUUID()),
                // Đổi thao tác: đổi sang REVERSE kèm bút toán đích để qua ck_resolution_target_entry,
                // tức chỉ còn trigger đứng giữa.
                new Sua("action", mark, "action = 'REVERSE_LEDGER_ENTRY', target_entry_id = ?",
                        UUID.randomUUID()));

        for (Sua sua : cacSua) {
            Map<String, Object> truoc = hang(sua.hang());
            Object[] args = new Object[sua.args().length + 1];
            System.arraycopy(sua.args(), 0, args, 0, sua.args().length);
            args[args.length - 1] = sua.hang();

            assertThatThrownBy(() -> jdbcTemplate.update(
                    "UPDATE resolution_requests SET " + sua.set() + " WHERE id = ?", args))
                    .as("sửa cột %s khi còn PENDING phải bị chặn", sua.cot())
                    .isInstanceOf(DataAccessException.class)
                    .hasMessageContaining("APPEND-ONLY")
                    .hasMessageContaining("nội dung đề xuất");
            assertThat(hang(sua.hang())).as("hàng không đổi sau khi sửa cột %s", sua.cot())
                    .isEqualTo(truoc);
        }
    }

    // ------------------------------------------------------------------
    // Đường ĐI ĐÚNG phải còn chạy
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đường đúng vẫn chạy: PENDING → quyết MỘT lần (status + checker_id/role/email + decided_at trong một câu) thành công")
    void van_quyet_duoc_mot_lan_tu_pending() {
        UUID id = yeuCauPending(giaoDich(), "MARK_REFUNDED_MANUALLY", null);
        Map<String, Object> truoc = hang(id);

        // Hình dạng của câu UPDATE mà `decide` sinh ra: đổi trạng thái, điền cả bốn cột người duyệt và
        // chạm updated_at (siêu dữ liệu, cố ý không nằm trong danh sách bất biến).
        int daSua = jdbcTemplate.update("""
                UPDATE resolution_requests
                   SET status = 'APPROVED', checker_id = ?, checker_role = 'ADMIN',
                       checker_email = 'admin@walletflow.dev', decided_at = now(), updated_at = now()
                 WHERE id = ?
                """, checker, id);

        assertThat(daSua).isEqualTo(1);
        Map<String, Object> sau = hang(id);
        assertThat(sau.get("status")).isEqualTo("APPROVED");
        assertThat(sau.get("checker_id")).isEqualTo(checker);
        assertThat(sau.get("checker_role")).isEqualTo("ADMIN");
        assertThat(sau.get("checker_email")).isEqualTo("admin@walletflow.dev");
        assertThat(sau.get("decided_at")).isNotNull();
        // Nội dung đề xuất giữ nguyên qua lần quyết.
        for (String cot : List.of("tenant_id", "transaction_id", "action", "maker_id", "maker_email",
                "maker_role", "reason", "evidence", "target_entry_id", "created_at")) {
            assertThat(sau.get(cot)).as("cột %s", cot).isEqualTo(truoc.get(cot));
        }
        // Và quyết rồi thì khoá: câu thứ hai (dù cùng nội dung) không lọt qua.
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE resolution_requests SET status = 'REJECTED' WHERE id = ?", id))
                .isInstanceOf(DataAccessException.class).hasMessageContaining("APPEND-ONLY");
    }

    @Test
    @DisplayName("Đường từ chối cũng chạy: PENDING → REJECTED một câu; còn PENDING thì chạm updated_at được")
    void van_tu_choi_duoc_tu_pending_va_cham_updated_at() {
        UUID id = yeuCauPending(giaoDich(), "MARK_REFUNDED_MANUALLY", null);

        // updated_at không phải nội dung đề xuất: chạm vào nó khi còn PENDING không bị chặn.
        assertThat(jdbcTemplate.update(
                "UPDATE resolution_requests SET updated_at = now() WHERE id = ?", id)).isEqualTo(1);

        assertThat(jdbcTemplate.update("""
                UPDATE resolution_requests
                   SET status = 'REJECTED', checker_id = ?, checker_role = 'ADMIN',
                       decided_at = now(), updated_at = now()
                 WHERE id = ?
                """, checker, id)).isEqualTo(1);
        assertThat(trangThai(id)).isEqualTo("REJECTED");
    }

    // ------------------------------------------------------------------
    // CHECK ck_resolution_decided mới
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đã quyết thì phải biết VAI người duyệt: APPROVED/REJECTED mà checker_role NULL bị CHECK từ chối (INSERT và UPDATE)")
    void da_quyet_thi_phai_biet_vai_nguoi_duyet() {
        UUID txn = giaoDich();

        for (String status : List.of("APPROVED", "REJECTED")) {
            assertThatThrownBy(() -> jdbcTemplate.update("""
                    INSERT INTO resolution_requests (id, tenant_id, transaction_id, action, status,
                                                     maker_id, maker_role, checker_id, checker_email,
                                                     decided_at, reason, created_at, updated_at)
                    VALUES (?, ?, ?, 'MARK_REFUNDED_MANUALLY', ?, ?, 'OPS', ?, 'a@walletflow.dev',
                            now(), 'thiếu vai', now(), now())
                    """, UUID.randomUUID(), TENANT, txn, status, maker, checker))
                    .as("INSERT %s với checker_role NULL", status)
                    .isInstanceOf(DataAccessException.class)
                    .hasMessageContaining("ck_resolution_decided");
        }

        // Cả đường UPDATE: trigger cho phép PENDING → quyết, nên chính CHECK phải bắt thiếu vai.
        UUID pending = yeuCauPending(txn, "MARK_REFUNDED_MANUALLY", null);
        Map<String, Object> truoc = hang(pending);
        assertThatThrownBy(() -> jdbcTemplate.update("""
                UPDATE resolution_requests
                   SET status = 'APPROVED', checker_id = ?, decided_at = now()
                 WHERE id = ?
                """, checker, pending))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("ck_resolution_decided");
        assertThat(hang(pending)).as("yêu cầu vẫn PENDING, nguyên vẹn").isEqualTo(truoc);
    }

    @Test
    @DisplayName("⭐ Còn PENDING thì không được điền sẵn người duyệt: INSERT PENDING có checker_* / decided_at bị từ chối (4-eyes không bị rút ruột)")
    void con_pending_thi_khong_duoc_dien_san_nguoi_duyet() {
        UUID txn = giaoDich();

        // Từng cột một: V6 cũ chỉ cấm checker_id/decided_at, còn checker_role và checker_email lọt.
        Map<String, Object> cotDienSan = new java.util.LinkedHashMap<>();
        cotDienSan.put("checker_id", checker);
        cotDienSan.put("checker_role", "ADMIN");
        cotDienSan.put("checker_email", "admin@walletflow.dev");
        cotDienSan.put("decided_at", java.time.OffsetDateTime.now());

        cotDienSan.forEach((cot, giaTri) -> assertThatThrownBy(() -> jdbcTemplate.update(
                "INSERT INTO resolution_requests (id, tenant_id, transaction_id, action, status, "
                        + "maker_id, maker_role, reason, " + cot + ", created_at, updated_at) "
                        + "VALUES (?, ?, ?, 'MARK_REFUNDED_MANUALLY', 'PENDING', ?, 'OPS', 'điền sẵn', ?, "
                        + "now(), now())",
                UUID.randomUUID(), TENANT, txn, maker, giaTri))
                .as("PENDING mà có sẵn %s", cot)
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("ck_resolution_decided"));

        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM resolution_requests",
                Integer.class)).as("không hàng nào lọt vào").isZero();
    }

    @Test
    @DisplayName("4-eyes vẫn được DB cưỡng chế sau khi siết CHECK: người duyệt = người đề xuất bị ck_resolution_four_eyes chặn")
    void bon_mat_van_duoc_cuong_che_khi_quyet_day_du() {
        UUID id = yeuCauPending(giaoDich(), "MARK_REFUNDED_MANUALLY", null);
        Map<String, Object> truoc = hang(id);

        // Điền ĐỦ checker_role/decided_at để ck_resolution_decided thoả; chỉ còn 4-eyes là lý do bị chặn.
        // (Ghi chú: Postgres KHÔNG cam kết thứ tự kiểm các CHECK, nên một câu thiếu checker_role có
        // thể bị `ck_resolution_decided` bắt trước và không chứng minh được gì về 4-eyes. Cách chắc
        // chắn duy nhất là làm cho 4-eyes là ràng buộc DUY NHẤT bị phạm.)
        assertThatThrownBy(() -> jdbcTemplate.update("""
                UPDATE resolution_requests
                   SET status = 'APPROVED', checker_id = maker_id, checker_role = 'ADMIN',
                       decided_at = now()
                 WHERE id = ?
                """, id))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("ck_resolution_four_eyes");
        assertThat(hang(id)).isEqualTo(truoc);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    /** Một giao dịch tối thiểu để thoả FK {@code transaction_id}. */
    private UUID giaoDich() {
        UUID id = UUID.randomUUID();
        UUID wallet = wallets.add(TENANT, UUID.randomUUID(), ECurrency.VND, EWalletStatus.ACTIVE);
        jdbcTemplate.update("""
                INSERT INTO transactions (id, tenant_id, type, status, initiated_by, to_wallet,
                                          amount_minor, currency, failure_code, failure_reason,
                                          provider, provider_ref, created_at, updated_at)
                VALUES (?, ?, 'TOPUP', 'MANUAL_REVIEW', ?, ?, 500000, 'VND', 'ACCOUNT_CLOSED',
                        'Nguyên nhân: ACCOUNT_CLOSED', 'mock', ?, now(), now())
                """, id, TENANT, UUID.randomUUID(), wallet, "MOCK-" + id);
        return id;
    }

    /** Yêu cầu đang chờ, đúng hình dạng app ghi lúc đề xuất (không có checker_*). */
    private UUID yeuCauPending(UUID txn, String action, UUID targetEntry) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO resolution_requests (id, tenant_id, transaction_id, action, status,
                                                 maker_id, maker_email, maker_role, reason, evidence,
                                                 target_entry_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'PENDING', ?, 'ops@walletflow.dev', 'OPS', 'Hoàn tay qua portal',
                        'VNP-RF-1', ?, now(), now())
                """, id, TENANT, txn, action, maker, targetEntry);
        return id;
    }

    /** Yêu cầu đã chốt (INSERT thẳng: trigger chỉ chặn UPDATE/DELETE, không chặn INSERT). */
    private UUID yeuCauDaQuyet(UUID txn, String status) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO resolution_requests (id, tenant_id, transaction_id, action, status,
                                                 maker_id, maker_email, maker_role, checker_id,
                                                 checker_email, checker_role, reason, evidence,
                                                 decided_at, created_at, updated_at)
                VALUES (?, ?, ?, 'MARK_REFUNDED_MANUALLY', ?, ?, 'ops@walletflow.dev', 'OPS', ?,
                        'admin@walletflow.dev', 'ADMIN', 'Hoàn tay qua portal', 'VNP-RF-1',
                        now(), now(), now())
                """, id, TENANT, txn, status, maker, checker);
        return id;
    }

    private Map<String, Object> hang(UUID id) {
        return jdbcTemplate.queryForMap("SELECT * FROM resolution_requests WHERE id = ?", id);
    }

    private String trangThai(UUID id) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM resolution_requests WHERE id = ?", String.class, id);
    }
}
