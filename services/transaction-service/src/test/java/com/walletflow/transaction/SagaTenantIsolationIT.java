package com.walletflow.transaction;

import com.walletflow.transaction.repositories.SagaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ba câu UPDATE của saga ({@code recordRetry}, {@code postpone}, {@code close}) phải lọc theo
 * {@code tenant_id} (roadmap 2.5 PR3, ADR-010). Trước đây chúng chỉ lọc {@code transaction_id}.
 *
 * <p>Không ca nào khai thác được hôm nay (id sinh phía ta, không đoán được), nhưng điều kiện tenant
 * là thứ khiến sự an toàn đó khỏi phụ thuộc vào việc ai đó nhớ nguồn của {@code transactionId}. Test
 * gọi repository THẲNG với tenant sai, vì đường HTTP không có cách nào đưa tenant sai vào đây — đó
 * chính là lý do lỗ hổng này im lặng được.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "walletflow.transaction.saga-scan.enabled=false",
        "management.tracing.enabled=false"
})
@Testcontainers
class SagaTenantIsolationIT {

    private static final UUID TENANT_A = UUID.randomUUID();
    private static final UUID TENANT_B = UUID.randomUUID();

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
    SagaRepository sagaRepository;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    PlatformTransactionManager transactionManager;

    private UUID txn;
    private UUID originalEventId;

    @BeforeEach
    void dungSagaCuaTenantA() {
        jdbcTemplate.execute("TRUNCATE resolution_requests, saga_state, transactions, idempotency_keys, outbox_event, audit_events");
        txn = UUID.randomUUID();
        originalEventId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO transactions (id, tenant_id, type, status, initiated_by, to_wallet, amount_minor,
                                          currency, provider, provider_ref, created_at, updated_at)
                VALUES (?, ?, 'TOPUP', 'PROCESSING', ?, ?, 500000, 'VND', 'mock', ?, now(), now())
                """, txn, TENANT_A, UUID.randomUUID(), UUID.randomUUID(), "MOCK-" + txn);
        // deadline cố định ở quá khứ xa để mọi `now() + ...` sau này đều khác nó — nếu không, "không đổi"
        // và "đổi" không phân biệt được.
        jdbcTemplate.update("""
                INSERT INTO saga_state (transaction_id, tenant_id, current_step, step_status, attempt_count,
                                        command_event_id, deadline_at, updated_at)
                VALUES (?, ?, 'BANK_CHARGE', 'SENT', 2, ?, TIMESTAMPTZ '2020-01-01 00:00:00+00',
                        TIMESTAMPTZ '2020-01-01 00:00:00+00')
                """, txn, TENANT_A, originalEventId);
    }

    // ------------------------------------------------------------------
    // Tenant sai: 0 hàng, hàng của A nguyên vẹn
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ close với tenant SAI: trả 0, hàng của tenant A KHÔNG đổi")
    void close_voi_tenant_sai_khong_dong_duoc_saga_cua_tenant_khac() {
        Map<String, Object> before = saga();

        int affected = inTx(() -> sagaRepository.close(TENANT_B, txn, "FAILED"));

        assertThat(affected).as("⚠ khác 0 = saga của tenant khác bị đóng xuyên tenant").isZero();
        assertThat(saga()).isEqualTo(before);
        assertThat(saga().get("step_status")).isEqualTo("SENT");
    }

    @Test
    @DisplayName("⭐ postpone với tenant SAI: trả 0, deadline_at của tenant A nguyên vẹn")
    void postpone_voi_tenant_sai_khong_doi_han_chot_cua_tenant_khac() {
        Map<String, Object> before = saga();

        int affected = inTx(() -> sagaRepository.postpone(TENANT_B, txn, 3_600));

        assertThat(affected).isZero();
        assertThat(saga().get("deadline_at")).isEqualTo(before.get("deadline_at"));
        assertThat(saga()).isEqualTo(before);
    }

    @Test
    @DisplayName("⭐ recordRetry với tenant SAI: trả 0, attempt_count và command_event_id của tenant A nguyên vẹn")
    void recordRetry_voi_tenant_sai_khong_dung_vao_saga_cua_tenant_khac() {
        Map<String, Object> before = saga();

        int affected = inTx(() -> sagaRepository.recordRetry(TENANT_B, txn, UUID.randomUUID(), 3_600));

        assertThat(affected).isZero();
        assertThat(saga().get("attempt_count")).isEqualTo(2);
        assertThat(saga().get("command_event_id")).isEqualTo(originalEventId);
        assertThat(saga()).isEqualTo(before);
    }

    // ------------------------------------------------------------------
    // Đối chứng: đúng tenant thì CÓ đổi
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Đối chứng — close với ĐÚNG tenant: trả 1 và đóng saga")
    void close_voi_dung_tenant_thi_dong_duoc() {
        int affected = inTx(() -> sagaRepository.close(TENANT_A, txn, "FAILED"));

        assertThat(affected).isEqualTo(1);
        assertThat(saga().get("step_status")).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("Đối chứng — postpone với ĐÚNG tenant: trả 1 và đẩy deadline_at ra sau")
    void postpone_voi_dung_tenant_thi_doi_han_chot() {
        Timestamp before = (Timestamp) saga().get("deadline_at");

        int affected = inTx(() -> sagaRepository.postpone(TENANT_A, txn, 3_600));

        assertThat(affected).isEqualTo(1);
        assertThat((Timestamp) saga().get("deadline_at")).isAfter(before);
    }

    @Test
    @DisplayName("Đối chứng — recordRetry với ĐÚNG tenant: trả 1, tăng attempt_count, đổi command_event_id")
    void recordRetry_voi_dung_tenant_thi_ghi_nhan_lan_gui_lai() {
        UUID newEventId = UUID.randomUUID();

        int affected = inTx(() -> sagaRepository.recordRetry(TENANT_A, txn, newEventId, 3_600));

        assertThat(affected).isEqualTo(1);
        assertThat(saga().get("attempt_count")).isEqualTo(3);
        assertThat(saga().get("command_event_id")).isEqualTo(newEventId).isNotEqualTo(originalEventId);
    }

    private Map<String, Object> saga() {
        return jdbcTemplate.queryForMap("SELECT * FROM saga_state WHERE transaction_id = ?", txn);
    }

    /** {@code @Modifying} cần transaction; repository không tự mở. */
    private int inTx(java.util.function.Supplier<Integer> action) {
        Integer result = new TransactionTemplate(transactionManager).execute(status -> action.get());
        assertThat(result).isNotNull();
        return result;
    }
}
