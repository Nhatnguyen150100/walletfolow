package com.walletflow.bank;

import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.bank.support.BankDb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.testcontainers.kafka.KafkaContainer;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mỗi chuyển trạng thái của {@code provider_transactions} là MỘT câu UPDATE có điều kiện trên trạng thái nguồn
 * (xem {@code ProviderTransactionRepository}) — đó là thứ duy nhất ngăn hai luồng đua nhau (lệnh gửi lại, job
 * đối chiếu, webhook) ghi đè kết quả cuối của nhau. Đặc biệt: một lần thu đã {@code SUCCEEDED} (tiền đã vào)
 * không bao giờ được bị một câu trả lời đến muộn lật thành {@code FAILED}.
 *
 * <p>Test chạy từng chuyển trạng thái trên MỌI trạng thái không phải nguồn và chứng minh: trả 0 và hàng
 * nguyên vẹn đến từng cột (kể cả {@code updated_at}, {@code raw_response}, {@code failure_code}). Có đối chứng
 * dương (từ đúng trạng thái nguồn thì trả 1) để test không xanh chỉ vì câu UPDATE chẳng bao giờ khớp gì.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "outbox.relay.delay-ms=300",
        "walletflow.bank.provider=mock",
        // Mock cần xác nhận thứ hai (PaymentProviderConfig) — chỉ stack dev/test bật
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        // Test chỉ làm việc với repository; không để scheduler đụng vào hàng
        "walletflow.bank.status-check.enabled=false"
})
@Testcontainers
class ProviderTransactionStateGuardIT {

    private static final List<String> ALL_STATUSES = List.of("NEW", "PENDING", "SUCCEEDED", "FAILED");
    private static final String ORIGINAL_FAILURE_CODE = "ORIGINAL_CODE";

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("railsdb");

    @SuppressWarnings("resource")
    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    ProviderTransactionRepository repository;
    @Autowired
    PlatformTransactionManager transactionManager;
    @Autowired
    JdbcTemplate jdbcTemplate;

    BankDb db;
    UUID tenant;

    @BeforeEach
    void reset() {
        db = new BankDb(jdbcTemplate);
        db.clean();
        tenant = UUID.randomUUID();
    }

    /** Một chuyển trạng thái của repository, gọi với tenant + id hàng. */
    @FunctionalInterface
    interface Transition {
        int apply(ProviderTransactionRepository repository, UUID tenantId, UUID id);
    }

    /** Tên chuyển trạng thái, trạng thái nguồn duy nhất cho phép, trạng thái đích. */
    record Move(String name, String source, String target, Transition transition) {
        @Override
        public String toString() {
            return name;
        }
    }

    static final List<Move> MOVES = List.of(
            new Move("markPending", "NEW", "PENDING",
                    (repo, tenantId, id) -> repo.markPending(tenantId, id, "REF-" + id, "http://pay.example/x",
                            "{\"late\":\"created\"}", 30)),
            new Move("markSucceeded", "PENDING", "SUCCEEDED",
                    (repo, tenantId, id) -> repo.markSucceeded(tenantId, id, "{\"late\":\"succeeded\"}")),
            new Move("markFailed(từ NEW)", "NEW", "FAILED",
                    (repo, tenantId, id) -> repo.markFailed(tenantId, id, "NEW", "LATE_DECLINE",
                            "{\"late\":\"declined\"}")),
            new Move("markFailed(từ PENDING)", "PENDING", "FAILED",
                    (repo, tenantId, id) -> repo.markFailed(tenantId, id, "PENDING", "LATE_FAILURE",
                            "{\"late\":\"failed\"}")));

    static Stream<Arguments> moves() {
        return MOVES.stream().map(Arguments::of);
    }

    /** Mọi cặp (chuyển trạng thái, trạng thái hiện tại của hàng) mà hàng KHÔNG ở trạng thái nguồn. */
    static Stream<Arguments> nonSourceCases() {
        return MOVES.stream().flatMap(move -> ALL_STATUSES.stream()
                .filter(status -> !status.equals(move.source()))
                .map(status -> Arguments.of(move, status)));
    }

    // ------------------------------------------------------------------
    // Không phải trạng thái nguồn -> không đổi gì
    // ------------------------------------------------------------------
    @ParameterizedTest(name = "{0} trên hàng đang {1}")
    @MethodSource("nonSourceCases")
    @DisplayName("⭐ Chuyển trạng thái trên hàng KHÔNG ở trạng thái nguồn: trả 0 và hàng nguyên vẹn đến từng cột")
    void chuyen_trang_thai_sai_nguon_tra_0_va_hang_nguyen_ven(Move move, String currentStatus) {
        UUID id = rowIn(currentStatus);
        Map<String, Object> before = db.rowById(id);

        int changed = inTransaction(() -> move.transition().apply(repository, tenant, id));

        assertThat(changed).as("%s trên hàng %s phải là no-op", move, currentStatus).isZero();
        assertThat(db.rowById(id))
                .as("hàng %s không được đổi bởi %s (status, failure_code, provider_ref, raw_response, updated_at...)",
                        currentStatus, move)
                .isEqualTo(before);
    }

    @Test
    @DisplayName("⭐ Hàng đã SUCCEEDED không bị lật sang FAILED dù câu trả lời muộn khai nguồn là PENDING hay NEW")
    void hang_da_thanh_cong_khong_bi_lat_sang_that_bai() {
        UUID id = rowIn("SUCCEEDED");
        Map<String, Object> before = db.rowById(id);

        int fromPending = inTransaction(() ->
                repository.markFailed(tenant, id, "PENDING", "LATE_FAILURE", "{\"late\":true}"));
        int fromNew = inTransaction(() ->
                repository.markFailed(tenant, id, "NEW", "LATE_DECLINE", "{\"late\":true}"));

        assertThat(fromPending).isZero();
        assertThat(fromNew).isZero();
        Map<String, Object> after = db.rowById(id);
        assertThat(after.get("status")).isEqualTo("SUCCEEDED");
        assertThat(after.get("failure_code")).isNull();
        assertThat(after.get("provider_ref")).isEqualTo(before.get("provider_ref"));
        assertThat(after).isEqualTo(before);
    }

    @Test
    @DisplayName("Hàng đã FAILED giữ nguyên mã lỗi gốc khi có markFailed muộn (mã lỗi đầu tiên là sự thật)")
    void hang_da_that_bai_giu_ma_loi_goc() {
        UUID id = rowIn("FAILED");

        int fromNew = inTransaction(() -> repository.markFailed(tenant, id, "NEW", "OTHER", "{\"x\":1}"));
        int fromPending = inTransaction(() -> repository.markFailed(tenant, id, "PENDING", "OTHER", "{\"x\":1}"));

        assertThat(fromNew).isZero();
        assertThat(fromPending).isZero();
        assertThat(db.rowById(id).get("failure_code")).isEqualTo(ORIGINAL_FAILURE_CODE);
    }

    // ------------------------------------------------------------------
    // Đối chứng dương + tenant
    // ------------------------------------------------------------------
    @ParameterizedTest(name = "{0}")
    @MethodSource("moves")
    @DisplayName("Đối chứng: từ ĐÚNG trạng thái nguồn thì trả 1 và hàng sang trạng thái đích (test trên không xanh vì UPDATE chẳng khớp gì)")
    void tu_dung_trang_thai_nguon_tra_1_va_sang_trang_thai_dich(Move move) {
        UUID id = rowIn(move.source());

        int changed = inTransaction(() -> move.transition().apply(repository, tenant, id));

        assertThat(changed).isEqualTo(1);
        assertThat(db.rowById(id).get("status")).isEqualTo(move.target());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("moves")
    @DisplayName("Đúng id, đúng trạng thái nguồn nhưng SAI tenant: trả 0, hàng nguyên vẹn (không chuyển trạng thái chéo tenant)")
    void sai_tenant_tra_0_va_hang_nguyen_ven(Move move) {
        UUID id = rowIn(move.source());
        Map<String, Object> before = db.rowById(id);

        int changed = inTransaction(() -> move.transition().apply(repository, UUID.randomUUID(), id));

        assertThat(changed).isZero();
        assertThat(db.rowById(id)).isEqualTo(before);
    }

    // ------------------------------------------------------------------
    // Schema
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Cột failure_code dài đúng 32: khớp giới hạn cắt mã lỗi ở ProviderResponses.failureCode (ProviderResponsesTest)")
    void cot_failure_code_dai_32() {
        assertThat(db.varcharLength("provider_transactions", "failure_code")).isEqualTo(32);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    /** Dựng một hàng ở đúng trạng thái, thoả các CHECK của V1. */
    private UUID rowIn(String status) {
        String providerRef = switch (status) {
            case "PENDING", "SUCCEEDED" -> "REF-" + UUID.randomUUID();
            default -> null;
        };
        String failureCode = "FAILED".equals(status) ? ORIGINAL_FAILURE_CODE : null;
        return db.insertRow(tenant, UUID.randomUUID(), UUID.randomUUID(), 5_000, "mock", status, providerRef,
                failureCode);
    }

    /** Các câu UPDATE của repository cần transaction (chúng chạy trong TransactionTemplate của service). */
    private int inTransaction(java.util.function.IntSupplier work) {
        return new TransactionTemplate(transactionManager).execute(status -> work.getAsInt());
    }
}
