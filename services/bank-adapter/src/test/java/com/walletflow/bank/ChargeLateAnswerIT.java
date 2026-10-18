package com.walletflow.bank;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.services.ChargeService;
import com.walletflow.bank.support.Await;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.BankFixtures;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.tenant.TenantContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Câu trả lời của provider đến SAU KHI hàng đã bị chuyển đi nơi khác (ADR-016, {@code ChargeService.record}).
 *
 * <p>Giữa lúc ta gọi provider (ngoài transaction DB) và lúc ghi câu trả lời, một đường khác có thể đã chốt hàng
 * — lệnh gửi lại chạy song song, hoặc job đối chiếu. Câu trả lời muộn không được ghi đè: {@code charge()} phải
 * trả về trạng thái HIỆN TẠI trong DB và event phát ra phải ứng với trạng thái đó, không phải với thứ luồng này
 * vừa nhận. Nếu không, saga có thể nghe "đang chờ" về một lần thu đã thất bại (hoặc ngược lại).
 *
 * <p>Xác định, không ngủ để đồng bộ: mock giữ lời gọi {@code charge} 2 giây (hạn chờ của lớp bọc là 10 giây),
 * test poll tới khi mock báo ĐÃ nhận lệnh — lúc đó hàng NEW đã commit — rồi mới chuyển hàng bằng SQL trong
 * lúc lời gọi còn đang treo.
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
        "walletflow.bank.status-check.interval-ms=3600000",
        "walletflow.bank.status-check.recheck-after=PT30S",
        // Rộng hơn nhiều so với độ trễ 2s của mock: lời gọi chậm nhưng KHÔNG hết giờ
        "walletflow.bank.resilience.timeout=10s"
})
@Testcontainers
class ChargeLateAnswerIT {

    private static final Duration PROVIDER_LATENCY = Duration.ofSeconds(2);

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
    ChargeService chargeService;
    @Autowired
    MockProvider mock;
    @Autowired
    ResilientPaymentProvider resilientProvider;
    @Autowired
    JdbcTemplate jdbcTemplate;

    BankDb db;
    UUID tenant;
    ExecutorService pool;

    @BeforeEach
    void reset() {
        db = new BankDb(jdbcTemplate);
        db.clean();
        mock.reset();
        resilientProvider.circuitBreaker().reset();
        tenant = UUID.randomUUID();
        pool = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void shutdown() {
        pool.shutdownNow();
    }

    @Test
    @DisplayName("⭐ Provider trả 'đã tạo phiên' nhưng hàng đã bị chốt FAILED trong lúc chờ: charge() trả FAILED hiện tại, "
            + "hàng không bị ghi đè, event phát ra là 'failed' (không phải 'pending')")
    void cau_tra_loi_created_den_muon_khong_ghi_de_hang_da_failed() throws Exception {
        BankChargeCommand command = BankFixtures.freshCharge(5_000);
        mock.setLatency(PROVIDER_LATENCY);

        Future<ProviderTransaction> inFlight = pool.submit(() -> charge(command));
        Await.until(() -> mock.chargeCalls() == 1, "provider đã nhận lệnh charge (lời gọi đang treo ở mock)");
        assertThat(db.status(tenant, command.transactionId()))
                .as("tiền điều kiện: hàng đã được giành quyền (NEW) trước khi gọi provider")
                .isEqualTo("NEW");

        // Một đường khác chốt hàng trong lúc lời gọi còn treo
        int moved = jdbcTemplate.update("UPDATE provider_transactions SET status = 'FAILED', "
                        + "failure_code = 'ELSEWHERE', raw_response = CAST('{\"by\":\"other-path\"}' AS jsonb), "
                        + "next_check_at = NULL, updated_at = now() "
                        + "WHERE tenant_id = ? AND transaction_id = ? AND status = 'NEW'",
                tenant, command.transactionId());
        assertThat(moved).as("tiền điều kiện: hàng được chuyển KHI lời gọi provider còn đang treo").isEqualTo(1);

        ProviderTransaction result = inFlight.get(30, TimeUnit.SECONDS);

        assertThat(mock.sessionCount())
                .as("provider thật sự đã tạo phiên và trả 'Created' — đây đúng là câu trả lời đến muộn")
                .isEqualTo(1);
        assertThat(result.getStatus()).as("trả về trạng thái HIỆN TẠI trong DB").isEqualTo(EProviderTxnStatus.FAILED);
        assertThat(result.getFailureCode()).isEqualTo("ELSEWHERE");
        assertThat(result.getProviderRef()).isNull();

        Map<String, Object> row = db.row(tenant, command.transactionId());
        assertThat(row.get("status")).isEqualTo("FAILED");
        assertThat(row.get("failure_code")).isEqualTo("ELSEWHERE");
        assertThat(row.get("provider_ref")).as("markPending trả 0, không ghi provider_ref muộn").isNull();
        assertThat(row.get("payment_url")).isNull();
        assertThat(String.valueOf(row.get("raw_response"))).contains("other-path");

        assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING))
                .as("không phát 'pending' cho một lần thu đã thất bại").isEmpty();
        assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxPayloads(command.transactionId(), WalletTopics.EVT_BANK_FAILED))
                .as("event phát ra ứng với trạng thái HIỆN TẠI (FAILED, mã lỗi do đường kia ghi)")
                .singleElement()
                .satisfies(payload -> assertThat(payload).contains("ELSEWHERE"));
    }

    @Test
    @DisplayName("⭐ Provider trả 'từ chối' nhưng hàng đã được chốt PENDING trong lúc chờ: charge() trả PENDING hiện tại "
            + "(đúng provider_ref của đường kia), không bị FAILED ghi đè, event là 'pending'")
    void cau_tra_loi_declined_den_muon_khong_ghi_de_hang_da_pending() throws Exception {
        BankChargeCommand command = BankFixtures.freshCharge(5_000);
        mock.setLatency(PROVIDER_LATENCY);
        mock.setDeclineAboveMinor(1_000);   // 5.000 > 1.000: lời gọi này sẽ nhận 'Declined'

        Future<ProviderTransaction> inFlight = pool.submit(() -> charge(command));
        Await.until(() -> mock.chargeCalls() == 1, "provider đã nhận lệnh charge (lời gọi đang treo ở mock)");

        int moved = jdbcTemplate.update("UPDATE provider_transactions SET status = 'PENDING', "
                        + "provider_ref = 'OTHER-REF', payment_url = 'http://pay.example/OTHER-REF', "
                        + "raw_response = CAST('{\"by\":\"other-path\"}' AS jsonb), "
                        + "next_check_at = now() + interval '30 seconds', updated_at = now() "
                        + "WHERE tenant_id = ? AND transaction_id = ? AND status = 'NEW'",
                tenant, command.transactionId());
        assertThat(moved).as("tiền điều kiện: hàng được chuyển KHI lời gọi provider còn đang treo").isEqualTo(1);

        ProviderTransaction result = inFlight.get(30, TimeUnit.SECONDS);

        assertThat(result.getStatus()).isEqualTo(EProviderTxnStatus.PENDING);
        assertThat(result.getProviderRef()).isEqualTo("OTHER-REF");
        assertThat(result.getFailureCode()).isNull();

        Map<String, Object> row = db.row(tenant, command.transactionId());
        assertThat(row.get("status")).as("markFailed(từ NEW) trả 0, không lật PENDING thành FAILED").isEqualTo("PENDING");
        assertThat(row.get("failure_code")).isNull();
        assertThat(row.get("provider_ref")).isEqualTo("OTHER-REF");

        assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_FAILED))
                .as("không phát 'failed' cho một lần thu đang chờ").isEmpty();
        assertThat(db.outboxPayloads(command.transactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING))
                .singleElement()
                .satisfies(payload -> assertThat(payload).contains("OTHER-REF"));
    }

    private ProviderTransaction charge(BankChargeCommand command) {
        return TenantContext.callWith(tenant, () -> chargeService.charge(command));
    }
}
