package com.walletflow.bank;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.provider.ProviderUnavailableException;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.bank.services.ChargeService;
import com.walletflow.bank.services.ChargeStatusService;
import com.walletflow.bank.support.Await;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.BankFixtures;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ConflictException;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tính chất tiền quan trọng nhất của bank-adapter: <b>một lệnh thu, tối đa MỘT phiên ở provider</b>, dù lệnh
 * bị gửi lại, câu trả lời bị mất, provider timeout hay nhiều luồng đua nhau (ADR-016).
 *
 * <p>Gọi thẳng {@link ChargeService} trong {@link TenantContext} (đúng như listener làm), với
 * {@link MockProvider} thật làm provider giả và Postgres thật. Thước đo "thu hai lần" là
 * {@code MockProvider.sessionCount()} — số phiên thật sự tồn tại ở phía provider, không phải số lần ta
 * tưởng là đã gọi.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "outbox.relay.delay-ms=300",
        "walletflow.bank.provider=mock",
        // Mock cần xác nhận thứ hai (PaymentProviderConfig) — chỉ stack dev/test bật
        "walletflow.bank.mock.allowed=true",
        // Rỗng -> null: phiên chỉ đổi trạng thái khi test gọi settle (PaymentProviderConfigTest chứng minh)
        "walletflow.bank.mock.auto-settle-after=",
        // Gọi reconcile()/refresh() trực tiếp, không đua với scheduler
        "walletflow.bank.status-check.interval-ms=3600000",
        "walletflow.bank.status-check.recheck-after=PT30S",
        "walletflow.bank.resilience.timeout=1s",
        "walletflow.kafka.retry.max-attempts=2",
        "walletflow.kafka.retry.initial-interval-ms=200",
        "walletflow.kafka.retry.max-interval-ms=400"
})
@Testcontainers
class ChargeIdempotencyIT {

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
    ChargeStatusService chargeStatusService;
    @Autowired
    ProviderTransactionRepository repository;
    @Autowired
    MockProvider mock;
    @Autowired
    ResilientPaymentProvider resilientProvider;
    @Autowired
    JdbcTemplate jdbcTemplate;

    BankDb db;
    UUID tenant;

    @BeforeEach
    void reset() {
        db = new BankDb(jdbcTemplate);
        db.clean();
        mock.reset();
        resilientProvider.circuitBreaker().reset();
        tenant = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // Gửi lại
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Gửi lại cùng lệnh: provider chỉ bị gọi MỘT lần, một hàng, một phiên; hai event pending cùng eventId")
    void gui_lai_cung_lenh_chi_goi_provider_mot_lan() {
        BankChargeCommand command = BankFixtures.freshCharge(5_000);

        ProviderTransaction first = charge(tenant, command);
        ProviderTransaction second = charge(tenant, command);

        assertThat(first.getStatus()).isEqualTo(EProviderTxnStatus.PENDING);
        assertThat(second.getStatus()).isEqualTo(EProviderTxnStatus.PENDING);
        assertThat(second.getProviderRef()).isEqualTo(first.getProviderRef());
        assertThat(second.getPaymentUrl()).isEqualTo(first.getPaymentUrl()).isNotBlank();

        assertThat(mock.chargeCalls()).as("lệnh gửi lại KHÔNG được gọi provider lần nữa").isEqualTo(1);
        assertThat(mock.sessionCount()).isEqualTo(1);
        assertThat(db.rowCount(tenant, command.transactionId())).isEqualTo(1);

        // Mỗi lần nhận lệnh đều có câu trả lời (saga gửi lại vì chưa nghe được câu trả lời trước), nhưng
        // cùng eventId để consumer dedup được
        List<UUID> pendingEvents = db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING);
        assertThat(pendingEvents).hasSize(2);
        assertThat(new HashSet<>(pendingEvents)).as("cùng eventId ở cả hai lần phát").hasSize(1);
    }

    @Test
    @DisplayName("⭐ Mất câu trả lời của provider: lần đầu nổ, hàng ở NEW; thử lại vẫn đúng MỘT phiên và provider_ref khớp phiên đó")
    void mat_cau_tra_loi_thu_lai_van_mot_phien() {
        BankChargeCommand command = BankFixtures.freshCharge(12_000);
        mock.loseNextChargeResponses(1);

        assertThatThrownBy(() -> charge(tenant, command))
                .as("không biết provider đã nhận chưa -> báo lỗi kỹ thuật để Kafka thử lại, KHÔNG báo thất bại")
                .isInstanceOf(ProviderUnavailableException.class);

        assertThat(db.status(tenant, command.transactionId())).isEqualTo("NEW");
        assertThat(db.providerRef(tenant, command.transactionId())).isNull();
        assertThat(mock.sessionCount())
                .as("provider ĐÃ tạo phiên dù câu trả lời không về tới")
                .isEqualTo(1);
        assertThat(db.outboxCountAnyTopic(command.transactionId()))
                .as("chưa có câu trả lời nào của provider thì không phát event nào (nhất là không phải 'failed')")
                .isZero();

        ProviderTransaction retried = charge(tenant, command);

        assertThat(retried.getStatus()).isEqualTo(EProviderTxnStatus.PENDING);
        assertThat(mock.chargeCalls()).as("một lần mất câu trả lời + một lần thử lại").isEqualTo(2);
        assertThat(mock.sessionCount())
                .as("thử lại phải gửi ĐÚNG khoá cũ nên provider trả phiên đã tạo, không tạo phiên thứ hai")
                .isEqualTo(1);
        // Mock chỉ biết phiên duy nhất đó: hỏi bằng provider_ref trong DB mà không nổ nghĩa là hàng trỏ đúng phiên
        String providerRef = db.providerRef(tenant, command.transactionId());
        assertThat(providerRef).isEqualTo(retried.getProviderRef()).isNotBlank();
        assertThat(mock.getStatus(providerRef).state()).isEqualTo(EProviderState.PENDING);
        assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING)).hasSize(1);
    }

    @Test
    @DisplayName("Provider lỗi kỹ thuật TRƯỚC khi làm gì: hàng ở NEW (không phải FAILED); thử lại thành công, một phiên")
    void provider_loi_truoc_khi_nhan_thi_hang_o_new_roi_thu_lai_thanh_cong() {
        BankChargeCommand command = BankFixtures.freshCharge(3_000);
        mock.failNextCalls(1);

        assertThatThrownBy(() -> charge(tenant, command)).isInstanceOf(ProviderUnavailableException.class);

        assertThat(db.status(tenant, command.transactionId())).isEqualTo("NEW");
        assertThat(mock.sessionCount()).isZero();
        assertThat(db.outboxCountAnyTopic(command.transactionId())).isZero();

        ProviderTransaction retried = charge(tenant, command);

        assertThat(retried.getStatus()).isEqualTo(EProviderTxnStatus.PENDING);
        assertThat(mock.sessionCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("⭐ Timeout (provider chậm hơn hạn chờ) rồi thử lại: provider vẫn tạo phiên muộn, nhưng tổng cộng đúng MỘT phiên")
    void timeout_roi_thu_lai_van_mot_phien() {
        BankChargeCommand command = BankFixtures.freshCharge(8_000);
        mock.setLatency(Duration.ofSeconds(2));   // > walletflow.bank.resilience.timeout = 1s

        assertThatThrownBy(() -> charge(tenant, command))
                .isInstanceOf(ProviderUnavailableException.class)
                .hasMessageContaining("quá thời gian chờ");
        assertThat(db.status(tenant, command.transactionId()))
                .as("timeout ≠ thất bại: hàng phải còn NEW")
                .isEqualTo("NEW");

        // Phía provider vẫn làm nốt việc sau khi ta bỏ cuộc: phiên được tạo muộn
        Await.until(() -> mock.sessionCount() == 1, "provider tạo phiên sau khi ta đã hết giờ chờ");

        mock.setLatency(Duration.ZERO);
        ProviderTransaction retried = charge(tenant, command);

        assertThat(retried.getStatus()).isEqualTo(EProviderTxnStatus.PENDING);
        assertThat(mock.sessionCount()).as("thử lại sau timeout không tạo phiên thứ hai").isEqualTo(1);
        assertThat(db.rowCount(tenant, command.transactionId())).isEqualTo(1);
        assertThat(mock.getStatus(retried.getProviderRef()).state()).isEqualTo(EProviderState.PENDING);
    }

    // ------------------------------------------------------------------
    // Đồng thời
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ 10 lệnh giống hệt nhau ĐỒNG THỜI: đúng một hàng, một phiên, cùng provider_ref, cùng eventId")
    void muoi_lenh_trung_dong_thoi_chi_mot_hang_mot_phien() throws Exception {
        BankChargeCommand command = BankFixtures.freshCharge(20_000);
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<ProviderTransaction>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return charge(tenant, command);
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();

            List<ProviderTransaction> results = new ArrayList<>();
            for (Future<ProviderTransaction> future : futures) {
                results.add(future.get(60, TimeUnit.SECONDS));
            }

            assertThat(db.rowCount(tenant, command.transactionId())).as("một hàng cho một lệnh").isEqualTo(1);
            assertThat(mock.sessionCount())
                    .as("nhiều luồng cùng thấy hàng NEW có thể cùng gọi provider, nhưng cùng khoá -> một phiên")
                    .isEqualTo(1);
            assertThat(results).allSatisfy(result ->
                    assertThat(result.getStatus()).isEqualTo(EProviderTxnStatus.PENDING));
            assertThat(results.stream().map(ProviderTransaction::getProviderRef).distinct().toList())
                    .as("mọi luồng nhận cùng một provider_ref")
                    .hasSize(1);
            List<UUID> eventIds = db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING);
            assertThat(eventIds).isNotEmpty();
            assertThat(new HashSet<>(eventIds)).as("mọi lần phát cùng một eventId").hasSize(1);
        } finally {
            pool.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // Tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Cùng transactionId ở hai tenant: hai hàng, hai phiên riêng, đọc không chéo tenant")
    void cung_transaction_id_o_hai_tenant_la_hai_lan_thu_rieng() {
        BankChargeCommand command = BankFixtures.freshCharge(15_000);
        UUID tenantA = UUID.randomUUID();
        UUID tenantB = UUID.randomUUID();

        ProviderTransaction a = charge(tenantA, command);
        ProviderTransaction b = charge(tenantB, command);

        assertThat(db.rowCountAnyTenant(command.transactionId())).isEqualTo(2);
        assertThat(mock.sessionCount())
                .as("khoá idempotency gửi provider có tenant trong đó — hai tenant không dùng chung phiên")
                .isEqualTo(2);
        assertThat(a.getProviderRef()).isNotEqualTo(b.getProviderRef());
        assertThat(a.getIdempotencyKey()).isNotEqualTo(b.getIdempotencyKey());

        ProviderTransaction readA = repository.findByTenantIdAndTransactionIdAndOperation(
                tenantA, command.transactionId(), EBankOperation.CHARGE).orElseThrow();
        ProviderTransaction readB = repository.findByTenantIdAndTransactionIdAndOperation(
                tenantB, command.transactionId(), EBankOperation.CHARGE).orElseThrow();
        assertThat(readA.getTenantId()).isEqualTo(tenantA);
        assertThat(readA.getProviderRef()).isEqualTo(a.getProviderRef());
        assertThat(readB.getTenantId()).isEqualTo(tenantB);
        assertThat(readB.getProviderRef()).isEqualTo(b.getProviderRef());
        assertThat(repository.findByTenantIdAndTransactionIdAndOperation(
                UUID.randomUUID(), command.transactionId(), EBankOperation.CHARGE))
                .as("tenant thứ ba không thấy lần thu nào của giao dịch này")
                .isEmpty();

        List<Map<String, Object>> events = db.outbox(command.transactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING);
        assertThat(events).extracting(row -> row.get("tenant_id")).containsExactlyInAnyOrder(tenantA, tenantB);
        assertThat(events).extracting(row -> row.get("event_id")).doesNotHaveDuplicates();
    }

    // ------------------------------------------------------------------
    // Lệnh sai
    // ------------------------------------------------------------------
    @ParameterizedTest(name = "đổi {0}")
    @ValueSource(strings = {"số tiền", "ví", "loại tiền"})
    @DisplayName("Cùng giao dịch mà khác ví/số tiền/loại tiền: BANK_CHARGE_MISMATCH, hàng cũ và provider không bị chạm")
    void lenh_khac_noi_dung_cung_giao_dich_bi_tu_choi(String khacBiet) {
        BankChargeCommand original = BankFixtures.freshCharge(5_000);
        ProviderTransaction first = charge(tenant, original);

        BankChargeCommand mismatched = switch (khacBiet) {
            case "số tiền" -> BankFixtures.charge(original.transactionId(), original.walletId(), 9_000);
            case "ví" -> BankFixtures.charge(original.transactionId(), UUID.randomUUID(), original.amountMinor());
            default -> new BankChargeCommand(original.transactionId(), original.walletId(),
                    original.amountMinor(), ECurrency.USD);
        };

        assertThatThrownBy(() -> charge(tenant, mismatched))
                .isInstanceOfSatisfying(ConflictException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo("BANK_CHARGE_MISMATCH"));

        Map<String, Object> row = db.row(tenant, original.transactionId());
        assertThat(row.get("amount_minor")).isEqualTo(5_000L);
        assertThat(row.get("wallet_id")).isEqualTo(original.walletId());
        assertThat(row.get("currency")).isEqualTo("VND");
        assertThat(row.get("status")).isEqualTo("PENDING");
        assertThat(row.get("provider_ref")).isEqualTo(first.getProviderRef());
        assertThat(mock.chargeCalls()).isEqualTo(1);
        assertThat(mock.sessionCount()).isEqualTo(1);
        assertThat(db.outboxEventIds(original.transactionId(), WalletTopics.EVT_BANK_CHARGE_PENDING))
                .as("lệnh sai không được phát thêm event nào")
                .hasSize(1);
    }

    @Test
    @DisplayName("Lần thu dở dang thuộc provider khác: BANK_PROVIDER_MISMATCH, không gọi provider đang chạy")
    void lan_thu_do_dang_cua_provider_khac_bi_tu_choi() {
        BankChargeCommand command = BankFixtures.freshCharge(5_000);
        db.insertNewRowOfProvider(tenant, command.transactionId(), command.walletId(), 5_000, "vnpay");

        assertThatThrownBy(() -> charge(tenant, command))
                .as("khoá idempotency chỉ có nghĩa ở provider cũ: gửi sang mock là có thể thu hai lần")
                .isInstanceOfSatisfying(ConflictException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo("BANK_PROVIDER_MISMATCH"));

        assertThat(mock.chargeCalls()).isZero();
        assertThat(db.status(tenant, command.transactionId())).isEqualTo("NEW");
    }

    @ParameterizedTest(name = "số tiền {0}")
    @ValueSource(longs = {0, -1, -5_000})
    @DisplayName("Số tiền không dương: BANK_CHARGE_INVALID, không tạo hàng, không gọi provider")
    void so_tien_khong_duong_bi_tu_choi(long amountMinor) {
        BankChargeCommand command = BankFixtures.freshCharge(amountMinor);

        assertThatThrownBy(() -> charge(tenant, command))
                .isInstanceOfSatisfying(BadRequestException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo("BANK_CHARGE_INVALID"));

        assertThat(db.rowCountAnyTenant(command.transactionId())).isZero();
        assertThat(mock.chargeCalls()).isZero();
        assertThat(db.outboxCountAnyTopic(command.transactionId())).isZero();
    }

    // ------------------------------------------------------------------
    // Gửi lại sau khi đã có kết quả cuối
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Gửi lại sau SUCCEEDED: phát lại 'completed' cùng eventId, KHÔNG gọi provider")
    void gui_lai_sau_thanh_cong_phat_lai_completed_cung_event_id() {
        BankChargeCommand command = BankFixtures.freshCharge(50_000);
        ProviderTransaction pending = charge(tenant, command);
        mock.settle(pending.getProviderRef(), EProviderState.SUCCEEDED);
        EProviderTxnStatus afterRefresh = TenantContext.callWith(tenant, () -> chargeStatusService.refresh(pending));
        assertThat(afterRefresh).isEqualTo(EProviderTxnStatus.SUCCEEDED);
        assertThat(db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_COMPLETED)).hasSize(1);
        int callsBefore = mock.chargeCalls();

        ProviderTransaction resent = charge(tenant, command);

        assertThat(resent.getStatus()).isEqualTo(EProviderTxnStatus.SUCCEEDED);
        assertThat(mock.chargeCalls()).as("đã có kết quả cuối thì không hỏi provider thêm").isEqualTo(callsBefore);
        assertThat(mock.sessionCount()).isEqualTo(1);
        List<UUID> completed = db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_COMPLETED);
        assertThat(completed).as("saga gửi lại vì chưa nghe câu trả lời -> phải được trả lời lại").hasSize(2);
        assertThat(new HashSet<>(completed)).as("cùng eventId để consumer dedup").hasSize(1);
    }

    @Test
    @DisplayName("Provider từ chối rồi gửi lại: kết quả FAILED là cuối cùng — không gọi provider lần hai, phát lại 'failed' cùng eventId")
    void gui_lai_sau_tu_choi_khong_goi_lai_provider() {
        BankChargeCommand command = BankFixtures.freshCharge(5_000);
        mock.setDeclineAboveMinor(1_000);

        ProviderTransaction declined = charge(tenant, command);
        assertThat(declined.getStatus()).isEqualTo(EProviderTxnStatus.FAILED);
        assertThat(declined.getProviderRef()).isNull();
        assertThat(declined.getFailureCode()).isEqualTo("AMOUNT_LIMIT_EXCEEDED");

        mock.setDeclineAboveMinor(0);   // provider "đổi ý" — lệnh cũ vẫn phải giữ nguyên kết luận
        ProviderTransaction resent = charge(tenant, command);

        assertThat(resent.getStatus()).isEqualTo(EProviderTxnStatus.FAILED);
        assertThat(mock.chargeCalls()).isEqualTo(1);
        assertThat(mock.sessionCount()).isZero();
        List<UUID> failed = db.outboxEventIds(command.transactionId(), WalletTopics.EVT_BANK_FAILED);
        assertThat(failed).hasSize(2);
        assertThat(new HashSet<>(failed)).hasSize(1);
    }

    private ProviderTransaction charge(UUID tenantId, BankChargeCommand command) {
        return TenantContext.callWith(tenantId, () -> chargeService.charge(command));
    }
}
