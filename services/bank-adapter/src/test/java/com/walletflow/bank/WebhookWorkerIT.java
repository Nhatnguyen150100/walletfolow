package com.walletflow.bank;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.services.ChargeService;
import com.walletflow.bank.services.PendingChargeReconciler;
import com.walletflow.bank.services.WebhookIngestService;
import com.walletflow.bank.services.WebhookWorker;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.BankFixtures;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.tenant.TenantContext;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Worker xử lý webhook đã nhận (roadmap 2.3b, ADR-018 §2 và §4).
 *
 * <p><b>Tính chất đắt nhất ở đây: webhook chỉ KÍCH HOẠT việc hỏi lại, không bao giờ là căn cứ ghi tiền.</b>
 * Một IPN hợp lệ nói "SUCCEEDED" mà provider vẫn nói "chưa thu" thì không một đồng nào được ghi — nếu không,
 * ai ký được một webhook là nạp được tiền vào ví. Test đầu tiên trong file chứng minh đúng chuyện đó.
 *
 * <p>Job đối chiếu bị TẮT HẲN ({@code status-check.enabled=false}) để không có đường nào khác chốt kết quả:
 * mọi thay đổi trạng thái quan sát được trong file này đều do worker gây ra.
 *
 * <p>Scheduler bị đẩy ra xa (interval 1 giờ) và test gọi {@code worker.process()} trực tiếp; hàng được làm
 * "tới hạn" bằng cách lùi {@code next_attempt_at} trong DB, không chờ giờ thật.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "outbox.relay.delay-ms=300",
        "walletflow.bank.provider=mock",
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        "walletflow.bank.mock.webhook-secret=" + WebhookWorkerIT.SECRET,
        // Không có lưới an toàn nào: kết quả tới được là nhờ worker, không nhờ job đối chiếu
        "walletflow.bank.status-check.enabled=false",
        "walletflow.bank.webhook.interval-ms=3600000",
        // 30s thật: hàng vừa thử CHƯA tới hạn lại, và "lùi lịch" nhìn thấy được (next_attempt_at > now())
        "walletflow.bank.webhook.retry-after=PT30S",
        "walletflow.bank.webhook.max-attempts=3",
        "walletflow.bank.resilience.timeout=1s"
})
@Testcontainers
class WebhookWorkerIT {

    /** Khoá ký của mock trong test. Fixture bịa, không mở được gì. */
    static final String SECRET = "wf-test-worker-secret";

    private static final String FAILURE_METRIC = "walletflow.bank.webhook.failures";
    private static final String ABANDONED_METRIC = "walletflow.bank.webhook.abandoned";
    private static final String UNRESOLVED_METRIC = "walletflow.bank.webhook.unresolved";
    private static final String NEEDS_REVIEW_METRIC = "walletflow.bank.expired_needs_review";

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
    WebhookWorker worker;
    @Autowired
    WebhookIngestService ingestService;
    @Autowired
    ChargeService chargeService;
    @Autowired
    MockProvider mock;
    @Autowired
    ResilientPaymentProvider resilientProvider;
    @Autowired
    MeterRegistry meterRegistry;
    @Autowired
    ApplicationContext context;
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
    // Webhook là gợi ý, không phải sự thật
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Webhook ký đúng nói ĐÃ THU nhưng provider vẫn nói chưa: KHÔNG ghi tiền — webhook chỉ kích hoạt "
            + "việc hỏi lại, không bao giờ là căn cứ")
    void webhook_noi_thanh_cong_ma_provider_noi_chua_thi_khong_ghi_tien() {
        ProviderTransaction row = createPending(50_000);
        // Phiên ở mock KHÔNG được settle: provider sẽ trả lời "vẫn PENDING"
        accept(row.getProviderRef(), "SUCCEEDED");

        assertThat(worker.process()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId()))
                .as("tin payload là biến mọi người ký được webhook thành người nạp được tiền")
                .isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();

        Map<String, Object> webhook = db.webhookOfRef(row.getProviderRef());
        assertThat(webhook.get("processed_at")).as("đã hỏi xong, dù câu trả lời là 'chưa thu'").isNotNull();
        assertThat(webhook.get("outcome")).isEqualTo("RESOLVED");
        assertThat(webhook.get("tenant_id")).as("tenant tra từ provider_ref, không từ payload").isEqualTo(tenant);
    }

    @Test
    @DisplayName("⭐ Provider xác nhận đã thu: worker chốt SUCCEEDED và phát ĐÚNG MỘT bank.event.completed — không cần job đối chiếu")
    void provider_xac_nhan_thi_worker_chot_ket_qua() {
        ProviderTransaction row = createPending(70_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        accept(row.getProviderRef(), "SUCCEEDED");

        assertThat(worker.process()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.outbox(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED))
                .hasSize(1)
                .allSatisfy(event -> assertThat(event.get("tenant_id")).isEqualTo(tenant));
        assertThat(db.webhookOfRef(row.getProviderRef()).get("outcome")).isEqualTo("RESOLVED");
        assertThat(worker.process()).as("đã đóng thì không được lấy ra lần nữa").isZero();
        assertThat(TenantContext.current())
                .as("worker đặt tenant theo từng hàng rồi trả lại, không để tenant dính vào luồng")
                .isEmpty();
    }

    @Test
    @DisplayName("⭐ Job đối chiếu bị tắt hẳn trong test này — mọi kết quả quan sát được đều do worker gây ra")
    void khong_co_job_doi_soat_nao_chay_xen_vao() {
        assertThat(context.getBeanNamesForType(PendingChargeReconciler.class))
                .as("còn job đối chiếu thì không chứng minh được worker làm gì")
                .isEmpty();
    }

    // ------------------------------------------------------------------
    // Webhook thành công đến MUỘN, sau khi đã báo hết hạn
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Webhook báo thành công tới SAU khi lần thu đã FAILED(EXPIRED): KHÔNG ghi đè trạng thái, "
            + "bật metric cần-người-xem")
    void webhook_thanh_cong_toi_muon_khong_ghi_de_ma_keu() {
        ProviderTransaction row = createPending(80_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        // Lần thu đã bị kết luận hết hạn trước đó (event failed đã phát, saga đã kết thúc)
        db.forceFailed(row.getTransactionId(), "EXPIRED", true);
        accept(row.getProviderRef(), "SUCCEEDED");
        double reviewBefore = count(NEEDS_REVIEW_METRIC);

        assertThat(worker.process()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId()))
                .as("lật trạng thái ở đây là ghi tiền cho một saga đã đóng")
                .isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, row.getTransactionId())).isEqualTo("EXPIRED");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(count(NEEDS_REVIEW_METRIC) - reviewBefore)
                .as("người dùng có thể đã mất tiền mà ví không được cộng — phải kêu (alert ExpiredChargeNeedsReview)")
                .isEqualTo(1.0);
        assertThat(db.nextCheckIsNull(row.getTransactionId()))
                .as("webhook chỉ THÊM một lần nhìn, không lấy đi lượt hỏi xác nhận đã lên lịch (ADR-017)")
                .isFalse();
        assertThat(db.webhookOfRef(row.getProviderRef()).get("outcome")).isEqualTo("RESOLVED");
    }

    @Test
    @DisplayName("⭐ Webhook tới SỚM sau khi hết hạn, provider vẫn nói chưa thu: KHÔNG tiêu mất lượt hỏi xác nhận "
            + "đã lên lịch — tiền về muộn vẫn còn người phát hiện")
    void webhook_som_khong_tieu_mat_luot_hoi_xac_nhan_da_len_lich() {
        ProviderTransaction row = createPending(45_000);
        // Hết hạn lúc T+30', lượt hỏi xác nhận đã được đặt cho T+1h30' (next_check_at ở TƯƠNG LAI)
        db.forceFailed(row.getTransactionId(), "EXPIRED", false);
        jdbcTemplate.update("UPDATE provider_transactions SET next_check_at = now() + interval '1 hour' "
                + "WHERE transaction_id = ?", row.getTransactionId());
        // Phiên ở mock KHÔNG settle: ngân hàng chưa chốt sổ, provider nói "chưa thu" — chuyện thường ở T+31'
        accept(row.getProviderRef(), "SUCCEEDED");
        double reviewBefore = count(NEEDS_REVIEW_METRIC);

        assertThat(worker.process()).isEqualTo(1);

        assertThat(count(NEEDS_REVIEW_METRIC)).as("provider nói chưa thu thì chưa có gì để kêu").isEqualTo(reviewBefore);
        assertThat(db.nextCheckIsNull(row.getTransactionId()))
                .as("xoá lịch ở đây là đóng cửa sổ phát hiện tiền về muộn sớm cả tiếng — tiền về sau đó "
                        + "sẽ không còn ai thấy")
                .isFalse();
        assertThat(db.webhookOfRef(row.getProviderRef()).get("outcome")).isEqualTo("RESOLVED");

        // Tiền về thật sau đó, lượt hỏi theo lịch vẫn còn nguyên nên vẫn bắt được
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        db.makeExpiredConfirmationDue(row.getTransactionId());
        accept(row.getProviderRef(), "SUCCEEDED-LATE");
        assertThat(worker.process()).isEqualTo(1);
        assertThat(count(NEEDS_REVIEW_METRIC) - reviewBefore).isEqualTo(1.0);
    }

    @Test
    @DisplayName("⭐ Webhook của lần thu thuộc TÀI KHOẢN MERCHANT khác: đóng FOREIGN_ACCOUNT ngay, không hỏi provider, "
            + "không thử lại — đó là lỗi nghiệp vụ, không phải provider chết")
    void webhook_cua_tai_khoan_merchant_khac_dong_ngay() {
        ProviderTransaction row = createPending(35_000);
        db.setProviderAccount(row.getTransactionId(), "tmn-cu");
        accept(row.getProviderRef(), "SUCCEEDED");
        int providerCallsBefore = providerCalls();
        double abandonedBefore = count(ABANDONED_METRIC);

        assertThat(worker.process()).isEqualTo(1);

        Map<String, Object> webhook = db.webhookOfRef(row.getProviderRef());
        assertThat(webhook.get("outcome")).isEqualTo("FOREIGN_ACCOUNT");
        assertThat(webhook.get("processed_at")).isNotNull();
        assertThat(providerCalls()).as("hỏi tài khoản mới về phiên của tài khoản cũ là vô nghĩa")
                .isEqualTo(providerCallsBefore);
        assertThat(count(ABANDONED_METRIC))
                .as("không được bắn alert 'provider không trả lời' cho một ca đổi cấu hình")
                .isEqualTo(abandonedBefore);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("⭐ Hai luồng worker chạy SONG SONG trên cùng tồn đọng: mỗi webhook xử lý đúng một lần, "
            + "đúng một bank.event.completed cho mỗi lần thu")
    void hai_luong_worker_song_song_khong_xu_ly_trung() throws Exception {
        List<ProviderTransaction> rows = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            ProviderTransaction row = createPending(10_000 + i);
            mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
            accept(row.getProviderRef(), "SUCCEEDED");
            rows.add(row);
        }
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);

        List<Future<Integer>> results = List.of(
                pool.submit(() -> {
                    start.await();
                    return worker.process();
                }),
                pool.submit(() -> {
                    start.await();
                    return worker.process();
                }));
        start.countDown();
        int total = 0;
        for (Future<Integer> result : results) {
            total += result.get();
        }
        pool.shutdownNow();

        assertThat(total).as("SKIP LOCKED: hai luồng chia nhau lô, không luồng nào lấy lại hàng của luồng kia")
                .isEqualTo(rows.size());
        for (ProviderTransaction row : rows) {
            assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
            assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED))
                    .as("event trùng = saga chạy hai lần = cộng tiền hai lần").hasSize(1);
            assertThat(db.webhookOfRef(row.getProviderRef()).get("outcome")).isEqualTo("RESOLVED");
        }
    }

    @Test
    @DisplayName("Webhook tới cho lần thu đã SUCCEEDED: đóng lại, không phát thêm event nào")
    void webhook_toi_cho_hang_da_chot_khong_phat_them_event() {
        ProviderTransaction row = createPending(30_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        accept(row.getProviderRef(), "SUCCEEDED");
        assertThat(worker.process()).isEqualTo(1);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");

        // Provider gửi thêm một IPN mang trạng thái khác -> hàng webhook mới, vẫn phải được xử lý
        accept(row.getProviderRef(), "SUCCEEDED-AGAIN");
        assertThat(worker.process()).isEqualTo(1);

        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED))
                .as("event completed chỉ được phát đúng một lần cho một lần thu").hasSize(1);
    }

    // ------------------------------------------------------------------
    // Provider không trả lời
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Provider chết khi worker hỏi: webhook KHÔNG bị đóng, được lùi lịch, đếm lỗi; provider hồi lại thì xử lý xong")
    void provider_chet_thi_webhook_chua_dong_va_duoc_lui_lich() {
        ProviderTransaction row = createPending(40_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        accept(row.getProviderRef(), "SUCCEEDED");
        mock.failNextCalls(1);
        double failuresBefore = count(FAILURE_METRIC);

        assertThat(worker.process()).isEqualTo(1);

        Map<String, Object> webhook = db.webhookOfRef(row.getProviderRef());
        assertThat(webhook.get("processed_at"))
                .as("không hỏi được thì KHÔNG được coi là đã xử lý — tin báo sẽ mất hẳn")
                .isNull();
        assertThat(webhook.get("attempts")).isEqualTo(1);
        assertThat(db.secondsUntilNextWebhookAttempt(row.getProviderRef()))
                .as("đã lùi lịch retry-after = 30s, không bị thử dội mỗi lượt quét")
                .isBetween(25.0, 30.5);
        assertThat(count(FAILURE_METRIC) - failuresBefore).isEqualTo(1.0);
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(worker.process()).as("đã lùi lịch: lượt kế tiếp không còn gì tới hạn").isZero();

        db.makeWebhookDue(row.getProviderRef());
        assertThat(worker.process()).isEqualTo(1);

        assertThat(db.webhookOfRef(row.getProviderRef()).get("outcome")).isEqualTo("RESOLVED");
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("⭐ Hết trần 3 lượt thử mà vẫn không hỏi được: đóng ABANDONED, cộng counter, thôi thử lại")
    void het_luot_thu_thi_dong_abandoned() {
        ProviderTransaction row = createPending(60_000);
        accept(row.getProviderRef(), "SUCCEEDED");
        double abandonedBefore = count(ABANDONED_METRIC);

        // Trần max-attempts = 3: hai lượt đầu chỉ lùi lịch, lượt thứ ba đẩy attempts tới trần -> bỏ cuộc
        for (int attempt = 1; attempt <= 3; attempt++) {
            mock.failNextCalls(1);
            db.makeWebhookDue(row.getProviderRef());
            assertThat(worker.process()).isEqualTo(1);
            if (attempt < 3) {
                assertThat(db.webhookOfRef(row.getProviderRef()).get("processed_at"))
                        .as("còn lượt thì chưa được bỏ cuộc (lượt " + attempt + ")").isNull();
            }
        }

        Map<String, Object> webhook = db.webhookOfRef(row.getProviderRef());
        assertThat(webhook.get("processed_at")).isNotNull();
        assertThat(webhook.get("outcome")).isEqualTo("ABANDONED");
        assertThat(webhook.get("tenant_id")).as("chưa bao giờ tra được tenant").isNull();
        assertThat(count(ABANDONED_METRIC) - abandonedBefore)
                .as("counter walletflow.bank.webhook.abandoned +1 (alert BankWebhookAbandoned)").isEqualTo(1.0);

        db.makeWebhookDue(row.getProviderRef());
        assertThat(worker.process()).as("đã bỏ cuộc thì thôi thử mãi").isZero();
    }

    @Test
    @DisplayName("Câu trả lời tới đúng ở LƯỢT CHÓT: xử lý xong là RESOLVED, không bị đánh nhầm thành ABANDONED")
    void cau_tra_loi_toi_o_luot_chot_khong_bi_danh_nham_la_bo_cuoc() {
        ProviderTransaction row = createPending(25_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        accept(row.getProviderRef(), "SUCCEEDED");

        for (int attempt = 1; attempt <= 2; attempt++) {
            mock.failNextCalls(1);
            db.makeWebhookDue(row.getProviderRef());
            worker.process();
        }
        // Lượt thứ ba (lượt chót theo trần) provider trả lời được
        db.makeWebhookDue(row.getProviderRef());
        assertThat(worker.process()).isEqualTo(1);

        assertThat(db.webhookOfRef(row.getProviderRef()).get("outcome")).isEqualTo("RESOLVED");
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
    }

    // ------------------------------------------------------------------
    // Mã tham chiếu lạ và provider khác
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Chữ ký đúng nhưng mã tham chiếu KHÔNG thuộc hệ thống: đóng UNRESOLVED_REF, không hỏi provider, đếm counter")
    void ma_tham_chieu_la_bi_dong_lai_khong_hoi_provider() {
        accept("KHONG-TON-TAI", "SUCCEEDED");
        double unresolvedBefore = count(UNRESOLVED_METRIC);
        int providerCallsBefore = providerCalls();

        assertThat(worker.process()).isEqualTo(1);

        Map<String, Object> webhook = db.webhookOfRef("KHONG-TON-TAI");
        assertThat(webhook.get("processed_at")).as("tra mãi cũng không ra: đóng lại, đừng thử mãi").isNotNull();
        assertThat(webhook.get("outcome")).isEqualTo("UNRESOLVED_REF");
        assertThat(webhook.get("tenant_id")).isNull();
        assertThat(count(UNRESOLVED_METRIC) - unresolvedBefore)
                .as("ký được bằng khoá của ta mà mã lạ là chuyện phải biết (alert BankWebhookUnresolvedRef)")
                .isEqualTo(1.0);
        assertThat(providerCalls()).as("không có gì để hỏi").isEqualTo(providerCallsBefore);
    }

    @Test
    @DisplayName("⭐ Webhook của provider KHÁC provider đang chạy: worker không nhặt, hàng nguyên vẹn")
    void webhook_cua_provider_khac_khong_bi_nhat() {
        UUID otherId = db.insertWebhookOfProvider("other", "OTHER-REF-1");
        Map<String, Object> before = db.webhookById(otherId);
        // Đối chứng: một webhook của mock cũng tới hạn trong CÙNG lượt — worker không phải "không nhặt gì cả"
        ProviderTransaction own = createPending(15_000);
        mock.settle(own.getProviderRef(), EProviderState.SUCCEEDED);
        accept(own.getProviderRef(), "SUCCEEDED");

        assertThat(worker.process()).as("chỉ webhook của mock được nhặt").isEqualTo(1);

        assertThat(db.webhookById(otherId))
                .as("hàng của provider 'other' nguyên vẹn đến từng cột, kể cả next_attempt_at")
                .isEqualTo(before);
        assertThat(db.status(tenant, own.getTransactionId())).isEqualTo("SUCCEEDED");
    }

    // ------------------------------------------------------------------
    // Tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Webhook của hai tenant trong cùng một lô: mỗi hàng chốt trong ĐÚNG tenant của nó, không rò tenant")
    void hai_tenant_trong_mot_lo_khong_ro_tenant() {
        UUID tenantX = UUID.randomUUID();
        UUID tenantY = UUID.randomUUID();
        ProviderTransaction x = createPending(tenantX, 10_000);
        ProviderTransaction y = createPending(tenantY, 20_000);
        mock.settle(x.getProviderRef(), EProviderState.SUCCEEDED);
        mock.settle(y.getProviderRef(), EProviderState.FAILED);
        accept(x.getProviderRef(), "SUCCEEDED");
        accept(y.getProviderRef(), "FAILED");

        assertThat(worker.process()).isEqualTo(2);

        assertThat(db.status(tenantX, x.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(db.status(tenantY, y.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.webhookOfRef(x.getProviderRef()).get("tenant_id")).isEqualTo(tenantX);
        assertThat(db.webhookOfRef(y.getProviderRef()).get("tenant_id")).isEqualTo(tenantY);
        assertThat(db.outbox(x.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED))
                .extracting(event -> event.get("tenant_id")).containsExactly(tenantX);
        assertThat(db.outbox(y.getTransactionId(), WalletTopics.EVT_BANK_FAILED))
                .extracting(event -> event.get("tenant_id")).containsExactly(tenantY);
        assertThat(db.outboxEventIds(x.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        assertThat(db.outboxEventIds(y.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(TenantContext.current()).isEmpty();
    }

    @Test
    @DisplayName("Một webhook lỗi không chặn các webhook khác trong cùng lô")
    void mot_webhook_loi_khong_chan_cac_webhook_khac() {
        ProviderTransaction a = createPending(11_000);
        ProviderTransaction b = createPending(22_000);
        mock.settle(a.getProviderRef(), EProviderState.SUCCEEDED);
        mock.settle(b.getProviderRef(), EProviderState.SUCCEEDED);
        accept(a.getProviderRef(), "SUCCEEDED");   // nhận trước -> tới hạn trước -> gặp lỗi
        accept(b.getProviderRef(), "SUCCEEDED");
        mock.failNextCalls(1);

        assertThat(worker.process()).isEqualTo(2);

        assertThat(db.status(tenant, a.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.webhookOfRef(a.getProviderRef()).get("processed_at")).isNull();
        assertThat(db.status(tenant, b.getTransactionId())).as("hàng sau hàng lỗi vẫn được xử lý").isEqualTo("SUCCEEDED");
        assertThat(db.webhookOfRef(b.getProviderRef()).get("outcome")).isEqualTo("RESOLVED");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private ProviderTransaction createPending(long amountMinor) {
        return createPending(tenant, amountMinor);
    }

    /** Tạo một lần thu ở trạng thái PENDING (phiên đã tạo ở mock, chưa ai trả tiền). */
    private ProviderTransaction createPending(UUID tenantId, long amountMinor) {
        BankChargeCommand command = BankFixtures.freshCharge(amountMinor);
        ProviderTransaction row = TenantContext.callWith(tenantId, () -> chargeService.charge(command));
        assertThat(row.getProviderRef()).as("tiền điều kiện: lần thu phải PENDING").isNotNull();
        return row;
    }

    /** Nhận một webhook ký đúng, như thể provider vừa gọi tới — phần HTTP đã có {@code WebhookIT}. */
    private void accept(String providerRef, String state) {
        SortedMap<String, String> params = signed(Map.of("ref", providerRef, "state", state));
        assertThat(ingestService.accept("mock", params, rawPayload(params)))
                .as("tiền điều kiện: webhook phải được ghi lại")
                .isEqualTo(WebhookIngestService.Outcome.ACCEPTED);
    }

    /** Ký ĐỘC LẬP với code production để một lỗi trong thuật toán ký không tự xác nhận chính nó. */
    private static SortedMap<String, String> signed(Map<String, String> params) {
        SortedMap<String, String> sorted = new TreeMap<>(params);
        sorted.put("signature", hmac(rawPayload(sorted)));
        return sorted;
    }

    private static String rawPayload(SortedMap<String, String> params) {
        return params.entrySet().stream()
                .filter(entry -> !entry.getKey().equals("signature"))
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.US_ASCII)
                        + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.US_ASCII))
                .collect(Collectors.joining("&"));
    }

    private static String hmac(String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA512"));
            return HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private int providerCalls() {
        return resilientProvider.circuitBreaker().getMetrics().getNumberOfBufferedCalls();
    }

    private double count(String metric) {
        return meterRegistry.get(metric).counter().count();
    }
}
