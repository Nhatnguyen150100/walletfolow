package com.walletflow.bank;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.provider.ResilientPaymentProvider;
import com.walletflow.bank.services.ChargeService;
import com.walletflow.bank.services.PendingChargeReconciler;
import com.walletflow.bank.services.WebhookIngestService;
import com.walletflow.bank.services.WebhookWorker;
import com.walletflow.bank.support.Await;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.FakeVnPayServer;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chaos test của DoD Phase 2: <b>provider chết 5 phút → không mất tiền, không saga treo</b>.
 *
 * <p>Toxiproxy đứng giữa adapter và {@code querydr} của VNPay giả, tạo các kiểu chết ở tầng TCP mà một HTTP
 * server giả không tự tạo được: ngắt hẳn, nhận kết nối rồi im, reset, cắt ngang câu trả lời. Chỉ chặn được
 * {@code querydr} — đó cũng là lời gọi mạng DUY NHẤT tới VNPay: tạo phiên chỉ ký URL tại chỗ, hoàn tiền chưa gọi
 * VNPay (2.4c), và IPN là VNPay gọi vào ta.
 *
 * <p>"Không mất tiền" ở tầng này nghĩa là ba thứ, đúng ba cách một lần mất kết nối biến thành mất tiền:
 * <ol>
 *   <li>"không biết" không bao giờ thành "thất bại" hay "hết hạn" — người đã trả tiền mà bị báo thất bại thì
 *       ví không bao giờ được cộng;</li>
 *   <li>sống lại thì người đã trả tiền được ĐÚNG MỘT {@code completed} đúng số tiền — không mất, không cộng hai lần
 *       dù job đối chiếu và worker webhook đều chạy lại (tuần tự) sau khi provider sống lại;</li>
 *   <li>không lần thu nào bị bỏ rơi: còn lịch hỏi lại, hoặc đã có câu trả lời cuối.</li>
 * </ol>
 * "Không treo" ở tầng này: mỗi lời gọi bị chặn trước {@code resilience.timeout}, và sau khi provider sống lại
 * mọi lần thu tới được trạng thái cuối mà không cần người. Phía saga (transaction-service) có điểm kết thúc
 * riêng: bank-adapter im lặng quá lâu thì hết lượt gửi lại → {@code MANUAL_REVIEW(BANK_NO_RESPONSE)}, xem
 * {@code StuckSagaIT}.
 *
 * <p>Thời gian được TUA chứ không chờ thật: mỗi "nhịp" ({@link #tick}) đẩy hạn hỏi lại của mọi hàng về quá khứ
 * rồi chạy job đối chiếu + worker webhook. {@link #OUTAGE_ROUNDS} nhịp nhiều hơn số lượt hỏi thật lọt vào
 * 5 phút ở nhịp mặc định (job đối chiếu 30s→1m→2m→4m: 4 lượt; webhook 10s→20s→40s→80s→160s: 5 lượt), tức là
 * khắc nghiệt hơn thực tế.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "outbox.relay.delay-ms=300",
        "walletflow.bank.provider=vnpay",
        "walletflow.bank.vnpay.tmn-code=" + ChaosProviderIT.TMN_CODE,
        "walletflow.bank.vnpay.hash-secret=" + ChaosProviderIT.HASH_SECRET,
        "walletflow.bank.vnpay.return-url=http://localhost/return",
        "walletflow.bank.vnpay.server-ip=10.0.0.1",
        // HTTP client phải timeout NGẮN hơn resilience.timeout (PaymentProviderConfig kiểm lúc khởi động)
        "walletflow.bank.vnpay.connect-timeout=1s",
        "walletflow.bank.vnpay.read-timeout=500ms",
        "walletflow.bank.resilience.timeout=2s",
        // Mạch mở 2s thay vì 30s: chỉ để test không phải ngồi chờ; cửa sổ, ngưỡng và số lời gọi tối thiểu giữ
        // nguyên như production — mạch phải tự mở được với đúng cấu hình thật
        "walletflow.bank.resilience.open-wait=2s",
        // Job đối chiếu và worker webhook được gọi tay, lịch tự chạy đẩy ra xa để không chen vào giữa test.
        // Nhịp giãn (recheck-after, retry-after) và trần lượt thử giữ mặc định production.
        "walletflow.bank.status-check.interval-ms=3600000",
        "walletflow.bank.webhook.interval-ms=3600000"
})
@Testcontainers
class ChaosProviderIT {

    static final String TMN_CODE = "WFCHAOS1";
    /** Giá trị giả, chỉ dùng trong test này — không phải bí mật của môi trường nào. */
    static final String HASH_SECRET = "it-only-fake-chaos-hash-secret-0123456789";
    /** Phải khớp walletflow.bank.session.expiry-grace mặc định (PT15M). */
    private static final long GRACE_SECONDS = 900;
    /** Số nhịp "provider đang chết" — xem javadoc của lớp: nhiều hơn số lượt hỏi thật lọt vào 5 phút. */
    private static final int OUTAGE_ROUNDS = 6;
    /** Cổng đầu tiên trong dải Toxiproxy mở sẵn (8666-8697). */
    private static final int PROXY_PORT = 8666;

    private static final String STATUS_FAILURES = "walletflow.bank.status_check.failures";
    private static final String WEBHOOK_FAILURES = "walletflow.bank.webhook.failures";
    private static final String WEBHOOK_ABANDONED = "walletflow.bank.webhook.abandoned";
    private static final String OVERDUE_GAUGE = "walletflow.bank.pending_overdue";

    static final FakeVnPayServer fake = new FakeVnPayServer(TMN_CODE, HASH_SECRET);

    static {
        // Container Toxiproxy gọi ngược về VNPay giả chạy trên máy test: cổng phải được mở TRƯỚC khi container
        // khởi động (Testcontainers dựng đường hầm lúc start)
        org.testcontainers.Testcontainers.exposeHostPorts(URI.create(fake.apiUrl()).getPort());
    }

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("railsdb");

    @SuppressWarnings("resource")
    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @SuppressWarnings("resource")
    @Container
    static ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0");

    private static Proxy proxy;

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        // Người dùng mở trang thanh toán thẳng ở VNPay giả; chỉ chiều adapter → querydr đi qua proxy
        registry.add("walletflow.bank.vnpay.pay-url", fake::payUrl);
        registry.add("walletflow.bank.vnpay.api-url", () -> {
            proxy();
            return "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT)
                    + FakeVnPayServer.QUERY_PATH;
        });
    }

    @AfterAll
    static void stopFake() {
        fake.close();
    }

    @Autowired
    PendingChargeReconciler reconciler;
    @Autowired
    WebhookWorker worker;
    @Autowired
    WebhookIngestService ingestService;
    @Autowired
    ChargeService chargeService;
    @Autowired
    ResilientPaymentProvider resilientProvider;
    @Autowired
    MeterRegistry meterRegistry;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    JdbcTemplate jdbcTemplate;

    /**
     * Mọi lần mạch đổi trạng thái, theo thứ tự. Kiểm chuyển trạng thái ĐÃ XẢY RA thay vì đọc trạng thái ở một thời
     * điểm: mạch tự sang HALF_OPEN sau {@code open-wait}, nên "đang OPEN" chỉ đúng nếu test đọc đủ nhanh.
     */
    private static final List<CircuitBreaker.StateTransition> transitions = new CopyOnWriteArrayList<>();
    /** Context Spring được cache giữa các test: mạch là một, chỉ đăng ký nghe một lần. */
    private static boolean listening;

    BankDb db;
    UUID tenant;

    @BeforeEach
    void reset() {
        db = new BankDb(jdbcTemplate);
        db.clean();
        fake.reset();
        heal();
        if (!listening) {
            resilientProvider.circuitBreaker().getEventPublisher()
                    .onStateTransition(event -> transitions.add(event.getStateTransition()));
            listening = true;
        }
        resilientProvider.circuitBreaker().reset();
        transitions.clear();
        tenant = UUID.randomUUID();
    }

    // ------------------------------------------------------------------
    // DoD: provider chết 5 phút
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ VNPay chết 5 phút khi đang có lần thu chờ: không lần thu nào bị kết luận (không completed, không failed), "
            + "mạch mở, webhook không bị bỏ; sống lại → mỗi người đã trả được ĐÚNG MỘT completed đúng số tiền, "
            + "người chưa trả vẫn PENDING và vẫn còn lịch hỏi")
    void provider_chet_5_phut_khong_mat_tien_khong_treo() {
        ProviderTransaction paidBefore = createPending(150_000);   // trả xong ngay trước khi VNPay chết
        ProviderTransaction paidDuring = createPending(275_000);   // trả TRONG lúc ta không với tới VNPay
        ProviderTransaction openedOnly = createPending(90_000);    // mở trang, chưa trả xong
        ProviderTransaction untouched = createPending(60_000);     // chưa ai mở URL
        List<ProviderTransaction> all = List.of(paidBefore, paidDuring, openedOnly, untouched);
        fake.pay(paidBefore.getPaymentUrl());
        fake.open(openedOnly.getPaymentUrl());

        disable();
        fake.pay(paidDuring.getPaymentUrl());
        // IPN đi chiều VNPay → ta nên vẫn tới; adapter phải hỏi lại querydr mà không hỏi được
        acceptIpn(paidDuring);
        int queriesBefore = fake.requests().size();
        double statusFailuresBefore = counter(STATUS_FAILURES);
        double webhookFailuresBefore = counter(WEBHOOK_FAILURES);
        double abandonedBefore = counter(WEBHOOK_ABANDONED);

        for (int round = 0; round < OUTAGE_ROUNDS; round++) {
            tick(all);
        }

        assertThat(fake.requests()).as("tiền điều kiện: VNPay thật sự không nhận được gì").hasSize(queriesBefore);
        for (ProviderTransaction row : all) {
            assertThat(db.checkAttempts(row.getTransactionId()))
                    .as("tiền điều kiện: lần thu thật sự được hỏi ở mọi nhịp — không phải 'PENDING vì chẳng ai hỏi'")
                    .isGreaterThanOrEqualTo(OUTAGE_ROUNDS);
            assertThat(db.status(tenant, row.getTransactionId()))
                    .as("không có câu trả lời thì không kết luận gì (ADR-017)").isEqualTo("PENDING");
            assertThat(db.nextCheckIsNull(row.getTransactionId()))
                    .as("vẫn còn lịch hỏi lại — không bị bỏ rơi").isFalse();
            assertNoOutcome(row);
        }
        assertThat(counter(STATUS_FAILURES) - statusFailuresBefore)
                .as("mọi lần hỏi đều là lỗi kỹ thuật được đếm, kể cả khi mạch chặn (CallNotPermitted)")
                .isEqualTo((double) OUTAGE_ROUNDS * all.size());
        assertThat(counter(WEBHOOK_FAILURES) - webhookFailuresBefore).isEqualTo((double) OUTAGE_ROUNDS);
        assertThat(transitions)
                .as("mạch tự mở với đúng cửa sổ/ngưỡng production: không dội provider đang chết")
                .contains(CircuitBreaker.StateTransition.CLOSED_TO_OPEN);
        assertThat(db.webhookOfRef(paidDuring.getProviderRef()).get("processed_at")).isNull();
        assertThat(counter(WEBHOOK_ABANDONED))
                .as("5 phút không được tiêu hết lượt thử của webhook — webhook là đường nhanh, phải còn sống")
                .isEqualTo(abandonedBefore);

        enable();
        // Chờ mạch hết open-wait mà KHÔNG gọi gì, như thời gian thật — tick dồn dập trong lúc mạch còn mở sẽ
        // tiêu lượt thử của webhook nhanh hơn thực tế và làm nó bị bỏ oan
        awaitBreakerLeavesOpen();
        Await.until(() -> {
            tick(all);
            return isSucceeded(paidBefore) && isSucceeded(paidDuring);
        }, "người đã trả tiền được chốt SUCCEEDED sau khi VNPay sống lại", Duration.ofSeconds(30));
        // Chạy lại cả hai đường sau khi đã chốt: lặp lại không được sinh completed thứ hai
        for (int round = 0; round < 3; round++) {
            tick(all);
        }

        for (ProviderTransaction paid : List.of(paidBefore, paidDuring)) {
            assertThat(completedAmounts(paid))
                    .as("ĐÚNG MỘT completed mang đúng số tiền: thiếu là mất tiền của người dùng, thừa là cộng hai lần")
                    .containsExactly(paid.getAmountMinor());
            assertThat(db.outboxEventIds(paid.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        }
        for (ProviderTransaction unpaid : List.of(openedOnly, untouched)) {
            assertThat(db.status(tenant, unpaid.getTransactionId()))
                    .as("phiên còn hạn: người dùng vẫn trả được, không được báo thất bại").isEqualTo("PENDING");
            assertThat(db.nextCheckIsNull(unpaid.getTransactionId())).isFalse();
            assertNoOutcome(unpaid);
        }
        assertThat(resilientProvider.circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(db.webhookOfRef(paidDuring.getProviderRef()).get("outcome"))
                .as("webhook đến lúc provider chết được XỬ LÝ khi provider sống lại — processed_at thôi chưa đủ: "
                        + "hàng bị bỏ (ABANDONED) cũng có processed_at")
                .isEqualTo("RESOLVED");
        assertThat(counter(WEBHOOK_ABANDONED)).isEqualTo(abandonedBefore);
    }

    @Test
    @DisplayName("⭐ VNPay chết vắt qua hạn phiên + grace: 'không biết' KHÔNG BAO GIỜ thành EXPIRED, gauge quá hạn bật; "
            + "sống lại → người đã trả vẫn được completed (thành công thắng dù quá hạn), người chưa trả mới FAILED(EXPIRED)")
    void provider_chet_qua_han_phien_khong_bien_khong_biet_thanh_het_han() {
        ProviderTransaction paid = createPending(320_000);
        ProviderTransaction neverPaid = createPending(45_000);
        List<ProviderTransaction> all = List.of(paid, neverPaid);

        disable();
        fake.pay(paid.getPaymentUrl());
        all.forEach(row -> db.makeOverdue(row.getTransactionId(), GRACE_SECONDS, 60));

        for (int round = 0; round < OUTAGE_ROUNDS; round++) {
            tick(all);
        }

        for (ProviderTransaction row : all) {
            assertThat(db.status(tenant, row.getTransactionId()))
                    .as("quá hạn + grace nhưng không hỏi được: chỉ câu trả lời của provider mới kết thúc được lần thu")
                    .isEqualTo("PENDING");
            assertNoOutcome(row);
        }
        assertThat(meterRegistry.get(OVERDUE_GAUGE).gauge().value())
                .as("mỗi hàng quá hạn mà vẫn PENDING là một ca alert PendingChargeOverdue — người trực thấy được")
                .isEqualTo(2.0);

        enable();
        Await.until(() -> {
            tick(all);
            return !"PENDING".equals(db.status(tenant, paid.getTransactionId()))
                    && !"PENDING".equals(db.status(tenant, neverPaid.getTransactionId()));
        }, "cả hai lần thu có câu trả lời cuối sau khi VNPay sống lại", Duration.ofSeconds(30));

        assertThat(db.status(tenant, paid.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(completedAmounts(paid)).containsExactly(320_000L);
        assertThat(db.outboxEventIds(paid.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();

        assertThat(db.status(tenant, neverPaid.getTransactionId())).isEqualTo("FAILED");
        assertThat(db.failureCode(tenant, neverPaid.getTransactionId())).isEqualTo("EXPIRED");
        assertThat(db.outboxEventIds(neverPaid.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).hasSize(1);
        assertThat(db.outboxEventIds(neverPaid.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(meterRegistry.get(OVERDUE_GAUGE).gauge().value()).isZero();
    }

    // ------------------------------------------------------------------
    // Từng kiểu chết ở tầng TCP
    // ------------------------------------------------------------------
    /** Các kiểu provider chết mà một HTTP server giả không tự tạo được. */
    enum Fault {
        /** Không có gì nghe ở đầu kia. */
        NGAT_HAN,
        /** Nhận kết nối rồi im lặng mãi — kiểu chết nguy hiểm nhất vì giữ thread. */
        TREO,
        /** Kết nối bị reset giữa chừng. */
        RESET,
        /** Câu trả lời bị cắt ngang: nửa đầu tới, nửa sau mất. */
        CAT_NGANG,
        /** Trả lời chậm hơn read-timeout. */
        CHAM
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Fault.class)
    @DisplayName("Mỗi kiểu chết ở tầng TCP là 'không biết': người đã trả vẫn PENDING + đếm lỗi, không event; lời gọi bị chặn "
            + "trước resilience.timeout nên thread đối soát không bị giữ; hết lỗi thì chốt đúng một completed")
    void moi_kieu_chet_deu_la_khong_biet(Fault fault) {
        ProviderTransaction row = createPending(110_000);
        fake.pay(row.getPaymentUrl());
        db.makeDue(row.getTransactionId(), 1);   // lần hỏi đầu chỉ tới hạn sau recheck-after (30s mặc định)
        inject(fault);
        double failuresBefore = counter(STATUS_FAILURES);

        long startedAt = System.nanoTime();
        assertThat(reconciler.reconcile()).isEqualTo(1);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(counter(STATUS_FAILURES) - failuresBefore).isEqualTo(1.0);
        assertNoOutcome(row);
        assertThat(elapsed)
                .as("timeout của HTTP client (connect 1s, read 500ms) phải nổ trước resilience.timeout 2s")
                .isLessThan(Duration.ofMillis(1_800));
        if (fault == Fault.TREO || fault == Fault.CHAM) {
            assertThat(elapsed)
                    .as("tiền điều kiện: lời gọi thật sự bị giữ tới read-timeout — độc tố đã có tác dụng, không phải "
                            + "hỏng nhanh vì lý do khác")
                    .isGreaterThanOrEqualTo(Duration.ofMillis(450));
        }

        heal();
        db.makeDue(row.getTransactionId(), 1);
        assertThat(reconciler.reconcile()).isEqualTo(1);

        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("SUCCEEDED");
        assertThat(completedAmounts(row)).containsExactly(110_000L);
    }

    @Test
    @DisplayName("VNPay chập chờn (chết → lượt thăm dò của mạch nửa mở gặp lúc vẫn chết → sống hẳn): mạch đi đủ "
            + "CLOSED→OPEN→HALF_OPEN→OPEN→HALF_OPEN→CLOSED; người trả tiền giữa lúc chập chờn được ĐÚNG MỘT completed, "
            + "không failed nào")
    void provider_chap_chon_khong_chot_trung() {
        // Bốn lần thu đã mở trang nhưng CHƯA trả: chúng còn PENDING nên được hỏi ở MỌI nhịp — đủ lời gọi để mạch
        // tự mở với minimum-calls = 10 của production
        List<ProviderTransaction> rows = List.of(createPending(70_000), createPending(80_000),
                createPending(95_000), createPending(55_000));
        List<String> refs = rows.stream().map(row -> fake.open(row.getPaymentUrl()).get("vnp_TxnRef")).toList();

        disable();
        for (int round = 0; round < 3; round++) {
            tick(rows);   // 12 lời gọi lỗi
        }
        assertThat(transitions).as("10 lời gọi lỗi liên tiếp → mạch mở")
                .containsExactly(CircuitBreaker.StateTransition.CLOSED_TO_OPEN);

        // Hết open-wait, mạch nửa mở thăm dò đúng lúc VNPay vẫn chết → mở lại, không cho lời gọi nào lọt qua tiếp
        awaitBreakerLeavesOpen();
        tick(rows);
        assertThat(transitions).containsExactly(CircuitBreaker.StateTransition.CLOSED_TO_OPEN,
                CircuitBreaker.StateTransition.OPEN_TO_HALF_OPEN, CircuitBreaker.StateTransition.HALF_OPEN_TO_OPEN);
        rows.forEach(row -> assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING"));

        // Người dùng trả xong giữa lúc chập chờn; rồi VNPay sống hẳn
        refs.forEach(ref -> fake.setStatus(ref, "00"));
        enable();
        awaitBreakerLeavesOpen();
        Await.until(() -> {
            tick(rows);
            return rows.stream().allMatch(this::isSucceeded);
        }, "mọi người đã trả được chốt SUCCEEDED khi VNPay ổn định", Duration.ofSeconds(30));
        tick(rows);

        assertThat(transitions).containsExactly(CircuitBreaker.StateTransition.CLOSED_TO_OPEN,
                CircuitBreaker.StateTransition.OPEN_TO_HALF_OPEN, CircuitBreaker.StateTransition.HALF_OPEN_TO_OPEN,
                CircuitBreaker.StateTransition.OPEN_TO_HALF_OPEN, CircuitBreaker.StateTransition.HALF_OPEN_TO_CLOSED);
        for (ProviderTransaction row : rows) {
            assertThat(completedAmounts(row)).containsExactly(row.getAmountMinor());
            assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
        }
        assertThat(resilientProvider.circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * Một nhịp của hệ thống với thời gian được tua: mọi lần thu còn chờ và mọi webhook chưa xử lý đều "tới hạn",
     * rồi job đối chiếu và worker webhook mỗi bên chạy một lượt.
     */
    private void tick(List<ProviderTransaction> rows) {
        for (ProviderTransaction row : rows) {
            db.makeDue(row.getTransactionId(), 1);
            db.makeWebhookDue(row.getProviderRef());
        }
        reconciler.reconcile();
        worker.process();
    }

    /**
     * Chờ mạch hết {@code open-wait} và tự sang HALF_OPEN mà KHÔNG gọi gì — thời gian trôi như thật, không tiêu lượt
     * thử nào của webhook hay job đối chiếu.
     */
    private void awaitBreakerLeavesOpen() {
        Await.until(() -> resilientProvider.circuitBreaker().getState() != CircuitBreaker.State.OPEN,
                "mạch rời OPEN sau open-wait", Duration.ofSeconds(10));
    }

    /** Tạo một lần thu PENDING qua service (URL thanh toán đã ký, chưa ai mở). */
    private ProviderTransaction createPending(long amountMinor) {
        BankChargeCommand command = new BankChargeCommand(UUID.randomUUID(), UUID.randomUUID(), amountMinor,
                ECurrency.VND, "203.0.113.7");
        ProviderTransaction row = TenantContext.callWith(tenant, () -> chargeService.charge(command));
        assertThat(row.getProviderRef()).as("tiền điều kiện: lần thu phải PENDING").isNotNull();
        return row;
    }

    /** VNPay gọi IPN vào ta cho lần thu này — ghi nhận qua đúng service của endpoint webhook. */
    private void acceptIpn(ProviderTransaction row) {
        Map<String, String> params = fake.ipn(row.getProviderRef());
        String rawQuery = params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.US_ASCII))
                .collect(Collectors.joining("&"));
        assertThat(ingestService.accept("vnpay", params, rawQuery))
                .as("tiền điều kiện: IPN ký đúng được nhận").isEqualTo(WebhookIngestService.Outcome.ACCEPTED);
    }

    private boolean isSucceeded(ProviderTransaction row) {
        return "SUCCEEDED".equals(db.status(tenant, row.getTransactionId()));
    }

    private void assertNoOutcome(ProviderTransaction row) {
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_FAILED)).isEmpty();
    }

    /** Số tiền mang trong từng event completed của lần thu — đúng con số ledger sẽ cộng vào ví. */
    private List<Long> completedAmounts(ProviderTransaction row) {
        return db.outboxPayloads(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED).stream()
                .map(json -> {
                    try {
                        return objectMapper.readTree(json).path("payload").path("amountMinor").asLong();
                    } catch (IOException unreadable) {
                        throw new UncheckedIOException(unreadable);
                    }
                })
                .toList();
    }

    private double counter(String name) {
        return meterRegistry.get(name).counter().count();
    }

    private static void inject(Fault fault) {
        try {
            switch (fault) {
                case NGAT_HAN -> proxy().disable();
                // timeout = 0: giữ kết nối, nuốt dữ liệu, không bao giờ đóng
                case TREO -> proxy().toxics().timeout("treo", ToxicDirection.DOWNSTREAM, 0);
                case RESET -> proxy().toxics().resetPeer("reset", ToxicDirection.DOWNSTREAM, 0);
                // Cắt sau 200 byte: rơi vào phần header hay thân JSON tuỳ độ dài header — ở đâu thì câu trả lời
                // cũng không trọn (thân có chữ ký dài hơn nhiều)
                case CAT_NGANG -> proxy().toxics().limitData("cat-ngang", ToxicDirection.DOWNSTREAM, 200);
                case CHAM -> proxy().toxics().latency("cham", ToxicDirection.DOWNSTREAM, 1_500);
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private static void disable() {
        try {
            proxy().disable();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private static void enable() {
        try {
            proxy().enable();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    /** Proxy về trạng thái lành: bật, không còn độc tố nào. */
    private static void heal() {
        try {
            proxy().enable();
            for (Toxic toxic : proxy().toxics().getAll()) {
                toxic.remove();
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    /** Proxy duy nhất của lớp, tạo lười khi container đã chạy: cổng 8666 của Toxiproxy → VNPay giả trên máy test. */
    private static synchronized Proxy proxy() {
        if (proxy == null) {
            try {
                ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
                proxy = client.createProxy("vnpay-querydr", "0.0.0.0:" + PROXY_PORT,
                        "host.testcontainers.internal:" + URI.create(fake.apiUrl()).getPort());
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }
        return proxy;
    }
}
