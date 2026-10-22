package com.walletflow.bank;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.mock.MockProvider;
import com.walletflow.bank.services.ChargeService;
import com.walletflow.bank.support.BankDb;
import com.walletflow.bank.support.BankFixtures;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.tenant.TenantContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
import java.util.HexFormat;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Trang người dùng quay về sau khi trả tiền (roadmap 2.3b).
 *
 * <p><b>Tính chất phải giữ: tham số redirect KHÔNG BAO GIỜ là kết quả.</b> Nó đi qua trình duyệt của người
 * dùng — tức qua tay người có động cơ sửa nó nhất — và ngay cả một redirect thật cũng chỉ nói "người dùng đã
 * rời trang thanh toán", không nói "tiền đã về". Endpoint chỉ được ĐỌC trạng thái adapter đang ghi nhận.
 *
 * <p>Chữ ký trong test tự tính ĐỘC LẬP với code production (không gọi {@code MockProvider}).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "walletflow.bank.provider=mock",
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        "walletflow.bank.mock.webhook-secret=" + PaymentReturnIT.SECRET,
        // Không luồng nền nào được chốt kết quả sau lưng test: trạng thái trả về phải là thứ ĐANG có trong DB
        "walletflow.bank.status-check.enabled=false",
        "walletflow.bank.webhook.enabled=false",
        // Để next_check_at của hàng vừa tạo nằm ở TƯƠNG LAI — có thế mới thấy được việc kéo lịch về
        "walletflow.bank.status-check.recheck-after=PT30S"
})
@Testcontainers
class PaymentReturnIT {

    /** Khoá ký của mock trong test. Fixture bịa, không mở được gì. */
    static final String SECRET = "wf-test-return-secret";

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

    @LocalServerPort
    int port;
    @Autowired
    TestRestTemplate http;
    @Autowired
    ChargeService chargeService;
    @Autowired
    MockProvider mock;
    @Autowired
    JdbcTemplate jdbcTemplate;

    BankDb db;
    UUID tenant;

    @BeforeEach
    void reset() {
        db = new BankDb(jdbcTemplate);
        db.clean();
        mock.reset();
        tenant = UUID.randomUUID();
    }

    @Test
    @DisplayName("⭐ Redirect ký đúng nói ĐÃ THU nhưng lần thu chưa chốt: trả PENDING — trạng thái lấy từ DB, "
            + "không bao giờ từ tham số redirect")
    void tra_trang_thai_trong_db_chu_khong_phai_ket_qua_trong_redirect() {
        ProviderTransaction row = createPending(50_000);
        // Phiên ở mock KHÔNG được settle, nhưng redirect vẫn khai thành công
        ResponseEntity<String> response = get(signed(Map.of("ref", row.getProviderRef(), "state", "SUCCEEDED")));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .as("đọc kết quả từ redirect là cho người dùng tự khai mình đã trả tiền")
                .contains("\"status\":\"PENDING\"")
                .contains("\"settled\":false")
                .contains(row.getTransactionId().toString());
        assertThat(db.status(tenant, row.getTransactionId())).isEqualTo("PENDING");
        assertThat(db.outboxEventIds(row.getTransactionId(), WalletTopics.EVT_BANK_COMPLETED)).isEmpty();
    }

    @Test
    @DisplayName("⭐ Mở trang quay về KHÔNG ghi gì vào webhook_events — đây là một lần người dùng bấm F5, "
            + "không phải thông báo của provider")
    void mo_trang_quay_ve_khong_ghi_webhook_events() {
        ProviderTransaction row = createPending(10_000);
        SortedMap<String, String> params = signed(Map.of("ref", row.getProviderRef(), "state", "SUCCEEDED"));

        get(params);
        get(params);
        get(params);

        assertThat(db.webhookCount())
                .as("ghi mỗi lần F5 thành một 'sự kiện provider' là bơm rác vào bảng bằng chứng đối soát")
                .isZero();
    }

    @Test
    @DisplayName("⭐ Chữ ký sai: 401 và câu trả lời GIỐNG HỆT nhau cho mã có thật và mã bịa — không dò được lần thu nào tồn tại")
    void chu_ky_sai_tra_loi_nhu_nhau_cho_ma_that_va_ma_bia() {
        ProviderTransaction row = createPending(10_000);
        SortedMap<String, String> real = new TreeMap<>(Map.of("ref", row.getProviderRef(), "state", "SUCCEEDED"));
        real.put("signature", "00".repeat(64));
        SortedMap<String, String> fake = new TreeMap<>(Map.of("ref", "KHONG-TON-TAI", "state", "SUCCEEDED"));
        fake.put("signature", "00".repeat(64));

        ResponseEntity<String> onReal = get(real);
        ResponseEntity<String> onFake = get(fake);

        assertThat(onReal.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(onFake.getStatusCode()).isEqualTo(onReal.getStatusCode());
        assertThat(onFake.getBody()).isEqualTo(onReal.getBody());
        assertThat(onReal.getBody()).as("lỗi của endpoint này là RFC 7807, client là frontend của ta")
                .contains("INVALID_SIGNATURE");
    }

    @Test
    @DisplayName("Thiếu hẳn chữ ký: 401")
    void thieu_chu_ky_tra_401() {
        ProviderTransaction row = createPending(10_000);

        ResponseEntity<String> response = get(new TreeMap<>(Map.of("ref", row.getProviderRef())));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("⭐ Redirect gửi cho provider KHÁC provider đang chạy: 401 — không bao giờ kiểm chữ ký VNPay bằng khoá của mock")
    void redirect_cua_provider_khac_bi_tu_choi() {
        ProviderTransaction row = createPending(10_000);
        SortedMap<String, String> params = signed(Map.of("ref", row.getProviderRef(), "state", "SUCCEEDED"));

        ResponseEntity<String> response = http.getForEntity(url("vnpay", params), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("Chữ ký đúng nhưng mã tham chiếu không có trong hệ thống: 404")
    void ma_tham_chieu_la_tra_404() {
        ResponseEntity<String> response = get(signed(Map.of("ref", "KHONG-TON-TAI", "state", "SUCCEEDED")));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("Tham số lặp tên: 401 — ký trên giá trị này rồi cho đọc giá trị kia là một đường lách chữ ký")
    void tham_so_lap_ten_bi_tu_choi() {
        ProviderTransaction row = createPending(10_000);
        SortedMap<String, String> params = signed(Map.of("ref", row.getProviderRef(), "state", "SUCCEEDED"));

        ResponseEntity<String> response = http.getForEntity(url("mock", params) + "&state=FAILED", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    @DisplayName("⭐ Người dùng quay về: lần hỏi provider kế tiếp được kéo về NGAY, không bắt chờ hết nhịp backoff")
    void quay_ve_keo_lich_hoi_lai_ve_ngay() {
        ProviderTransaction row = createPending(10_000);
        assertThat(db.nextCheckIsInFuture(row.getTransactionId()))
                .as("tiền điều kiện: hàng vừa tạo còn 30s nữa mới tới hạn hỏi lại").isTrue();

        get(signed(Map.of("ref", row.getProviderRef(), "state", "SUCCEEDED")));

        assertThat(db.nextCheckIsInFuture(row.getTransactionId()))
                .as("tín hiệu về THỜI ĐIỂM thì tin được, dù kết quả thì không").isFalse();
        assertThat(db.status(tenant, row.getTransactionId()))
                .as("kéo lịch KHÔNG phải là chốt kết quả").isEqualTo("PENDING");
        assertThat(db.checkAttempts(row.getTransactionId()))
                .as("không đụng bộ đếm backoff: F5 nhiều lần không được làm nhịp hỏi giãn ra")
                .isZero();
    }

    @Test
    @DisplayName("Lần thu đã chốt thành công: settled = true, trạng thái SUCCEEDED, không có mã lỗi")
    void lan_thu_da_chot_tra_settled_true() {
        ProviderTransaction row = createPending(10_000);
        mock.settle(row.getProviderRef(), EProviderState.SUCCEEDED);
        jdbcTemplate.update("UPDATE provider_transactions SET status = 'SUCCEEDED', next_check_at = NULL "
                + "WHERE id = ?", row.getId());

        ResponseEntity<String> response = get(signed(Map.of("ref", row.getProviderRef(), "state", "SUCCEEDED")));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .contains("\"status\":\"SUCCEEDED\"")
                .contains("\"settled\":true")
                .contains("\"failureCode\":null");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private ProviderTransaction createPending(long amountMinor) {
        BankChargeCommand command = BankFixtures.freshCharge(amountMinor);
        ProviderTransaction row = TenantContext.callWith(tenant, () -> chargeService.charge(command));
        assertThat(row.getProviderRef()).as("tiền điều kiện: lần thu phải PENDING").isNotNull();
        return row;
    }

    private ResponseEntity<String> get(SortedMap<String, String> params) {
        return http.getForEntity(url("mock", params), String.class);
    }

    private String url(String provider, SortedMap<String, String> params) {
        String query = params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        return "http://localhost:" + port + "/payments/return/" + provider + "?" + query;
    }

    /** Ký ĐỘC LẬP với code production: HMAC-SHA512 trên "k=v&k=v" đã sắp theo tên, như quy tắc của mock. */
    private static SortedMap<String, String> signed(Map<String, String> params) {
        SortedMap<String, String> sorted = new TreeMap<>(params);
        String data = sorted.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.US_ASCII)
                        + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.US_ASCII))
                .collect(Collectors.joining("&"));
        sorted.put("signature", hmac(data));
        return sorted;
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
}
