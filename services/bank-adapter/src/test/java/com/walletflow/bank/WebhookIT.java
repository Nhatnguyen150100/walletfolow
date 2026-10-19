package com.walletflow.bank;

import com.walletflow.bank.support.BankDb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Nhận webhook/IPN (roadmap 2.3, ADR-018) — endpoint CÔNG KHAI đầu tiên của bank-adapter.
 *
 * <p>Thứ phải chứng minh ở đây không phải "đường hạnh phúc chạy được", mà là endpoint này KHÔNG trở thành
 * một đường ghi dữ liệu cho người lạ: chữ ký sai thì không một hàng nào được tạo, webhook trùng không được
 * xử lý hai lần, và không ai suy ra được lần thu nào tồn tại qua mã trả về.
 *
 * <p>Chữ ký trong test tự tính ĐỘC LẬP với code production (không gọi {@code MockProvider}), để một lỗi
 * trong thuật toán ký không tự xác nhận chính nó.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "walletflow.bank.provider=mock",
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        // Job đối chiếu không được chạy xen vào: test này chỉ nói về việc NHẬN webhook
        "walletflow.bank.status-check.enabled=false",
        "walletflow.bank.mock.webhook-secret=" + WebhookIT.SECRET
})
@Testcontainers
class WebhookIT {

    /** Khoá ký của mock trong test. Fixture bịa, không mở được gì. */
    static final String SECRET = "wf-test-webhook-secret";

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
    JdbcTemplate jdbcTemplate;

    BankDb db;

    @BeforeEach
    void reset() {
        db = new BankDb(jdbcTemplate);
        db.clean();
    }

    @Test
    @DisplayName("⭐ Webhook ký đúng: 200, ghi đúng một hàng webhook_events mang provider_ref, chưa xử lý và chưa có tenant")
    void webhook_ky_dung_duoc_ghi_lai() {
        ResponseEntity<String> response = send(signed(Map.of("ref", "MOCK-REF-1", "state", "SUCCEEDED")));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("ACCEPTED");

        List<Map<String, Object>> rows = db.webhooks();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("provider")).isEqualTo("mock");
        assertThat(rows.get(0).get("provider_ref")).isEqualTo("MOCK-REF-1");
        assertThat(rows.get(0).get("processed_at")).as("worker 2.3b chưa chạy").isNull();
        assertThat(rows.get(0).get("tenant_id")).as("tenant chỉ suy ra được khi tra provider_ref (2.3b)").isNull();
        assertThat((String) rows.get(0).get("provider_event_id")).as("SHA-256 hex").hasSize(64);
    }

    @Test
    @DisplayName("⭐ Chữ ký SAI: 401 và KHÔNG ghi gì — endpoint công khai không được trở thành đường bơm dữ liệu vào DB")
    void chu_ky_sai_thi_khong_ghi_gi() {
        SortedMap<String, String> params = new TreeMap<>(Map.of("ref", "MOCK-REF-1", "state", "SUCCEEDED"));
        params.put("signature", "00".repeat(64));   // 128 ký tự hex, rõ ràng không phải chữ ký thật

        ResponseEntity<String> response = send(params);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(db.webhookCount()).isZero();
    }

    @Test
    @DisplayName("⭐ Nội dung bị sửa sau khi ký (chữ ký thật của payload KHÁC): 401, không ghi gì")
    void noi_dung_bi_sua_sau_khi_ky_bi_tu_choi() {
        SortedMap<String, String> params = signed(Map.of("ref", "MOCK-REF-1", "state", "FAILED"));
        params.put("state", "SUCCEEDED");   // đổi nghĩa, giữ nguyên chữ ký

        ResponseEntity<String> response = send(params);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(db.webhookCount()).isZero();
    }

    @ParameterizedTest(name = "thiếu/rỗng chữ ký: [{0}]")
    @ValueSource(strings = {"", "   "})
    @DisplayName("Chữ ký rỗng hoặc thiếu hẳn: 401, không ghi gì")
    void thieu_chu_ky_bi_tu_choi(String signature) {
        SortedMap<String, String> params = new TreeMap<>(Map.of("ref", "MOCK-REF-1"));
        if (!signature.isEmpty()) {
            params.put("signature", signature);
        }

        assertThat(send(params).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(db.webhookCount()).isZero();
    }

    @Test
    @DisplayName("⭐ Webhook TRÙNG (provider gửi lại y hệt): vẫn 200 cho provider thôi gửi lại, nhưng chỉ đúng một hàng")
    void webhook_trung_chi_ghi_mot_lan() {
        SortedMap<String, String> params = signed(Map.of("ref", "MOCK-REF-1", "state", "SUCCEEDED"));

        assertThat(send(params).getStatusCode()).isEqualTo(HttpStatus.OK);
        ResponseEntity<String> again = send(params);

        assertThat(again.getStatusCode()).as("báo lỗi chỉ khiến provider gửi lại thứ ta đã có").isEqualTo(HttpStatus.OK);
        assertThat(again.getBody()).contains("DUPLICATE");
        assertThat(db.webhookCountOfRef("MOCK-REF-1")).isEqualTo(1);
    }

    @Test
    @DisplayName("⭐ Cùng giao dịch nhưng TRẠNG THÁI MỚI: là webhook khác, phải được ghi — webhook đến sai thứ tự là chuyện thường")
    void trang_thai_moi_cung_ref_van_duoc_ghi() {
        send(signed(Map.of("ref", "MOCK-REF-1", "state", "PENDING")));
        send(signed(Map.of("ref", "MOCK-REF-1", "state", "SUCCEEDED")));

        assertThat(db.webhookCountOfRef("MOCK-REF-1"))
                .as("gộp hai trạng thái thành một là bỏ mất kết quả cuối").isEqualTo(2);
    }

    @Test
    @DisplayName("⭐ Webhook gửi cho provider KHÁC provider đang chạy: 401 — không bao giờ kiểm chữ ký VNPay bằng khoá của mock")
    void webhook_cua_provider_khac_bi_tu_choi() {
        SortedMap<String, String> params = signed(Map.of("ref", "MOCK-REF-1"));

        ResponseEntity<String> response = http.getForEntity(url("/webhooks/vnpay", params), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(db.webhookCount()).isZero();
    }

    @Test
    @DisplayName("⭐ Nhồi dấu phân cách vào GIÁ TRỊ để dựng lại cùng chuỗi ký: 401 — hai payload khác nhau không bao giờ "
            + "được ra cùng chuỗi ký")
    void nhoi_dau_phan_cach_vao_gia_tri_khong_dung_lai_duoc_chu_ky() {
        // Chữ ký thật của {ref=A, state=PENDING}
        SortedMap<String, String> legit = signed(Map.of("ref", "A", "state", "PENDING"));
        String stolenSignature = legit.get("signature");

        // Gửi lại dưới dạng MỘT tham số mang cả dấu & và =. Không encode thì chuỗi ký dựng ra y hệt chuỗi
        // trên, nên chữ ký cũ khớp — và provider_ref ghi vào DB lại là một giá trị khác hẳn.
        //
        // Gửi bằng HttpClient thô, KHÔNG qua TestRestTemplate: RestTemplate coi chuỗi URL là uri template và
        // encode lại '%' thành '%25', nên server sẽ nhận một giá trị khác và test xanh vì lý do sai.
        int status = rawGet("/webhooks/mock?ref=A%26state%3DPENDING&signature=" + stolenSignature);

        assertThat(status).isEqualTo(401);
        assertThat(db.webhookCount()).isZero();
    }

    @Test
    @DisplayName("Tham số lặp tên: 401 — ký trên giá trị này rồi cho đọc giá trị kia là một đường lách chữ ký")
    void tham_so_lap_ten_bi_tu_choi() {
        SortedMap<String, String> params = signed(Map.of("ref", "MOCK-REF-1", "state", "SUCCEEDED"));

        ResponseEntity<String> response = http.getForEntity(
                url("/webhooks/mock", params) + "&state=FAILED", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(db.webhookCount()).isZero();
    }

    @Test
    @DisplayName("⭐ Mã tham chiếu KHÔNG tồn tại nhưng chữ ký đúng: vẫn 200 và vẫn ghi — câu trả lời không được tiết lộ lần thu nào có thật")
    void ref_khong_ton_tai_van_tra_loi_nhu_nhau() {
        ResponseEntity<String> known = send(signed(Map.of("ref", "MOCK-REF-1", "state", "SUCCEEDED")));
        ResponseEntity<String> unknown = send(signed(Map.of("ref", "KHONG-TON-TAI", "state", "SUCCEEDED")));

        assertThat(unknown.getStatusCode()).isEqualTo(known.getStatusCode());
        assertThat(unknown.getBody()).isEqualTo(known.getBody());
        assertThat(db.webhookCount()).as("ghi lại để worker 2.3b và người đối soát còn thấy").isEqualTo(2);
    }

    @Test
    @DisplayName("⭐ 10 request SONG SONG cùng payload: đúng 1 hàng, đúng 1 ACCEPTED, 9 DUPLICATE, không 500 nào "
            + "— ON CONFLICT DO NOTHING phải giữ được, không phải SELECT-rồi-INSERT")
    void gui_song_song_cung_payload_chi_ghi_mot_hang() throws Exception {
        SortedMap<String, String> params = signed(Map.of("ref", "MOCK-REF-RACE", "state", "SUCCEEDED"));
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);

        List<Future<String>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            results.add(pool.submit(() -> {
                start.await();
                return send(params).getBody();
            }));
        }
        start.countDown();
        List<String> bodies = new ArrayList<>();
        for (Future<String> result : results) {
            bodies.add(result.get());
        }
        pool.shutdownNow();

        assertThat(bodies).hasSize(threads).allSatisfy(body ->
                assertThat(body).as("không request nào được phép 500").doesNotContain("500"));
        assertThat(bodies.stream().filter(b -> b.contains("ACCEPTED")).count()).isEqualTo(1);
        assertThat(bodies.stream().filter(b -> b.contains("DUPLICATE")).count()).isEqualTo(threads - 1L);
        assertThat(db.webhookCountOfRef("MOCK-REF-RACE")).isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Ký độc lập với code production
    // ------------------------------------------------------------------

    /** Thêm {@code signature} tính tay theo cùng quy tắc mock: HMAC-SHA512 trên "k=v&k=v" đã sắp theo tên. */
    private static SortedMap<String, String> signed(Map<String, String> params) {
        SortedMap<String, String> sorted = new TreeMap<>(params);
        String data = sorted.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.US_ASCII)
                        + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.US_ASCII))
                .collect(Collectors.joining("&"));
        sorted.put("signature", hmac(SECRET, data));
        return sorted;
    }

    private static String hmac(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA512"));
            return HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /**
     * GET bằng HttpClient thô — giữ NGUYÊN VĂN query string. {@code TestRestTemplate} coi chuỗi URL là uri
     * template và encode lại dấu {@code %}, nên không dùng được cho các test về canonicalization.
     */
    private int rawGet(String pathAndQuery) {
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + pathAndQuery)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return response.statusCode();
        } catch (IOException | InterruptedException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private ResponseEntity<String> send(SortedMap<String, String> params) {
        return http.getForEntity(url("/webhooks/mock", params), String.class);
    }

    private String url(String path, SortedMap<String, String> params) {
        String query = params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        return "http://localhost:" + port + path + "?" + query;
    }
}
