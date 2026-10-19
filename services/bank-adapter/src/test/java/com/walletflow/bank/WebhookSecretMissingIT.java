package com.walletflow.bank;

import com.walletflow.bank.support.BankDb;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KHÔNG cấu hình {@code walletflow.bank.mock.webhook-secret} ⇒ webhook của mock phải TẮT HẲN (ADR-018 §7).
 *
 * <p>Đây là một quyết định bảo mật tường minh của CLAUDE.md §6 ("thiếu bí mật thì tính năng tắt, không bao
 * giờ là một khoá mặc định ai cũng đoán được"), và nó chỉ nằm ở đúng một dòng trong {@code MockProvider}.
 * Không có test thì không có gì ngăn ai đó "sửa cho tiện" thành một khoá mặc định.
 *
 * <p>Phải là một IT riêng vì khoá được nạp lúc dựng bean, không đổi được giữa các test trong cùng context.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "walletflow.bank.provider=mock",
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        "walletflow.bank.status-check.enabled=false"
        // CỐ Ý không đặt walletflow.bank.mock.webhook-secret
})
@Testcontainers
class WebhookSecretMissingIT {

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
    @DisplayName("⭐ Thiếu webhook-secret: service VẪN khởi động bình thường nhưng mọi webhook bị 401 và không ghi gì")
    void thieu_khoa_thi_webhook_tat_han() {
        // Ký bằng một khoá bất kỳ — không có khoá nào đúng cả, vì phía server không có khoá nào
        SortedMap<String, String> params = signedWith("ke-tan-cong-tu-chon-khoa",
                Map.of("ref", "MOCK-REF-1", "state", "SUCCEEDED"));

        ResponseEntity<String> response = http.getForEntity(url(params), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(db.webhookCount()).isZero();
    }

    @Test
    @DisplayName("Thiếu webhook-secret: webhook KHÔNG có chữ ký cũng bị 401, không có đường tắt nào")
    void thieu_khoa_thi_webhook_khong_chu_ky_cung_bi_tu_choi() {
        SortedMap<String, String> params = new TreeMap<>(Map.of("ref", "MOCK-REF-1"));

        assertThat(http.getForEntity(url(params), String.class).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(db.webhookCount()).isZero();
    }

    private static SortedMap<String, String> signedWith(String secret, Map<String, String> params) {
        SortedMap<String, String> sorted = new TreeMap<>(params);
        String data = sorted.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.US_ASCII)
                        + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.US_ASCII))
                .collect(Collectors.joining("&"));
        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA512"));
            sorted.put("signature", HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8))));
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
        return sorted;
    }

    private String url(SortedMap<String, String> params) {
        String query = params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        return "http://localhost:" + port + "/webhooks/mock?" + query;
    }
}
