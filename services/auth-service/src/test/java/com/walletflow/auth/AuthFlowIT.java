package com.walletflow.auth;

import com.fasterxml.jackson.databind.JsonNode;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Jwk;
import io.jsonwebtoken.security.Jwks;
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

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test end-to-end cho vòng đời xác thực sau khi chuyển sang RS256 + refresh token xoay vòng.
 *
 * <p>Bốn tính chất được kiểm chứng, mỗi cái ứng với một lỗ hổng của bản trước:
 * <ol>
 *   <li><b>Token ký bằng RS256</b> và verify được bằng đúng public key lấy từ JWKS — nghĩa là
 *       gateway không cần (và không thể) cầm khoá ký.</li>
 *   <li><b>Refresh xoay vòng</b>: token cũ chết ngay sau khi dùng.</li>
 *   <li><b>Phát hiện tái sử dụng</b>: dùng lại token đã thu hồi giết cả chuỗi. Không có bước này
 *       thì rotation chỉ làm phiền kẻ trộm token chứ không chặn được hắn.</li>
 *   <li><b>Bộ đếm đăng nhập sai SỐNG SÓT qua exception</b> — xem {@code LoginAttemptService}. Đây
 *       là tính chất dễ hỏng âm thầm nhất: nếu bộ đếm nằm chung transaction với luồng đăng nhập,
 *       nó bị rollback cùng exception và chống brute-force không hoạt động, trong khi code trông
 *       hoàn toàn đúng.</li>
 * </ol>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eureka.client.enabled=false",
                "spring.cloud.config.enabled=false",
                "management.tracing.enabled=false"
        })
@Testcontainers
class AuthFlowIT {

    private static final String PASSWORD = "mat-khau-du-dai-12";

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("authdb");

    /** Khoá chỉ dùng cho test — sinh mới mỗi lần chạy, không có khoá cố định nào trong repo. */
    private static final KeyPair KEY_PAIR = generateKeyPair();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("jwt.private-key",
                () -> Base64.getEncoder().encodeToString(KEY_PAIR.getPrivate().getEncoded()));
        registry.add("jwt.public-key",
                () -> Base64.getEncoder().encodeToString(KEY_PAIR.getPublic().getEncoded()));
    }

    @LocalServerPort
    int port;
    @Autowired
    TestRestTemplate restTemplate;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("DELETE FROM refresh_tokens");
        jdbcTemplate.execute("TRUNCATE audit_events");
        jdbcTemplate.execute("DELETE FROM users");
    }

    // ------------------------------------------------------------------
    // 1 — Token ký RS256, verify được bằng khoá công khai từ JWKS
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Access token ký RS256 và verify được bằng public key lấy từ /.well-known/jwks.json")
    void access_token_verify_duoc_bang_khoa_tu_jwks() {
        JsonNode auth = register("a@b.com").getBody().get("data");
        String accessToken = auth.get("accessToken").asText();

        PublicKey fromJwks = publicKeyFromJwks();

        Claims claims = Jwts.parser()
                .verifyWith(fromJwks)
                .requireIssuer("walletflow-auth")
                .build()
                .parseSignedClaims(accessToken)
                .getPayload();

        assertThat(claims.getSubject())
                .as("sub là userId chứ không phải email — email đổi được, danh tính thì không")
                .isEqualTo(auth.get("userId").asText());
        assertThat(claims.get("email")).isEqualTo("a@b.com");
        assertThat(claims.get("role")).isEqualTo("USER");
        assertThat(auth.get("expiresInSeconds").asLong())
                .as("access token sống ngắn vì nó không thu hồi được")
                .isEqualTo(900);
    }

    // ------------------------------------------------------------------
    // 2 + 3 — ⭐ Xoay vòng và phát hiện tái sử dụng
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Refresh xoay vòng token cũ; dùng lại token đã thu hồi sẽ giết cả chuỗi")
    void refresh_xoay_vong_va_phat_hien_tai_su_dung() {
        String firstRefresh = register("a@b.com").getBody()
                .get("data").get("refreshToken").asText();

        // Lần refresh hợp lệ -> nhận token mới
        ResponseEntity<JsonNode> rotated = post("/api/auth/refresh",
                Map.of("refreshToken", firstRefresh));
        assertThat(rotated.getStatusCode()).isEqualTo(HttpStatus.OK);
        String secondRefresh = rotated.getBody().get("data").get("refreshToken").asText();
        assertThat(secondRefresh).isNotEqualTo(firstRefresh);

        // Dùng lại token ĐÃ bị xoay -> phát hiện tái sử dụng
        ResponseEntity<JsonNode> reuse = post("/api/auth/refresh",
                Map.of("refreshToken", firstRefresh));
        assertThat(reuse.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(reuse.getBody().get("errorCode").asText()).isEqualTo("TOKEN_REUSE_DETECTED");

        // Audit phải được ghi dù request kết thúc bằng exception (AuditService dùng REQUIRES_NEW)
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM audit_events WHERE action = 'TOKEN_REUSE_DETECTED'",
                Integer.class))
                .as("sự kiện bảo mật phải sống sót qua rollback của luồng request")
                .isEqualTo(1);

        // ...và token MỚI cũng phải chết theo: cả family bị thu hồi.
        // Nếu chỉ token cũ chết, kẻ trộm vẫn giữ được chuỗi của hắn và cơ chế này vô dụng.
        ResponseEntity<JsonNode> afterPurge = post("/api/auth/refresh",
                Map.of("refreshToken", secondRefresh));
        assertThat(afterPurge.getStatusCode())
                .as("phát hiện trộm token phải thu hồi TOÀN BỘ family, không riêng token bị dùng lại")
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(afterPurge.getBody().get("errorCode").asText())
                .as("token trong family đã bị đốt: mọi lần dùng tiếp đều là tái sử dụng")
                .isEqualTo("TOKEN_REUSE_DETECTED");
    }

    @Test
    @DisplayName("Đăng xuất thu hồi cả chuỗi token của phiên")
    void dang_xuat_thu_hoi_ca_chuoi() {
        String refresh = register("a@b.com").getBody().get("data").get("refreshToken").asText();

        assertThat(post("/api/auth/logout", Map.of("refreshToken", refresh)).getStatusCode())
                .isEqualTo(HttpStatus.OK);

        assertThat(post("/api/auth/refresh", Map.of("refreshToken", refresh)).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    // ------------------------------------------------------------------
    // 4 — ⭐ Bộ đếm đăng nhập sai phải sống sót qua exception
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Sai mật khẩu 5 lần thì khoá tài khoản — bộ đếm không bị rollback theo exception")
    void sai_mat_khau_nhieu_lan_thi_khoa_tai_khoan() {
        register("a@b.com");

        for (int i = 1; i <= 4; i++) {
            ResponseEntity<JsonNode> attempt = post("/api/auth/login",
                    Map.of("email", "a@b.com", "password", "sai-mat-khau-roi"));
            assertThat(attempt.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

            assertThat(jdbcTemplate.queryForObject(
                    "SELECT failed_login_count FROM users WHERE email = 'a@b.com'", Integer.class))
                    .as("lần sai thứ %d phải được GHI LẠI; nếu luôn bằng 0 nghĩa là bộ đếm "
                            + "đang bị rollback cùng exception và chống brute-force vô hiệu", i)
                    .isEqualTo(i);
        }

        // Lần thứ 5 chạm ngưỡng -> khoá
        post("/api/auth/login", Map.of("email", "a@b.com", "password", "sai-mat-khau-roi"));

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM users WHERE email = 'a@b.com'", String.class))
                .isEqualTo("LOCKED");

        // Đúng mật khẩu nhưng đang khoá -> vẫn từ chối
        ResponseEntity<JsonNode> correct = post("/api/auth/login",
                Map.of("email", "a@b.com", "password", PASSWORD));
        assertThat(correct.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(correct.getBody().get("errorCode").asText()).isEqualTo("ACCOUNT_LOCKED");

        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM audit_events WHERE action = 'ACCOUNT_LOCKED'", Integer.class))
                .isEqualTo(1);
    }

    @Test
    @DisplayName("Email không phân biệt hoa thường: không đăng ký trùng được bằng cách đổi chữ hoa")
    void email_khong_phan_biet_hoa_thuong() {
        assertThat(register("a@b.com").getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<JsonNode> duplicate = post("/api/auth/register", Map.of(
                "email", "A@B.COM", "password", PASSWORD, "fullName", "Nguyen Van A"));

        assertThat(duplicate.getStatusCode())
                .as("cho đăng ký cả hai sẽ tạo hai tài khoản cùng danh tính nhưng khác ví")
                .isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    @DisplayName("Mật khẩu ngắn hơn 12 ký tự bị từ chối")
    void mat_khau_ngan_bi_tu_choi() {
        ResponseEntity<JsonNode> weak = post("/api/auth/register", Map.of(
                "email", "a@b.com", "password", "ngan", "fullName", "Nguyen Van A"));
        assertThat(weak.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private ResponseEntity<JsonNode> register(String email) {
        return post("/api/auth/register", Map.of(
                "email", email, "password", PASSWORD, "fullName", "Nguyen Van A"));
    }

    private ResponseEntity<JsonNode> post(String path, Map<String, String> body) {
        return restTemplate.postForEntity(url(path), body, JsonNode.class);
    }

    private PublicKey publicKeyFromJwks() {
        String json = restTemplate.getForObject(url("/.well-known/jwks.json"), String.class);
        assertThat(json).as("JWKS phải công bố khoá").contains("\"kty\"");
        assertThat(json)
                .as("JWKS CHỈ được chứa public key — lọt 'd' là lộ private key ra Internet")
                .doesNotContain("\"d\"");

        Jwk<?> jwk = Jwks.setParser().build().parse(json).getKeys().iterator().next();
        return (PublicKey) jwk.toKey();
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private static KeyPair generateKeyPair() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
