package com.walletflow.bank;

import com.walletflow.bank.support.BankDb;
import com.walletflow.common.bank.ProviderContext;
import com.walletflow.common.bank.ProviderOperationView;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code GET /internal/provider-transactions?transactionId=...} (roadmap 2.5 PR3) — ngữ cảnh provider của
 * một lệnh tiền cho người trực.
 *
 * <p>Tính chất quan trọng nhất: <b>{@code raw_response} chỉ rời bank-adapter sau khi đã che</b>. Đây là lần
 * đầu cột đó đi ra khỏi DB; nó là nguyên văn thứ cổng thanh toán gửi về nên ta không kiểm soát được nó chứa
 * gì. Một số thẻ hay một chữ ký lọt ra màn hình người trực là sự cố PCI/bảo mật.
 *
 * <p>Các tính chất còn lại: thứ tự thu → hoàn, "chưa có thao tác" là 200 rỗng chứ không 404 (phân biệt được
 * với "không hỏi được bank-adapter"), cô lập tenant, và {@code raw_response = NULL} không làm nổ API.
 *
 * <p>Chỉ cần Postgres: đường này chỉ đọc DB, không đụng Kafka (listener tắt, relay outbox bị đẩy ra xa).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "outbox.relay.delay-ms=3600000",
        "walletflow.bank.provider=mock",
        "walletflow.bank.mock.allowed=true",
        "walletflow.bank.mock.auto-settle-after=",
        // Không luồng nền nào được đụng vào hàng test dựng sẵn
        "walletflow.bank.status-check.enabled=false",
        "walletflow.bank.webhook.enabled=false"
})
@Testcontainers
class InternalProviderContextIT {

    private static final String PAN = "4111111111111111";
    private static final String SECURE_HASH = "9f2c1ab4d5e6f70812345678deadbeefcafebabe0011223344556677889900aabbccddeeff";
    private static final String TOKEN = "tok_live_Zk39sJdQ81xYw";

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("railsdb");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @LocalServerPort
    int port;
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

    // ------------------------------------------------------------------
    // 1 — Che dữ liệu nhạy cảm
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ raw_response chứa số thẻ, chữ ký và token: API KHÔNG trả giá trị thật, chỉ trả '***'")
    void raw_response_nhay_cam_bi_che_truoc_khi_roi_bank_adapter() {
        UUID transactionId = UUID.randomUUID();
        db.insertOperation(tenant, transactionId, "CHARGE", "mock", "SUCCEEDED", "REF-" + transactionId, null,
                50_000, null, """
                        {"vnp_ResponseCode":"00","vnp_TransactionNo":"1234567890123",
                         "card":"%s","vnp_SecureHash":"%s","token": "%s"}
                        """.formatted(PAN, SECURE_HASH, TOKEN), 60);

        Raw raw = getRaw(tenant, transactionId);
        ProviderContext context = getContext(tenant, transactionId);

        // Kiểm trên BODY THÔ chứ không chỉ trên trường đã parse: một chỗ nào đó trong response còn
        // mang giá trị thật (kể cả ở trường khác) cũng là rò rỉ.
        assertThat(raw.body())
                .as("số thẻ qua được Luhn không được rời bank-adapter")
                .doesNotContain(PAN)
                .as("chữ ký VNPay")
                .doesNotContain(SECURE_HASH)
                .as("token")
                .doesNotContain(TOKEN);

        String masked = context.operations().getFirst().rawResponse();
        assertThat(masked).contains("***");
        assertThat(masked)
                .as("giữ 4 số cuối như ngân hàng hiển thị, đủ đối chiếu nhưng không đủ để dùng")
                .contains("***1111");
        assertThat(masked)
                .as("che có chọn lọc: mã lỗi provider là thứ người trực CẦN đọc, không được bị xoá theo")
                .contains("\"vnp_ResponseCode\"")
                .contains("\"00\"")
                .as("dãy số dài không phải thẻ (không qua Luhn) giữ nguyên để còn đối chiếu")
                .contains("1234567890123");
    }

    // ------------------------------------------------------------------
    // 2 — Thu rồi hoàn
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Lệnh có cả CHARGE và REFUND: 2 thao tác, CHARGE trước REFUND, REFUND trỏ về id của CHARGE")
    void thu_roi_hoan_tra_ve_theo_thu_tu_va_refund_tro_ve_charge() {
        UUID transactionId = UUID.randomUUID();
        UUID chargeId = db.insertOperation(tenant, transactionId, "CHARGE", "mock", "SUCCEEDED",
                "CH-" + transactionId, null, 80_000, null, "{\"state\":\"paid\"}", 120);
        UUID refundId = db.insertOperation(tenant, transactionId, "REFUND", "mock", "SUCCEEDED",
                "RF-" + transactionId, null, 80_000, chargeId, "{\"state\":\"refunded\"}", 30);
        // Ghi lại hàng CHARGE sau cùng: Postgres đặt phiên bản mới của hàng ở cuối heap, nên một câu SELECT
        // thiếu ORDER BY created_at sẽ trả REFUND trước. Có thế test mới phân biệt được "đúng thứ tự" với
        // "may mắn đúng thứ tự chèn".
        jdbcTemplate.update("UPDATE provider_transactions SET updated_at = now() WHERE id = ?", chargeId);

        ProviderContext context = getContext(tenant, transactionId);

        assertThat(context.transactionId()).isEqualTo(transactionId);
        assertThat(context.operations()).extracting(ProviderOperationView::operation)
                .as("đọc từ trên xuống là đọc đúng trình tự câu chuyện: thu rồi mới hoàn")
                .containsExactly(EBankOperation.CHARGE, EBankOperation.REFUND);
        ProviderOperationView charge = context.operations().get(0);
        ProviderOperationView refund = context.operations().get(1);
        assertThat(charge.id()).isEqualTo(chargeId);
        assertThat(charge.refundOf()).as("lần THU không hoàn gì cả").isNull();
        assertThat(refund.id()).isEqualTo(refundId);
        assertThat(refund.refundOf())
                .as("người trực phải thấy lần hoàn này hoàn đúng lần thu nào")
                .isEqualTo(chargeId);
        assertThat(charge.createdAt()).isBefore(refund.createdAt());
    }

    // ------------------------------------------------------------------
    // 3 — Chưa có thao tác ≠ lỗi
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ transactionId chưa có thao tác nào: 200 + danh sách RỖNG, KHÔNG phải 404")
    void chua_co_thao_tac_tra_ve_200_rong_khong_phai_404() {
        UUID neverSeen = UUID.randomUUID();

        Raw raw = getRaw(tenant, neverSeen);

        assertThat(raw.status())
                .as("404 buộc người gọi coi 'bank-adapter chưa từng nhận lệnh' như 'không hỏi được bank-adapter'")
                .isEqualTo(200);
        ProviderContext context = getContext(tenant, neverSeen);
        assertThat(context.transactionId()).isEqualTo(neverSeen);
        assertThat(context.operations()).isEmpty();
    }

    // ------------------------------------------------------------------
    // 4 — Cô lập tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Cô lập tenant: thao tác của tenant A không hiện khi hỏi bằng tenant B, trả rỗng y như id lạ")
    void thao_tac_cua_tenant_khac_khong_hien_ra() {
        UUID tenantB = UUID.randomUUID();
        UUID transactionId = UUID.randomUUID();
        UUID chargeId = db.insertOperation(tenant, transactionId, "CHARGE", "mock", "SUCCEEDED",
                "CH-" + transactionId, null, 50_000, null, "{\"state\":\"paid\"}", 10);

        Raw asOther = getRaw(tenantB, transactionId);
        Raw asUnknownId = getRaw(tenantB, UUID.randomUUID());

        assertThat(asOther.status())
                .as("không 404, không 403: khác biệt với id lạ sẽ lộ việc id này có tồn tại")
                .isEqualTo(200)
                .isEqualTo(asUnknownId.status());
        assertThat(getContext(tenantB, transactionId).operations()).isEmpty();
        assertThat(asOther.body())
                .as("không một mẩu dữ liệu nào của tenant A ra ngoài")
                .doesNotContain(chargeId.toString())
                .doesNotContain("CH-" + transactionId);
        assertThat(getContext(tenant, transactionId).operations()).extracting(ProviderOperationView::id)
                .as("chính tenant A vẫn thấy thao tác của mình")
                .containsExactly(chargeId);
    }

    // ------------------------------------------------------------------
    // 5 — raw_response NULL
    // ------------------------------------------------------------------
    @Test
    @DisplayName("raw_response NULL (provider chưa trả lời lần nào): không NPE, trả rawResponse = null")
    void raw_response_null_khong_no_va_tra_null() {
        UUID transactionId = UUID.randomUUID();
        db.insertOperation(tenant, transactionId, "CHARGE", "mock", "NEW", null, null,
                40_000, null, null, 5);

        Raw raw = getRaw(tenant, transactionId);
        ProviderContext context = getContext(tenant, transactionId);

        assertThat(raw.status()).isEqualTo(200);
        assertThat(context.operations()).singleElement().satisfies(op -> {
            assertThat(op.rawResponse()).isNull();
            assertThat(op.status()).isEqualTo("NEW");
            assertThat(op.providerRef()).isNull();
        });
    }

    // ------------------------------------------------------------------
    // 6 — Ánh xạ trường
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Các trường ánh xạ đúng: operation, provider, providerRef, amountMinor, currency, status, failureCode")
    void cac_truong_anh_xa_dung() {
        UUID transactionId = UUID.randomUUID();
        UUID failedId = db.insertOperation(tenant, transactionId, "CHARGE", "vnpay", "FAILED",
                "VNP-12345", "EXPIRED", 123_456, null, "{\"vnp_ResponseCode\":\"24\"}", 45);

        ProviderOperationView op = getContext(tenant, transactionId).operations().getFirst();

        assertThat(op.id()).isEqualTo(failedId);
        assertThat(op.operation()).isEqualTo(EBankOperation.CHARGE);
        assertThat(op.provider()).isEqualTo("vnpay");
        assertThat(op.providerRef()).isEqualTo("VNP-12345");
        assertThat(op.amountMinor()).as("tiền là long minor unit, không qua số thực").isEqualTo(123_456L);
        assertThat(op.currency()).isEqualTo(ECurrency.VND);
        assertThat(op.status()).isEqualTo("FAILED");
        assertThat(op.failureCode()).isEqualTo("EXPIRED");
        assertThat(op.createdAt()).isNotNull();
        assertThat(op.updatedAt()).isNotNull();
        assertThat(op.refundOf()).isNull();
        assertThat(op.rawResponse()).contains("\"vnp_ResponseCode\"").contains("\"24\"");
    }

    // ------------------------------------------------------------------
    // 7 — Thiếu header tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Thiếu header X-Tenant-Id: không trả dữ liệu (không đoán tenant), 500 không lộ message kỹ thuật")
    void thieu_header_tenant_khong_tra_du_lieu() {
        UUID transactionId = UUID.randomUUID();
        db.insertOperation(tenant, transactionId, "CHARGE", "mock", "SUCCEEDED", "CH-" + transactionId, null,
                50_000, null, "{\"state\":\"paid\"}", 10);

        Raw raw = RestClient.create("http://localhost:" + port).get()
                .uri("/internal/provider-transactions?transactionId={id}", transactionId)
                .exchange((request, response) -> new Raw(response.getStatusCode().value(),
                        new String(response.getBody().readAllBytes(), StandardCharsets.UTF_8)));

        // Quan sát thực tế: TenantFilter cho qua, TenantContext.require() ném IllegalStateException (lỗi
        // đường vào chứ không phải lỗi khách) -> GlobalExceptionHandler trả 500 INTERNAL_ERROR.
        assertThat(raw.status()).as("không bao giờ 200 khi không biết tenant").isEqualTo(500);
        assertThat(raw.body())
                .contains("INTERNAL_ERROR")
                .doesNotContain("TenantContext")
                .doesNotContain("paid")
                .doesNotContain("CH-" + transactionId);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private record Raw(int status, String body) {
    }

    /** Body thô dạng chuỗi — để khẳng định "không có giá trị nhạy cảm NÀO trong response", không chỉ ở một trường. */
    private Raw getRaw(UUID tenantId, UUID transactionId) {
        return RestClient.create("http://localhost:" + port).get()
                .uri("/internal/provider-transactions?transactionId={id}", transactionId)
                .header(AuthHeaders.TENANT_ID, tenantId.toString())
                .exchange((request, response) -> new Raw(response.getStatusCode().value(),
                        new String(response.getBody().readAllBytes(), StandardCharsets.UTF_8)));
    }

    private ProviderContext getContext(UUID tenantId, UUID transactionId) {
        ProviderContext context = RestClient.create("http://localhost:" + port).get()
                .uri("/internal/provider-transactions?transactionId={id}", transactionId)
                .header(AuthHeaders.TENANT_ID, tenantId.toString())
                .retrieve()
                .body(ProviderContext.class);
        assertThat(context).isNotNull();
        return context;
    }
}
