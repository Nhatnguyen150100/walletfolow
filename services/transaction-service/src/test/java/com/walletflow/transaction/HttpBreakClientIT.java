package com.walletflow.transaction;

import com.walletflow.common.reconciliation.ReconciliationBreakView;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.client.BreakClient.CloseOutcome;
import com.walletflow.transaction.client.EBreakSource;
import com.walletflow.transaction.client.HttpBreakClient;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link HttpBreakClient} nói chuyện với một HTTP server THẬT (roadmap 3.3, ADR-025).
 *
 * <p><b>Vì sao cần file này tuy đã có {@code AdminBreakIT} và ba {@code InternalBreakAdminIT}:</b>
 * hai bộ kia chạy ở hai phía của một đường nối mà KHÔNG bộ nào chạy chính đường nối đó —
 * {@code AdminBreakIT} thay client bằng fake, còn ba IT kia gọi controller bằng {@code RestClient}
 * tự dựng. Nếu path, tên query param, hay header lệch nhau, cả hai bộ vẫn xanh.
 *
 * <p>Và chế độ hỏng của chỗ lệch đó im lặng một cách nguy hiểm: path sai ⇒ service sở hữu trả 404 ⇒
 * client dịch thành {@code NOT_FOUND} ⇒ người trực đọc được đúng câu "không có chỗ lệch đang mở",
 * y như một break vừa được ai đó đóng hợp lệ. Ở đường đọc còn tệ hơn: path sai ⇒ {@code available
 * = false} vĩnh viễn, mà đó lại là trạng thái "bình thường khi service chết".
 *
 * <p>Vì vậy test này khẳng định HỢP ĐỒNG DÂY: đúng đường dẫn, đúng query param, đúng header, đúng
 * body — rồi mới tới việc map mã trạng thái sang {@link CloseOutcome}.
 *
 * <p>Dùng {@code com.sun.net.httpserver} thay vì {@code MockRestServiceServer}: cái sau thay
 * {@code ClientHttpRequestFactory} nên KHÔNG kiểm được timeout, và quan trọng hơn là nó không chứng
 * minh được lời gọi thật sự đi ra mạng với đúng URL. Repo cũng không có WireMock.
 */
class HttpBreakClientIT {

    private static final UUID TENANT = UUID.randomUUID();

    private HttpServer server;
    private HttpBreakClient client;

    /** Mọi request server nhận được, để assert hợp đồng dây. */
    private final List<Received> received = new ArrayList<>();
    private volatile int status = 200;
    private volatile String body = "[]";
    private final AtomicInteger delayMs = new AtomicInteger();

    private record Received(String method, String path, String query, String tenant, String user, String body) {
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String payload = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            received.add(new Received(
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestURI().getQuery(),
                    exchange.getRequestHeaders().getFirst(AuthHeaders.TENANT_ID),
                    exchange.getRequestHeaders().getFirst(AuthHeaders.USER_ID),
                    payload));
            try {
                Thread.sleep(delayMs.get());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            // 204 không được phép có thân — gửi thân cho nó là lỗi giao thức, server sẽ nổ.
            exchange.sendResponseHeaders(status, status == 204 ? -1 : out.length);
            if (status != 204) {
                exchange.getResponseBody().write(out);
            }
            exchange.close();
        });
        server.start();

        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        // Cả ba nguồn trỏ về cùng một server giả: test này kiểm hợp đồng dây, không kiểm định tuyến.
        client = new HttpBreakClient(RestClient.builder(), base, base, base,
                Duration.ofSeconds(1), Duration.ofMillis(300));
        TenantContext.set(TENANT);
    }

    @AfterEach
    void stopServer() {
        TenantContext.clear();
        server.stop(0);
    }

    // ------------------------------------------------------------------
    // Hợp đồng dây
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Đường đọc gọi đúng /internal/reconciliation/breaks?limit=, kèm X-Tenant-Id")
    void duong_doc_goi_dung_duong_dan_va_header() {
        body = "[]";

        Optional<List<ReconciliationBreakView>> result = client.openBreaks(EBreakSource.LEDGER, 37);

        assertThat(result).isPresent();
        assertThat(received).hasSize(1);
        Received call = received.getFirst();
        assertThat(call.method()).isEqualTo("GET");
        assertThat(call.path())
                .as("lệch một ký tự ở đây là `available=false` vĩnh viễn mà không ai nghi ngờ")
                .isEqualTo("/internal/reconciliation/breaks");
        assertThat(call.query()).isEqualTo("limit=37");
        assertThat(call.tenant()).isEqualTo(TENANT.toString());
    }

    @Test
    @DisplayName("⭐ Đường đóng gọi đúng .../{id}/close, kèm X-Tenant-Id + X-User-Id và body {note}")
    void duong_dong_goi_dung_duong_dan_header_va_body() {
        UUID breakId = UUID.randomUUID();
        UUID operator = UUID.randomUUID();
        status = 204;

        CloseOutcome outcome = client.close(EBreakSource.BANK, breakId, operator, "đã đối chiếu portal");

        assertThat(outcome).isEqualTo(CloseOutcome.CLOSED);
        Received call = received.getFirst();
        assertThat(call.method()).isEqualTo("POST");
        assertThat(call.path()).isEqualTo("/internal/reconciliation/breaks/" + breakId + "/close");
        assertThat(call.tenant()).isEqualTo(TENANT.toString());
        assertThat(call.user())
                .as("người đóng đi theo header, KHÔNG nằm trong body (CLAUDE.md)")
                .isEqualTo(operator.toString());
        assertThat(call.body()).contains("\"note\"").contains("đã đối chiếu portal");
        assertThat(call.body()).doesNotContain(operator.toString());
    }

    @Test
    @DisplayName("Đọc được danh sách thật: JSON của owner map đúng vào ReconciliationBreakView")
    void doc_duoc_danh_sach_that() {
        UUID id = UUID.randomUUID();
        body = """
                [{"id":"%s","tenantId":"%s","type":"LEDGER_IMBALANCE","subject":"VND","currency":"VND",
                  "expectedMinor":0,"actualMinor":-250,"deltaMinor":-250,"autoRepaired":false,
                  "timesSeen":3,"detail":"lệch","firstSeenAt":"2026-11-01T00:00:00Z",
                  "detectedAt":"2026-11-02T00:00:00Z"}]
                """.formatted(id, TENANT);

        List<ReconciliationBreakView> breaks = client.openBreaks(EBreakSource.LEDGER, 50).orElseThrow();

        assertThat(breaks).hasSize(1);
        assertThat(breaks.getFirst().id()).isEqualTo(id);
        assertThat(breaks.getFirst().actualMinor()).isEqualTo(-250);
        assertThat(breaks.getFirst().timesSeen()).isEqualTo(3);
    }

    // ------------------------------------------------------------------
    // Map mã trạng thái -> kết cục
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ 404 → NOT_FOUND, 4xx khác → REJECTED (chắc chắn chưa đóng), 5xx → UNAVAILABLE")
    void map_dung_tung_ma_trang_thai() {
        status = 404;
        assertThat(close()).isEqualTo(CloseOutcome.NOT_FOUND);

        status = 400;
        assertThat(close())
                .as("4xx là từ chối DỨT KHOÁT — không được trộn vào 'không hỏi được' (CLAUDE.md §7)")
                .isEqualTo(CloseOutcome.REJECTED);

        status = 403;
        assertThat(close()).isEqualTo(CloseOutcome.REJECTED);

        status = 500;
        assertThat(close()).isEqualTo(CloseOutcome.UNAVAILABLE);

        status = 503;
        assertThat(close()).isEqualTo(CloseOutcome.UNAVAILABLE);
    }

    @Test
    @DisplayName("Owner trả lỗi ở đường ĐỌC: Optional rỗng = KHÔNG HỎI ĐƯỢC, không phải danh sách rỗng")
    void duong_doc_hong_tra_optional_rong() {
        status = 500;

        assertThat(client.openBreaks(EBreakSource.WALLET, 50))
                .as("rỗng và không-hỏi-được là hai câu trả lời khác nhau (ADR-021 B1)")
                .isEmpty();
    }

    // ------------------------------------------------------------------
    // Hỏng mạng
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Owner chậm quá read-timeout: đường đọc KHÔNG ném, đường đóng → UNAVAILABLE")
    void qua_timeout_thi_khong_nem() {
        delayMs.set(1200);   // read-timeout đặt 300ms

        assertThat(client.openBreaks(EBreakSource.LEDGER, 50))
                .as("màn hình chữa cháy phải mở được kể cả khi một service treo")
                .isEmpty();
        assertThat(close())
                .as("treo thì CHƯA BIẾT đã đóng hay chưa — không được coi là đã đóng")
                .isEqualTo(CloseOutcome.UNAVAILABLE);
    }

    @Test
    @DisplayName("Owner chết hẳn (cổng đóng): cả hai đường đều không ném")
    void owner_chet_thi_khong_nem() {
        server.stop(0);

        assertThat(client.openBreaks(EBreakSource.BANK, 50)).isEmpty();
        assertThat(close()).isEqualTo(CloseOutcome.UNAVAILABLE);
    }

    @Test
    @DisplayName("Thân trả về rỗng/không phải JSON hợp lệ cũng không ném")
    void than_hong_thi_khong_nem() {
        body = "khong-phai-json";

        assertThat(client.openBreaks(EBreakSource.LEDGER, 50)).isEmpty();
    }

    private CloseOutcome close() {
        return client.close(EBreakSource.LEDGER, UUID.randomUUID(), UUID.randomUUID(), "lý do");
    }
}
