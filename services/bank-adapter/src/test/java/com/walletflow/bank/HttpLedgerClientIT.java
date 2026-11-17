package com.walletflow.bank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import com.walletflow.bank.client.HttpLedgerClient;
import com.walletflow.bank.client.LedgerUnavailableException;
import com.walletflow.common.ledger.TopupEntryRef;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code HttpLedgerClient} chạy với một HTTP server thật trên localhost — không mock lớp HTTP, vì thứ cần kiểm
 * chính là hình dạng request/response trên dây (header tenant, thân POST, tham số truy vấn, định dạng Instant).
 *
 * <p>Các IT đối soát thay client này bằng {@code FakeLedger}, nên đây là chỗ DUY NHẤT code HTTP thật được chạy.
 * Tính chất quan trọng nhất: ledger không trả lời được thì phải ném {@link LedgerUnavailableException}, KHÔNG BAO
 * GIỜ trả một câu trả lời rỗng — rỗng nghĩa là "sổ cái chưa có bút toán", và đối soát sẽ ghi break giả cho cả lô.
 */
class HttpLedgerClientIT {

    private static final UUID TENANT = UUID.randomUUID();
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Một request server đã nhận. */
    private record Seen(String method, String path, String rawQuery, String tenantHeader, String contentType,
                        String body) {
    }

    private HttpServer server;
    private final List<Seen> seen = new CopyOnWriteArrayList<>();
    private volatile int status;
    private volatile String responseBody;

    @BeforeEach
    void start() throws IOException {
        status = 200;
        responseBody = "[]";
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/internal/entries", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            seen.add(new Seen(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
                    exchange.getRequestURI().getRawQuery(),
                    exchange.getRequestHeaders().getFirst(AuthHeaders.TENANT_ID),
                    exchange.getRequestHeaders().getFirst("Content-Type"), body));
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                exchange.getResponseBody().write(bytes);
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    // ------------------------------------------------------------------
    // externalMovementsOf
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ external-movements: POST đúng đường dẫn, header X-Tenant-Id lấy từ TenantContext, thân {transactionIds}, đọc được kết quả")
    void external_movements_gui_dung_request_va_doc_duoc_ket_qua() throws Exception {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        responseBody = """
                [{"transactionId":"%s","currency":"VND","netInMinor":100000},
                 {"transactionId":"%s","currency":"VND","netInMinor":0},
                 {"transactionId":"%s","currency":"USD","netInMinor":-250}]
                """.formatted(a, b, b);

        Map<UUID, Map<ECurrency, Long>> result = movements(List.of(a, b));

        assertThat(seen).singleElement().satisfies(request -> {
            assertThat(request.method()).isEqualTo("POST");
            assertThat(request.path()).isEqualTo("/internal/entries/external-movements");
            assertThat(request.tenantHeader()).as("tenant đi bằng header từ TenantContext, không qua tham số")
                    .isEqualTo(TENANT.toString());
            assertThat(request.contentType()).startsWith("application/json");
        });
        JsonNode body = JSON.readTree(seen.getFirst().body());
        assertThat(body.path("transactionIds")).extracting(JsonNode::asText)
                .containsExactly(a.toString(), b.toString());
        assertThat(result.get(a)).containsExactlyEntriesOf(Map.of(ECurrency.VND, 100_000L));
        assertThat(result.get(b)).as("0 vẫn có mặt (khác vắng mặt); mỗi loại tiền một dòng")
                .containsEntry(ECurrency.VND, 0L).containsEntry(ECurrency.USD, -250L).hasSize(2);
    }

    @Test
    @DisplayName("⭐ external-movements: hai dòng trùng (lệnh, loại tiền) được CỘNG DỒN, không ghi đè — không để mất tiền khỏi phép so")
    void external_movements_dong_trung_thi_cong_don() {
        UUID a = UUID.randomUUID();
        responseBody = """
                [{"transactionId":"%s","currency":"VND","netInMinor":60000},
                 {"transactionId":"%s","currency":"VND","netInMinor":40000}]
                """.formatted(a, a);

        Map<UUID, Map<ECurrency, Long>> result = movements(List.of(a));

        assertThat(result.get(a)).containsExactlyEntriesOf(Map.of(ECurrency.VND, 100_000L));
    }

    @Test
    @DisplayName("external-movements: lệnh ledger không nhắc tới thì VẮNG MẶT trong map; mảng rỗng cho map rỗng")
    void external_movements_lenh_vang_mat_thi_khong_co_trong_map() {
        responseBody = "[]";

        Map<UUID, Map<ECurrency, Long>> result = movements(List.of(UUID.randomUUID()));

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("external-movements: danh sách rỗng thì không gọi mạng và trả map rỗng")
    void external_movements_danh_sach_rong_khong_goi_mang() {
        Map<UUID, Map<ECurrency, Long>> result = movements(List.of());

        assertThat(result).isEmpty();
        assertThat(seen).isEmpty();
    }

    @Test
    @DisplayName("external-movements: thiếu tenant trong luồng thì nổ (lỗi lập trình), không gửi request không-tenant")
    void external_movements_thieu_tenant_thi_khong_gui_request() {
        HttpLedgerClient client = clientFor(server.getAddress().getPort());

        assertThatThrownBy(() -> client.externalMovementsOf(List.of(UUID.randomUUID())))
                .isInstanceOf(IllegalStateException.class);
        assertThat(seen).isEmpty();
    }

    @Test
    @DisplayName("⭐ external-movements: ledger trả 4xx/5xx thì LedgerUnavailableException, KHÔNG phải kết quả rỗng")
    void external_movements_loi_http_thi_ne_ngoai_le_chu_khong_tra_rong() {
        for (int failure : new int[]{400, 404, 500, 503}) {
            status = failure;
            responseBody = "{\"errorCode\":\"X\"}";

            assertThatThrownBy(() -> movements(List.of(UUID.randomUUID())))
                    .as("HTTP %d", failure)
                    .isInstanceOf(LedgerUnavailableException.class)
                    .hasMessageNotContaining("localhost");
        }
    }

    @Test
    @DisplayName("external-movements: không kết nối được (cổng đóng) thì LedgerUnavailableException, message không lộ địa chỉ nội bộ")
    void external_movements_khong_ket_noi_duoc_thi_ne_ngoai_le() {
        int deadPort = server.getAddress().getPort();
        server.stop(0);

        assertThatThrownBy(() -> TenantContext.callWith(TENANT,
                () -> clientFor(deadPort).externalMovementsOf(List.of(UUID.randomUUID()))))
                .isInstanceOf(LedgerUnavailableException.class)
                .hasMessageNotContaining("localhost");
    }

    // ------------------------------------------------------------------
    // topupsAfter
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ topups: trang đầu (con trỏ null) chỉ gửi before + limit, KHÔNG gửi afterPostedAt/afterId; không cần tenant")
    void topups_trang_dau_khong_gui_con_tro() {
        UUID tenantOfEntry = UUID.randomUUID();
        UUID entryId = UUID.randomUUID();
        UUID transactionId = UUID.randomUUID();
        Instant postedAt = Instant.parse("2026-11-16T03:04:05.123456Z");
        responseBody = """
                [{"tenantId":"%s","entryId":"%s","transactionId":"%s","postedAt":"2026-11-16T03:04:05.123456Z"}]
                """.formatted(tenantOfEntry, entryId, transactionId);
        Instant before = Instant.parse("2026-11-16T04:00:00Z");

        List<TopupEntryRef> page = clientFor(server.getAddress().getPort()).topupsAfter(null, null, before, 200);

        assertThat(seen).singleElement().satisfies(request -> {
            assertThat(request.method()).isEqualTo("GET");
            assertThat(request.path()).isEqualTo("/internal/entries/topups");
            assertThat(queryOf(request)).containsExactlyInAnyOrderEntriesOf(
                    Map.of("before", "2026-11-16T04:00:00Z", "limit", "200"));
            assertThat(request.tenantHeader()).as("quét xuyên tenant: không gắn tenant nào").isNull();
        });
        assertThat(page).containsExactly(new TopupEntryRef(tenantOfEntry, entryId, transactionId, postedAt));
    }

    @Test
    @DisplayName("⭐ topups: trang sau gửi đủ afterPostedAt + afterId đúng nguyên văn (micro-giây giữ nguyên) cùng before + limit")
    void topups_trang_sau_gui_du_con_tro() {
        UUID afterId = UUID.randomUUID();
        Instant afterPostedAt = Instant.parse("2026-11-16T03:04:05.123456Z");
        Instant before = Instant.parse("2026-11-16T04:00:00.5Z");

        clientFor(server.getAddress().getPort()).topupsAfter(afterPostedAt, afterId, before, 500);

        assertThat(queryOf(seen.getFirst())).containsExactlyInAnyOrderEntriesOf(Map.of(
                "afterPostedAt", "2026-11-16T03:04:05.123456Z",
                "afterId", afterId.toString(),
                "before", before.toString(),
                "limit", "500"));
    }

    @Test
    @DisplayName("topups: con trỏ thiếu một nửa (chỉ có afterId) thì không gửi nửa nào — tránh một con trỏ sai nghĩa")
    void topups_con_tro_thieu_mot_nua_thi_khong_gui() {
        clientFor(server.getAddress().getPort()).topupsAfter(null, UUID.randomUUID(),
                Instant.parse("2026-11-16T04:00:00Z"), 10);

        assertThat(queryOf(seen.getFirst())).containsOnlyKeys("before", "limit");
    }

    @Test
    @DisplayName("topups: trang rỗng cho danh sách rỗng")
    void topups_trang_rong() {
        responseBody = "[]";

        List<TopupEntryRef> page = clientFor(server.getAddress().getPort())
                .topupsAfter(null, null, Instant.parse("2026-11-16T04:00:00Z"), 10);

        assertThat(page).isEmpty();
    }

    @Test
    @DisplayName("⭐ topups: ledger trả 4xx/5xx hoặc không kết nối được thì LedgerUnavailableException, KHÔNG phải trang rỗng")
    void topups_loi_thi_ne_ngoai_le_chu_khong_tra_trang_rong() {
        Instant before = Instant.parse("2026-11-16T04:00:00Z");
        for (int failure : new int[]{400, 500}) {
            status = failure;
            responseBody = "{}";
            assertThatThrownBy(() -> clientFor(server.getAddress().getPort()).topupsAfter(null, null, before, 10))
                    .as("HTTP %d", failure)
                    .isInstanceOf(LedgerUnavailableException.class)
                    .hasMessageNotContaining("localhost");
        }

        int deadPort = server.getAddress().getPort();
        server.stop(0);
        assertThatThrownBy(() -> clientFor(deadPort).topupsAfter(null, null, before, 10))
                .isInstanceOf(LedgerUnavailableException.class)
                .hasMessageNotContaining("localhost");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private Map<UUID, Map<ECurrency, Long>> movements(Collection<UUID> ids) {
        return TenantContext.callWith(TENANT, () -> clientFor(server.getAddress().getPort()).externalMovementsOf(ids));
    }

    private static HttpLedgerClient clientFor(int port) {
        return new HttpLedgerClient(RestClient.builder(), "http://localhost:" + port,
                Duration.ofSeconds(1), Duration.ofSeconds(2));
    }

    /** Tham số truy vấn đã giải mã; giá trị Instant/UUID không chứa ký tự cần mã hoá nên so nguyên văn được. */
    private static Map<String, String> queryOf(Seen request) {
        Map<String, String> query = new LinkedHashMap<>();
        if (request.rawQuery() == null) {
            return query;
        }
        for (String pair : request.rawQuery().split("&")) {
            int eq = pair.indexOf('=');
            query.put(java.net.URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                    java.net.URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return query;
    }
}
