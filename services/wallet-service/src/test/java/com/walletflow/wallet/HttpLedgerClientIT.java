package com.walletflow.wallet;

import com.sun.net.httpserver.HttpServer;
import com.walletflow.common.ledger.AccountStatement;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.client.HttpLedgerClient;
import com.walletflow.wallet.client.LedgerUnavailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Client gọi API sao kê của ledger, chạy với một HTTP server thật trên localhost — không mock lớp
 * HTTP, vì thứ cần kiểm chính là hình dạng request/response trên dây.
 */
class HttpLedgerClientIT {

    private static final UUID TENANT = UUID.randomUUID();
    private static final String WALLET = UUID.randomUUID().toString();

    private HttpServer server;
    private final AtomicReference<String> seenTenant = new AtomicReference<>();
    private final AtomicReference<String> seenPath = new AtomicReference<>();
    private volatile int status;
    private volatile String body;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/internal/accounts/", exchange -> {
            seenTenant.set(exchange.getRequestHeaders().getFirst(AuthHeaders.TENANT_ID));
            seenPath.set(exchange.getRequestURI().toString());
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
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

    @Test
    @DisplayName("200: đọc được sao kê, gửi đúng tenant từ TenantContext qua header")
    void doc_duoc_sao_ke() {
        status = 200;
        body = """
                {"accountId":"%s","accountRef":"%s","currency":"VND","status":"ACTIVE","statusVersion":3,
                 "lines":[{"entryId":"%s","transactionId":"%s","type":"TOPUP","amountMinor":5000,
                           "postedAt":"2026-10-16T00:00:00Z"}]}
                """.formatted(UUID.randomUUID(), WALLET, UUID.randomUUID(), UUID.randomUUID());

        Optional<AccountStatement> statement = call(clientFor(server.getAddress().getPort()));

        assertThat(statement).isPresent();
        assertThat(statement.get().balanceMinor()).isEqualTo(5_000);
        assertThat(statement.get().statusVersion()).isEqualTo(3);
        assertThat(seenTenant.get()).isEqualTo(TENANT.toString());
        assertThat(seenPath.get()).isEqualTo("/internal/accounts/" + WALLET + "?currency=VND");
    }

    @Test
    @DisplayName("404: ledger chưa có tài khoản -> rỗng, không phải lỗi")
    void chua_co_tai_khoan_la_rong() {
        status = 404;
        body = "";

        assertThat(call(clientFor(server.getAddress().getPort()))).isEmpty();
    }

    @Test
    @DisplayName("5xx hoặc không kết nối được: LedgerUnavailableException (503), không phải 500 chung chung")
    void ledger_loi_thanh_503() {
        status = 500;
        body = "{}";
        assertThatThrownBy(() -> call(clientFor(server.getAddress().getPort())))
                .isInstanceOf(LedgerUnavailableException.class);

        int deadPort = server.getAddress().getPort();
        server.stop(0);
        assertThatThrownBy(() -> call(clientFor(deadPort)))
                .isInstanceOf(LedgerUnavailableException.class)
                .as("message thành `detail` của câu trả lời HTTP: không được lộ địa chỉ nội bộ")
                .hasMessageNotContaining("localhost");
    }

    private static HttpLedgerClient clientFor(int port) {
        return new HttpLedgerClient(RestClient.builder(), "http://localhost:" + port,
                Duration.ofSeconds(1), Duration.ofSeconds(2));
    }

    private static Optional<AccountStatement> call(HttpLedgerClient client) {
        return TenantContext.callWith(TENANT, () -> client.statementOf(WALLET, ECurrency.VND));
    }
}
