package com.walletflow.transaction;

import com.sun.net.httpserver.HttpServer;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.common.wallet.WalletInfo;
import com.walletflow.transaction.client.HttpWalletDirectory;
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

/** Client gọi API nội bộ của wallet-service, chạy với HTTP server thật trên localhost. */
class HttpWalletDirectoryIT {

    private static final UUID TENANT = UUID.randomUUID();
    private static final UUID WALLET = UUID.randomUUID();

    private HttpServer server;
    private final AtomicReference<String> seenTenant = new AtomicReference<>();
    private volatile int status;
    private volatile String body;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/internal/wallets/", exchange -> {
            seenTenant.set(exchange.getRequestHeaders().getFirst(AuthHeaders.TENANT_ID));
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
    @DisplayName("200: đọc được thông tin ví, gửi tenant từ TenantContext")
    void doc_duoc_thong_tin_vi() {
        UUID owner = UUID.randomUUID();
        status = 200;
        body = """
                {"id":"%s","userId":"%s","currency":"VND","status":"FROZEN"}
                """.formatted(WALLET, owner);

        Optional<WalletInfo> wallet = call(clientFor(server.getAddress().getPort()));

        assertThat(wallet).contains(new WalletInfo(WALLET, owner, ECurrency.VND, EWalletStatus.FROZEN));
        assertThat(seenTenant.get()).isEqualTo(TENANT.toString());
    }

    @Test
    @DisplayName("404: rỗng; 5xx hoặc mất kết nối: 503 WALLET_SERVICE_UNAVAILABLE")
    void loi_duoc_phan_loai() {
        status = 404;
        body = "";
        assertThat(call(clientFor(server.getAddress().getPort()))).isEmpty();

        status = 500;
        body = "{}";
        assertThatThrownBy(() -> call(clientFor(server.getAddress().getPort())))
                .isInstanceOf(AppException.class)
                .extracting(e -> ((AppException) e).getErrorCode())
                .isEqualTo("WALLET_SERVICE_UNAVAILABLE");

        int deadPort = server.getAddress().getPort();
        server.stop(0);
        assertThatThrownBy(() -> call(clientFor(deadPort)))
                .isInstanceOf(AppException.class)
                .as("message thành `detail` gửi người dùng cuối: không được lộ địa chỉ nội bộ")
                .hasMessageNotContaining("localhost")
                .hasMessageNotContaining(String.valueOf(deadPort))
                .extracting(e -> ((AppException) e).getErrorCode())
                .isEqualTo("WALLET_SERVICE_UNAVAILABLE");
    }

    private static HttpWalletDirectory clientFor(int port) {
        return new HttpWalletDirectory(RestClient.builder(), "http://localhost:" + port,
                Duration.ofSeconds(1), Duration.ofSeconds(2));
    }

    private static Optional<WalletInfo> call(HttpWalletDirectory client) {
        return TenantContext.callWith(TENANT, () -> client.find(WALLET));
    }
}
