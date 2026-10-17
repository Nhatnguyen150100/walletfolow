package com.walletflow.transaction.client;

import com.walletflow.common.exception.AppException;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.WalletInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Gọi {@code GET /internal/wallets/{id}} của wallet-service.
 *
 * <p>Gọi TRƯỚC transaction nhận lệnh, không bao giờ bên trong: giữ một kết nối DB và khoá khoá chính
 * idempotency trong lúc chờ HTTP là cách nhanh nhất làm cạn pool khi wallet-service chậm. Timeout
 * ngắn vì người dùng đang chờ phản hồi đồng bộ.
 */
@Slf4j
@Component
public class HttpWalletDirectory implements WalletDirectory {

    private final RestClient restClient;

    public HttpWalletDirectory(
            RestClient.Builder builder,
            @Value("${walletflow.wallet.base-url}") String baseUrl,
            @Value("${walletflow.wallet.connect-timeout:1s}") Duration connectTimeout,
            @Value("${walletflow.wallet.read-timeout:2s}") Duration readTimeout) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);
        this.restClient = builder.baseUrl(baseUrl).requestFactory(factory).build();
    }

    @Override
    public Optional<WalletInfo> find(UUID walletId) {
        String tenantId = TenantContext.require().toString();
        try {
            return restClient.get()
                    .uri("/internal/wallets/{id}", walletId)
                    .header(AuthHeaders.TENANT_ID, tenantId)
                    .exchange((request, response) -> {
                        if (response.getStatusCode().value() == HttpStatus.NOT_FOUND.value()) {
                            return Optional.empty();
                        }
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("wallet-service trả {} khi tra ví {}", response.getStatusCode(), walletId);
                            throw unavailable();
                        }
                        return Optional.ofNullable(response.bodyTo(WalletInfo.class));
                    });
        } catch (ResourceAccessException unreachable) {
            log.warn("Không kết nối được wallet-service khi tra ví {}", walletId, unreachable);
            throw unavailable();
        }
    }

    /**
     * 503 chứ không phải 500: lệnh CHƯA được nhận (không có hàng idempotency nào được ghi), nên gửi
     * lại cùng Idempotency-Key sau đó là an toàn và đúng.
     *
     * <p>Thông điệp CỐ ĐỊNH: chi tiết kỹ thuật (URL nội bộ, tên host, lý do kết nối hỏng) chỉ vào log —
     * message của exception này thành {@code detail} của câu trả lời cho người dùng cuối.
     */
    private static AppException unavailable() {
        return new AppException(HttpStatus.SERVICE_UNAVAILABLE, "WALLET_SERVICE_UNAVAILABLE",
                "Tạm thời không kiểm tra được ví, vui lòng thử lại với cùng Idempotency-Key");
    }
}
