package com.walletflow.transaction.client;

import com.walletflow.common.bank.ProviderContext;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Gọi {@code GET /internal/provider-transactions?transactionId=...} của bank-adapter.
 *
 * <p><b>Không bao giờ ném</b> — xem {@link HttpLedgerEntriesClient} về lý do màn hình của người trực
 * phải suy giảm mềm thay vì hỏng cả trang.
 */
@Slf4j
@Component
public class HttpProviderContextClient implements ProviderContextClient {

    private final RestClient restClient;

    public HttpProviderContextClient(
            RestClient.Builder builder,
            @Value("${walletflow.bank.base-url}") String baseUrl,
            @Value("${walletflow.bank.connect-timeout:1s}") Duration connectTimeout,
            @Value("${walletflow.bank.read-timeout:2s}") Duration readTimeout) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);
        this.restClient = builder.baseUrl(baseUrl).requestFactory(factory).build();
    }

    @Override
    public Optional<ProviderContext> contextOf(UUID transactionId) {
        String tenantId = TenantContext.require().toString();
        try {
            return restClient.get()
                    .uri(uri -> uri.path("/internal/provider-transactions")
                            .queryParam("transactionId", transactionId).build())
                    .header(AuthHeaders.TENANT_ID, tenantId)
                    .exchange((request, response) -> {
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("bank-adapter trả {} khi tra ngữ cảnh provider của lệnh {}",
                                    response.getStatusCode(), transactionId);
                            return Optional.<ProviderContext>empty();
                        }
                        return Optional.ofNullable(response.bodyTo(ProviderContext.class));
                    });
        } catch (RuntimeException failure) {
            // Xem HttpLedgerEntriesClient về lý do bắt rộng tới RuntimeException.
            log.warn("Không hỏi được bank-adapter về ngữ cảnh provider của lệnh {}", transactionId, failure);
            return Optional.empty();
        }
    }
}
