package com.walletflow.wallet.client;

import com.walletflow.common.ledger.AccountStatement;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Optional;

/**
 * Gọi {@code GET /internal/accounts/{ref}} của ledger.
 *
 * <p><b>Timeout ngắn là bắt buộc:</b> lời gọi này nằm TRONG transaction đang giữ khoá hàng ví (xem
 * {@code WalletRebuildService}). Ledger treo mà không có timeout nghĩa là ví bị khoá vô thời hạn, và
 * mọi event của ví đó xếp hàng sau — tức là số dư ngừng cập nhật.
 *
 * <p>Tenant đi theo header {@code X-Tenant-Id}, lấy từ {@code TenantContext} — không bao giờ nhận
 * qua tham số, để không có chỗ nào truyền nhầm tenant của request khác.
 */
@Slf4j
@Component
public class HttpLedgerClient implements LedgerClient {

    private final RestClient restClient;

    public HttpLedgerClient(
            RestClient.Builder builder,
            @Value("${walletflow.ledger.base-url}") String baseUrl,
            @Value("${walletflow.ledger.connect-timeout:2s}") Duration connectTimeout,
            @Value("${walletflow.ledger.read-timeout:5s}") Duration readTimeout) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);
        this.restClient = builder.baseUrl(baseUrl).requestFactory(factory).build();
    }

    @Override
    public Optional<AccountStatement> statementOf(String accountRef, ECurrency currency) {
        String tenantId = TenantContext.require().toString();
        try {
            return restClient.get()
                    .uri("/internal/accounts/{ref}?currency={currency}", accountRef, currency)
                    .header(AuthHeaders.TENANT_ID, tenantId)
                    .exchange((request, response) -> {
                        if (response.getStatusCode().value() == HttpStatus.NOT_FOUND.value()) {
                            return Optional.empty();
                        }
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("Ledger trả {} cho sao kê {}", response.getStatusCode(), accountRef);
                            throw new LedgerUnavailableException();
                        }
                        return Optional.ofNullable(response.bodyTo(AccountStatement.class));
                    });
        } catch (ResourceAccessException unreachable) {
            // Chi tiết (URL nội bộ, lý do kết nối hỏng) chỉ vào log — message của exception thành
            // `detail` của câu trả lời HTTP.
            log.warn("Không kết nối được ledger khi lấy sao kê {}", accountRef, unreachable);
            throw new LedgerUnavailableException();
        }
    }
}
