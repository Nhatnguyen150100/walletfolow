package com.walletflow.transaction.client;

import com.walletflow.common.ledger.TransactionEntries;
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
 * Gọi {@code GET /internal/entries?transactionId=...} của ledger-service.
 *
 * <p><b>Không bao giờ ném — hỏng thì trả {@code Optional.empty()}.</b> Ngược hẳn với
 * {@link HttpWalletDirectory}, và đó là chủ ý: ở đó lời gọi quyết định một lệnh tiền có được nhận
 * hay không, nên hỏng phải dừng lại. Ở đây nó chỉ làm đầy một màn hình cho người đang xử lý sự cố —
 * mà sự cố diện rộng chính là lúc ledger dễ chậm nhất. Làm hỏng cả hàng đợi can thiệp vì không lấy
 * được một ô phụ là tước đi công cụ chữa cháy đúng lúc cần nó nhất.
 *
 * <p>Gọi NGOÀI mọi transaction DB (CLAUDE.md): {@code AdminReviewService} cố ý không có
 * {@code @Transactional}.
 */
@Slf4j
@Component
public class HttpLedgerEntriesClient implements LedgerEntriesClient {

    private final RestClient restClient;

    public HttpLedgerEntriesClient(
            RestClient.Builder builder,
            @Value("${walletflow.ledger.base-url}") String baseUrl,
            @Value("${walletflow.ledger.connect-timeout:1s}") Duration connectTimeout,
            @Value("${walletflow.ledger.read-timeout:2s}") Duration readTimeout) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);
        this.restClient = builder.baseUrl(baseUrl).requestFactory(factory).build();
    }

    @Override
    public Optional<TransactionEntries> entriesOf(UUID transactionId) {
        String tenantId = TenantContext.require().toString();
        try {
            return restClient.get()
                    .uri(uri -> uri.path("/internal/entries").queryParam("transactionId", transactionId).build())
                    .header(AuthHeaders.TENANT_ID, tenantId)
                    .exchange((request, response) -> {
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("ledger-service trả {} khi tra bút toán của lệnh {}",
                                    response.getStatusCode(), transactionId);
                            return Optional.<TransactionEntries>empty();
                        }
                        return Optional.ofNullable(response.bodyTo(TransactionEntries.class));
                    });
        } catch (RuntimeException failure) {
            // Bắt RuntimeException chứ không chỉ RestClientException: "không bao giờ ném" là lời hứa
            // quan trọng nhất của class này, và không có kiểu dữ liệu nào bảo đảm nó. Một lỗi dựng URI
            // hay một lỗi lúc đóng response cũng đủ làm hỏng cả màn hình chữa cháy — đúng thứ ADR-021
            // B2 muốn tránh.
            log.warn("Không hỏi được ledger-service về bút toán của lệnh {}", transactionId, failure);
            return Optional.empty();
        }
    }
}
