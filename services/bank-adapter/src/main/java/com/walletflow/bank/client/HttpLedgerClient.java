package com.walletflow.bank.client;

import com.walletflow.common.ledger.ExternalMovement;
import com.walletflow.common.ledger.ExternalMovementsRequest;
import com.walletflow.common.ledger.TopupEntryRef;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Gọi hai endpoint ĐỌC {@code /internal/entries/*} của ledger (roadmap 3.1 PR2a, ADR-024).
 *
 * <p>Timeout ngắn và tường minh: mỗi lời gọi chạy trên thread của bộ lập lịch, mà bộ đó dùng chung với
 * job hỏi lại provider và outbox relay ({@code spring.task.scheduling.pool.size}). Ledger treo không
 * timeout là một thread bị giữ vô hạn.
 *
 * <p>Tenant đi theo header {@code X-Tenant-Id}, lấy từ {@code TenantContext} — không nhận qua tham số,
 * cùng quy ước với {@code HttpLedgerClient} của wallet-service.
 */
@Slf4j
@Component
public class HttpLedgerClient implements LedgerClient {

    private static final ParameterizedTypeReference<List<ExternalMovement>> MOVEMENT_LIST =
            new ParameterizedTypeReference<>() {
            };
    private static final ParameterizedTypeReference<List<TopupEntryRef>> TOPUP_LIST =
            new ParameterizedTypeReference<>() {
            };

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
    public Map<UUID, Map<ECurrency, Long>> externalMovementsOf(Collection<UUID> transactionIds) {
        if (transactionIds.isEmpty()) {
            return Map.of();
        }
        String tenantId = TenantContext.require().toString();
        try {
            List<ExternalMovement> movements = restClient.post()
                    .uri("/internal/entries/external-movements")
                    .header(AuthHeaders.TENANT_ID, tenantId)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new ExternalMovementsRequest(List.copyOf(transactionIds)))
                    .exchange((request, response) -> {
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("Ledger trả {} cho lô {} lệnh tiền", response.getStatusCode(),
                                    transactionIds.size());
                            throw new LedgerUnavailableException();
                        }
                        return response.bodyTo(MOVEMENT_LIST);
                    });
            Map<UUID, Map<ECurrency, Long>> result = new LinkedHashMap<>();
            if (movements != null) {
                // Ledger GROUP BY (transaction_id, currency) nên không có cặp trùng; merge vẫn cộng dồn
                // để một thay đổi phía ledger phá giả định đó không làm MẤT tiền khỏi phép so. addExact
                // (ADR-005): tràn số thì ném, không lặng lẽ ra một con số sai dấu.
                movements.forEach(m -> result.computeIfAbsent(m.transactionId(), id -> new LinkedHashMap<>())
                        .merge(m.currency(), m.netInMinor(), Math::addExact));
            }
            return result;
        } catch (ResourceAccessException unreachable) {
            log.warn("Không kết nối được ledger khi hỏi {} lệnh tiền", transactionIds.size(), unreachable);
            throw new LedgerUnavailableException();
        }
    }

    @Override
    public List<TopupEntryRef> topupsAfter(Instant afterPostedAt, UUID afterId, Instant before, int limit) {
        try {
            List<TopupEntryRef> page = restClient.get()
                    .uri(builder -> topupsUri(builder, afterPostedAt, afterId, before, limit))
                    .exchange((request, response) -> {
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("Ledger trả {} cho trang bút toán nạp", response.getStatusCode());
                            throw new LedgerUnavailableException();
                        }
                        return response.bodyTo(TOPUP_LIST);
                    });
            if (page == null) {
                // Thân rỗng KHÔNG phải "hết trang": coi nó là trang rỗng thì chiều ngược dừng như đã quét
                // xong và lượt được ghi SUCCEEDED trong khi phần còn lại chưa ai soi.
                log.warn("Ledger trả 2xx nhưng không có thân cho trang bút toán nạp");
                throw new LedgerUnavailableException();
            }
            return page;
        } catch (ResourceAccessException unreachable) {
            log.warn("Không kết nối được ledger khi lấy trang bút toán nạp", unreachable);
            throw new LedgerUnavailableException();
        }
    }

    /** Con trỏ rỗng ở trang đầu thì KHÔNG gửi tham số, để ledger tự hiểu là "từ đầu". */
    private static URI topupsUri(UriBuilder builder, Instant afterPostedAt, UUID afterId,
                                 Instant before, int limit) {
        builder.path("/internal/entries/topups")
                .queryParam("before", before)
                .queryParam("limit", limit);
        if (afterPostedAt != null && afterId != null) {
            builder.queryParam("afterPostedAt", afterPostedAt).queryParam("afterId", afterId);
        }
        return builder.build();
    }
}
