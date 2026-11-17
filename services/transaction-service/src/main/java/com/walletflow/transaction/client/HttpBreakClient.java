package com.walletflow.transaction.client;

import com.walletflow.common.reconciliation.CloseBreakRequest;
import com.walletflow.common.reconciliation.ReconciliationBreakView;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Gọi {@code /internal/reconciliation/breaks} của ledger-service, wallet-service và bank-adapter
 * (roadmap 3.3, ADR-025 §1).
 *
 * <p><b>Đường ĐỌC không bao giờ ném</b> — hỏng thì {@code Optional.empty()}, cùng lý lẽ với
 * {@code HttpLedgerEntriesClient}: màn hình của người trực phải mở được ngay cả khi một service đang
 * chết, vì đó chính là lúc cần nó nhất. Một ô khuyết có ích hơn một trang lỗi.
 *
 * <p><b>Đường ĐÓNG thì phân biệt ba kết cục</b> thay vì nuốt tất: "đã đóng", "không có break đó", và
 * "không hỏi được" dẫn tới ba hành động khác nhau của người trực, nên không được trộn thành một.
 *
 * <p>Mọi lời gọi nằm NGOÀI transaction DB (CLAUDE.md).
 */
@Slf4j
@Component
public class HttpBreakClient implements BreakClient {

    private static final ParameterizedTypeReference<List<ReconciliationBreakView>> BREAK_LIST =
            new ParameterizedTypeReference<>() {
            };

    private final Map<EBreakSource, RestClient> clients = new EnumMap<>(EBreakSource.class);

    public HttpBreakClient(
            RestClient.Builder builder,
            @Value("${walletflow.ledger.base-url}") String ledgerUrl,
            @Value("${walletflow.wallet.base-url}") String walletUrl,
            @Value("${walletflow.bank.base-url}") String bankUrl,
            @Value("${walletflow.admin.breaks.connect-timeout:1s}") Duration connectTimeout,
            @Value("${walletflow.admin.breaks.read-timeout:2s}") Duration readTimeout) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);
        clients.put(EBreakSource.LEDGER, builder.clone().baseUrl(ledgerUrl).requestFactory(factory).build());
        clients.put(EBreakSource.WALLET, builder.clone().baseUrl(walletUrl).requestFactory(factory).build());
        clients.put(EBreakSource.BANK, builder.clone().baseUrl(bankUrl).requestFactory(factory).build());
    }

    @Override
    public Optional<List<ReconciliationBreakView>> openBreaks(EBreakSource source, int limit) {
        String tenantId = TenantContext.require().toString();
        try {
            return clients.get(source).get()
                    .uri(uri -> uri.path("/internal/reconciliation/breaks")
                            .queryParam("limit", limit).build())
                    .header(AuthHeaders.TENANT_ID, tenantId)
                    .exchange((request, response) -> {
                        if (!response.getStatusCode().is2xxSuccessful()) {
                            log.warn("{} trả {} khi đọc hàng đợi chỗ lệch",
                                    source.slug(), response.getStatusCode());
                            return Optional.<List<ReconciliationBreakView>>empty();
                        }
                        return Optional.ofNullable(response.bodyTo(BREAK_LIST));
                    });
        } catch (RuntimeException failure) {
            // Bắt RuntimeException chứ không chỉ RestClientException — "không bao giờ ném" là lời hứa
            // của method này và không có kiểu dữ liệu nào bảo đảm nó (xem HttpLedgerEntriesClient).
            log.warn("Không hỏi được {} về hàng đợi chỗ lệch", source.slug(), failure);
            return Optional.empty();
        }
    }

    @Override
    public CloseOutcome close(EBreakSource source, UUID breakId, UUID closedBy, String note) {
        String tenantId = TenantContext.require().toString();
        try {
            return clients.get(source).post()
                    .uri("/internal/reconciliation/breaks/{id}/close", breakId)
                    .header(AuthHeaders.TENANT_ID, tenantId)
                    .header(AuthHeaders.USER_ID, closedBy.toString())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new CloseBreakRequest(note))
                    .exchange((request, response) -> {
                        if (response.getStatusCode().is2xxSuccessful()) {
                            return CloseOutcome.CLOSED;
                        }
                        if (response.getStatusCode().value() == HttpStatus.NOT_FOUND.value()) {
                            return CloseOutcome.NOT_FOUND;
                        }
                        if (response.getStatusCode().is4xxClientError()) {
                            // 400 từ service sở hữu (lý do rỗng/quá dài) rơi vào đây. Người trực đã
                            // qua kiểm ở tầng này rồi, nên tới đây mà 4xx là LỆCH HỢP ĐỒNG giữa hai
                            // service — một sự cố cấu hình. Nhưng nó vẫn là một lời từ chối DỨT
                            // KHOÁT: chắc chắn chưa đóng gì, khác hẳn "không hỏi được".
                            log.error("{} TỪ CHỐI lệnh đóng chỗ lệch {} với {} — lệch hợp đồng",
                                    source.slug(), breakId, response.getStatusCode());
                            return CloseOutcome.REJECTED;
                        }
                        log.error("{} trả {} khi đóng chỗ lệch {}",
                                source.slug(), response.getStatusCode(), breakId);
                        return CloseOutcome.UNAVAILABLE;
                    });
        } catch (RuntimeException failure) {
            log.error("Không gọi được {} để đóng chỗ lệch {}", source.slug(), breakId, failure);
            return CloseOutcome.UNAVAILABLE;
        }
    }
}
