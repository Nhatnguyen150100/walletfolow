package com.walletflow.bank.controller;

import jakarta.servlet.http.HttpServletRequest;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Đọc tham số của một request ĐÃ KÝ do provider gửi tới (IPN ở {@link WebhookController}, trang quay về ở
 * {@link PaymentReturnController}).
 *
 * <p>Dùng chung cố ý: cả hai đường đều lấy chữ ký làm ranh giới tin cậy duy nhất, nên luật đọc tham số phải
 * y hệt nhau. Hai bản sao của đoạn này sẽ lệch nhau, và chỗ lệch chính là một đường lách chữ ký.
 */
final class SignedParams {

    private SignedParams() {
    }

    /**
     * {@code Optional.empty()} nếu có bất kỳ tham số nào xuất hiện nhiều hơn một lần — provider thật không
     * gửi thế. Chọn bừa một giá trị là mở đường lách chữ ký: ký trên giá trị này, cho ta đọc giá trị kia
     * (ADR-018 §1).
     */
    static Optional<Map<String, String>> singleValued(HttpServletRequest request) {
        Map<String, String> params = new LinkedHashMap<>();
        for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
            String[] values = entry.getValue();
            if (values == null || values.length == 0) {
                continue;
            }
            if (values.length > 1) {
                return Optional.empty();
            }
            params.put(entry.getKey(), values[0]);
        }
        return Optional.of(params);
    }

    /**
     * Bằng chứng đối soát. Với GET thì là nguyên văn query string. Với POST (tham số nằm ở body form,
     * {@code getQueryString()} rỗng) thì dựng lại từ chính tập tham số ĐÃ kiểm chữ ký — nếu không, cột
     * {@code raw_payload} sẽ rỗng và ta lưu một "bằng chứng" không chứng minh được gì.
     */
    static String rawPayload(HttpServletRequest request, Map<String, String> params) {
        String query = request.getQueryString();
        if (query != null && !query.isBlank()) {
            return query;
        }
        return params.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                        + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
    }
}
