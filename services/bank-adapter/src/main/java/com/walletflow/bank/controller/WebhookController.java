package com.walletflow.bank.controller;

import com.walletflow.bank.dto.VnPayIpnResponse;
import com.walletflow.bank.services.WebhookIngestService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Nhận webhook/IPN của cổng thanh toán (roadmap 2.3, ADR-018). Endpoint CÔNG KHAI — gateway không kiểm
 * JWT cho {@code /webhooks/**}, vì provider không có token của ta.
 *
 * <p>Ranh giới tin cậy duy nhất là chữ ký, kiểm trong {@link WebhookIngestService}. Controller này cố ý
 * mỏng: không đọc nghĩa payload, không chạm ví, không gọi provider.
 *
 * <p><b>Webhook đã qua chữ ký luôn được trả 200, kể cả khi trùng</b> — với provider, "đã nhận" là thông
 * điệp đúng; báo lỗi chỉ khiến nó gửi lại một thứ ta đã có. Chữ ký sai mới là 401.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/webhooks")
public class WebhookController {

    private final WebhookIngestService ingestService;

    /**
     * IPN của VNPay: GET, tham số trên query string, chữ ký ở {@code vnp_SecureHash}.
     *
     * <p>Thân câu trả lời phải đúng khuôn {@code {"RspCode","Message"}} của VNPay — nó đọc mã này để quyết
     * định có gửi lại hay không. Cố ý KHÔNG dùng ProblemDetail ở đây dù phần còn lại của hệ thống dùng
     * RFC 7807: hợp đồng này là của VNPay, không phải của ta.
     */
    @RequestMapping(path = "/vnpay", method = {RequestMethod.GET, RequestMethod.POST})
    public ResponseEntity<VnPayIpnResponse> vnpayIpn(HttpServletRequest request) {
        return switch (ingest("vnpay", request)) {
            // "00" với VNPay nghĩa là đã ghi nhận, đừng gửi lại nữa
            case ACCEPTED, DUPLICATE -> ResponseEntity.ok(VnPayIpnResponse.confirmed());
            case REJECTED -> ResponseEntity.status(401).body(VnPayIpnResponse.invalidSignature());
        };
    }

    /**
     * Webhook của {@code MockProvider} — để test tự động và chaos test không phụ thuộc VNPay sandbox.
     * Nhận cả GET lẫn POST form để mô phỏng được cả hai kiểu provider.
     */
    @RequestMapping(path = "/mock", method = {RequestMethod.GET, RequestMethod.POST})
    public ResponseEntity<Map<String, String>> mockWebhook(HttpServletRequest request) {
        WebhookIngestService.Outcome outcome = ingest("mock", request);
        Map<String, String> body = Map.of("outcome", outcome.name());
        return outcome == WebhookIngestService.Outcome.REJECTED
                ? ResponseEntity.status(401).body(body)
                : ResponseEntity.ok(body);
    }

    /**
     * Lỗi kỹ thuật ngoài dự kiến: trả 500 nhưng vẫn ĐÚNG KHUÔN của VNPay. Nếu để
     * {@code GlobalExceptionHandler} của common-lib trả ProblemDetail thì VNPay không đọc được, và hợp
     * đồng tuyên bố ở {@link VnPayIpnResponse} bị phá đúng lúc hệ thống đang hỏng.
     *
     * <p>500 là đúng phân loại: lỗi kỹ thuật ⇒ provider gửi lại (roadmap §5.7), khác hẳn 401 của chữ ký sai.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<VnPayIpnResponse> onUnexpected(Exception failure) {
        log.error("Bank: lỗi không lường trước khi nhận webhook — trả 500 để provider gửi lại", failure);
        return ResponseEntity.status(500).body(new VnPayIpnResponse("99", "Unknown error"));
    }

    private WebhookIngestService.Outcome ingest(String provider, HttpServletRequest request) {
        Optional<Map<String, String>> params = singleValuedParams(request);
        if (params.isEmpty()) {
            // Tham số lặp tên: provider thật không gửi thế. Chọn bừa một giá trị là mở đường lách chữ ký
            // (ký trên giá trị này, cho ta đọc giá trị kia), nên từ chối thẳng.
            log.debug("Bank: webhook {} có tham số lặp tên — từ chối", provider);
            return WebhookIngestService.Outcome.REJECTED;
        }
        return ingestService.accept(provider, params.get(), rawPayload(request, params.get()));
    }

    /**
     * Bằng chứng đối soát. Với GET thì là nguyên văn query string. Với POST (tham số nằm ở body form,
     * {@code getQueryString()} rỗng) thì dựng lại từ chính tập tham số ĐÃ kiểm chữ ký — nếu không, cột
     * {@code raw_payload} sẽ rỗng và ta lưu một "bằng chứng" không chứng minh được gì.
     */
    private static String rawPayload(HttpServletRequest request, Map<String, String> params) {
        String query = request.getQueryString();
        if (query != null && !query.isBlank()) {
            return query;
        }
        return params.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                        + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
    }

    /** {@code Optional.empty()} nếu có bất kỳ tham số nào xuất hiện nhiều hơn một lần. */
    private static Optional<Map<String, String>> singleValuedParams(HttpServletRequest request) {
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
}
