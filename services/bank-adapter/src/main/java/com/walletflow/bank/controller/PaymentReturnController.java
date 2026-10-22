package com.walletflow.bank.controller;

import com.walletflow.bank.dto.PaymentReturnResponse;
import com.walletflow.bank.services.PaymentReturnService;
import com.walletflow.common.exception.UnauthorizedException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;

/**
 * Trang người dùng quay về sau khi trả tiền ở cổng thanh toán (roadmap 2.3b).
 *
 * <p>Công khai như {@code /webhooks/**} và vì cùng lý do: trình duyệt người dùng tới đây thẳng từ provider,
 * không mang JWT của ta. Ranh giới tin cậy vẫn là chữ ký.
 *
 * <p>Khác {@link WebhookController} ở hai điểm: client là frontend của ta nên lỗi trả RFC 7807
 * ({@code ProblemDetail}) như phần còn lại của hệ thống, và request này KHÔNG được ghi vào
 * {@code webhook_events} — nó không phải thông báo của provider mà chỉ là một lần người dùng mở trang, lặp
 * lại bao nhiêu lần cũng được bằng nút F5.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/payments/return")
public class PaymentReturnController {

    private final PaymentReturnService returnService;

    /**
     * @return trạng thái ĐANG GHI NHẬN của lần thu. {@code PENDING} là câu trả lời bình thường: người dùng
     *         quay về gần như luôn sớm hơn lúc kết quả được chốt.
     */
    @GetMapping("/{provider}")
    public PaymentReturnResponse resolve(@PathVariable String provider, HttpServletRequest request) {
        Optional<Map<String, String>> params = SignedParams.singleValued(request);
        if (params.isEmpty()) {
            // Tham số lặp tên — xem SignedParams: chọn bừa một giá trị là mở đường lách chữ ký
            log.debug("Bank: trang quay về {} có tham số lặp tên — từ chối", provider);
            throw new UnauthorizedException("INVALID_SIGNATURE", "Chữ ký không hợp lệ");
        }
        return returnService.resolve(provider, params.get());
    }
}
