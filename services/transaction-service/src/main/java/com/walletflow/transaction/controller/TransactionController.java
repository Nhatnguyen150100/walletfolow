package com.walletflow.transaction.controller;

import com.walletflow.common.exception.UnauthorizedException;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.response.ResponseBuilder;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.transaction.dto.TransactionView;
import com.walletflow.transaction.dto.TransferRequest;
import com.walletflow.transaction.services.TransferService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/transactions")
@RequiredArgsConstructor
@Tag(name = "Transactions", description = "Lệnh chuyển tiền")
public class TransactionController {

    /** Header chuẩn (IETF draft "The Idempotency-Key HTTP Header Field"). */
    public static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    /** Đánh dấu câu trả lời là bản phát lại — client phân biệt được "vừa nhận" với "đã nhận từ trước". */
    public static final String REPLAYED = "Idempotent-Replayed";

    private final TransferService transferService;

    /**
     * Trả {@code 202 Accepted}: lệnh đã được NHẬN và sẽ được ghi sổ bất đồng bộ. Gửi lại cùng
     * {@code Idempotency-Key} trả về đúng câu trả lời cũ, không tạo lệnh thứ hai.
     *
     * <p>Body trả nguyên văn chuỗi JSON đã lưu (không serialize lại) để lần gửi lại khớp từng byte.
     */
    @PostMapping("/transfer")
    @Operation(summary = "Chuyển tiền giữa hai ví — bắt buộc header Idempotency-Key")
    public ResponseEntity<String> transfer(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID userId,
            @RequestHeader(value = IDEMPOTENCY_KEY, required = false) String idempotencyKey,
            @Valid @RequestBody TransferRequest request) {
        TransferService.AcceptResult result =
                transferService.accept(requireUser(userId), idempotencyKey, request);

        return ResponseEntity.status(result.status())
                .location(URI.create("/api/v1/transactions/" + result.transactionId()))
                .header(REPLAYED, String.valueOf(result.replayed()))
                .contentType(MediaType.APPLICATION_JSON)
                .body(result.body());
    }

    @GetMapping("/{transactionId}")
    @Operation(summary = "Trạng thái một lệnh chuyển tiền của người dùng hiện tại")
    public ResponseEntity<BaseResponse<TransactionView>> get(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID userId,
            @PathVariable UUID transactionId) {
        return ResponseBuilder.success("OK", transferService.get(requireUser(userId), transactionId));
    }

    private static UUID requireUser(UUID userId) {
        if (userId == null) {
            throw new UnauthorizedException("Thiếu danh tính người dùng");
        }
        return userId;
    }
}
