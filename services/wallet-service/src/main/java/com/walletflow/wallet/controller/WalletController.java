package com.walletflow.wallet.controller;

import com.walletflow.common.exception.UnauthorizedException;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.response.ResponseBuilder;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.wallet.dto.EntryPage;
import com.walletflow.wallet.dto.OpenWalletRequest;
import com.walletflow.wallet.dto.WalletView;
import com.walletflow.wallet.services.WalletService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * API ví của người dùng. Danh tính ({@code X-User-Id}) và tenant ({@code X-Tenant-Id}) do gateway
 * gắn sau khi verify JWT — không có endpoint nào nhận userId từ path hay body, nên không có cách nào
 * hỏi ví của người khác.
 */
@RestController
@RequestMapping("/api/v1/wallets")
@RequiredArgsConstructor
@Tag(name = "Wallet", description = "Ví của người dùng hiện tại")
public class WalletController {

    private final WalletService walletService;

    @PostMapping
    @Operation(summary = "Mở ví cho một loại tiền — idempotent, gọi lại trả về ví đã có")
    public ResponseEntity<BaseResponse<WalletView>> open(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID userId,
            @Valid @RequestBody OpenWalletRequest request) {
        WalletService.OpenResult result = walletService.open(requireUser(userId), request.currency());
        return result.created()
                ? ResponseBuilder.created("Đã mở ví, chờ sổ cái xác nhận", result.wallet())
                : ResponseBuilder.success("Ví đã tồn tại", result.wallet());
    }

    @GetMapping("/me")
    @Operation(summary = "Các ví của người dùng hiện tại")
    public ResponseEntity<BaseResponse<List<WalletView>>> mine(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID userId) {
        return ResponseBuilder.success("OK", walletService.walletsOf(requireUser(userId)));
    }

    @GetMapping("/me/transactions")
    @Operation(summary = "Lịch sử giao dịch theo cursor, mới nhất trước")
    public ResponseEntity<BaseResponse<EntryPage>> history(
            @RequestHeader(value = AuthHeaders.USER_ID, required = false) UUID userId,
            @RequestParam ECurrency currency,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "20") int limit) {
        return ResponseBuilder.success("OK",
                walletService.history(requireUser(userId), currency, cursor, limit));
    }

    /**
     * Thiếu {@code X-User-Id} ở path cần đăng nhập nghĩa là request không đi qua gateway — trả 401,
     * không coi là lỗi định dạng (400).
     */
    private static UUID requireUser(UUID userId) {
        if (userId == null) {
            throw new UnauthorizedException("Thiếu danh tính người dùng");
        }
        return userId;
    }
}
