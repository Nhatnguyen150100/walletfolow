package com.walletflow.wallet.controller;

import com.walletflow.common.exception.AppException;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.response.ResponseBuilder;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.wallet.services.WalletRebuildService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * Thao tác quản trị trên read model. Chỉ đụng tới read model — không có gì ở đây thay đổi được sổ
 * cái, nên chưa cần 4-eyes (roadmap 2.5 áp cho thao tác đụng TIỀN).
 */
@RestController
@RequestMapping("/api/v1/admin/wallets")
@RequiredArgsConstructor
@Tag(name = "Wallet admin", description = "Công cụ khôi phục read model")
public class AdminWalletController {

    private final WalletRebuildService rebuildService;

    @PostMapping("/{walletId}/rebuild")
    @Operation(summary = "Dựng lại số dư + lịch sử của ví từ sổ cái (ledger luôn thắng)")
    public ResponseEntity<BaseResponse<WalletRebuildService.RebuildResult>> rebuild(
            @RequestHeader(value = AuthHeaders.USER_ROLE, required = false) String role,
            @PathVariable UUID walletId) {
        if (!"ADMIN".equals(role)) {
            throw new AppException(HttpStatus.FORBIDDEN, "FORBIDDEN", "Chỉ ADMIN được dựng lại ví");
        }
        return ResponseBuilder.success("Đã dựng lại ví từ sổ cái", rebuildService.rebuild(walletId));
    }
}
