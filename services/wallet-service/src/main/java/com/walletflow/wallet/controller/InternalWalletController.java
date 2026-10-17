package com.walletflow.wallet.controller;

import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.WalletInfo;
import com.walletflow.wallet.repositories.WalletRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * API NỘI BỘ của wallet-service — gateway không có route nào tới {@code /internal/**}.
 *
 * <p>transaction-service dùng nó để kiểm ví nguồn thuộc về người gọi và ví đích tồn tại, TRƯỚC khi
 * nhận lệnh. Đây là kiểm tra để trả lỗi sớm và rõ cho người dùng; ledger vẫn là nơi cưỡng chế cuối
 * (tài khoản không tồn tại / bị đóng băng thì bút toán bị từ chối).
 *
 * <p>Tenant từ {@code X-Tenant-Id} qua {@code TenantFilter}; ví của tenant khác là 404.
 */
@RestController
@RequestMapping("/internal/wallets")
@RequiredArgsConstructor
public class InternalWalletController {

    private final WalletRepository walletRepository;

    @GetMapping("/{walletId}")
    @Transactional(readOnly = true)
    public WalletInfo wallet(@PathVariable UUID walletId) {
        return walletRepository.findByTenantIdAndId(TenantContext.require(), walletId)
                .map(w -> new WalletInfo(w.getId(), w.getUserId(), w.getCurrency(), w.getStatus()))
                .orElseThrow(() -> new ResourceNotFoundException("Không có ví " + walletId));
    }
}
