package com.walletflow.common.wallet;

import com.walletflow.common.money.ECurrency;

import java.util.UUID;

/**
 * Thông tin tối thiểu về một ví — hợp đồng của API nội bộ {@code GET /internal/wallets/{id}}
 * (wallet-service → transaction-service).
 *
 * <p>Cố ý KHÔNG có số dư: transaction-service không được quyết "đủ tiền" bằng read model (roadmap
 * 1.2). Chỉ ledger, dưới khoá {@code FOR UPDATE}, mới quyết. Không đưa số dư vào hợp đồng là cách rẻ
 * nhất để không ai bị cám dỗ dùng nó.
 */
public record WalletInfo(
    UUID id,
    UUID userId,
    ECurrency currency,
    EWalletStatus status
) {
}
