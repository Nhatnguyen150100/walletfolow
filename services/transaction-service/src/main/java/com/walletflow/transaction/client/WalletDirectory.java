package com.walletflow.transaction.client;

import com.walletflow.common.wallet.WalletInfo;

import java.util.Optional;
import java.util.UUID;

/**
 * Hỏi wallet-service về một ví trong tenant hiện tại. Chỉ để trả lỗi SỚM và RÕ cho người dùng
 * (không phải chủ ví, ví không tồn tại, ví bị đóng băng) — ledger vẫn là nơi cưỡng chế cuối cùng.
 */
public interface WalletDirectory {

    /** @return rỗng nếu không có ví này trong tenant hiện tại */
    Optional<WalletInfo> find(UUID walletId);
}
