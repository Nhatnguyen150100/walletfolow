package com.walletflow.wallet.dto;

import com.walletflow.common.money.ECurrency;
import com.walletflow.wallet.entity.Wallet;
import com.walletflow.wallet.enums.EWalletStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Ví như người dùng nhìn thấy. {@code balanceMinor} là READ MODEL — có thể trễ vài giây so với sổ
 * cái, và KHÔNG được dùng để quyết định "đủ tiền" (roadmap 1.2: chỉ ledger quyết).
 */
public record WalletView(
        UUID id,
        ECurrency currency,
        long balanceMinor,
        EWalletStatus status,
        Instant updatedAt
) {
    public static WalletView of(Wallet wallet) {
        return new WalletView(wallet.getId(), wallet.getCurrency(), wallet.getBalanceMinor(),
                wallet.getStatus(), wallet.getUpdatedAt());
    }
}
