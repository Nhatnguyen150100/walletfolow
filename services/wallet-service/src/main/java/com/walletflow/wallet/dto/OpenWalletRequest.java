package com.walletflow.wallet.dto;

import com.walletflow.common.money.ECurrency;
import jakarta.validation.constraints.NotNull;

public record OpenWalletRequest(@NotNull(message = "currency là bắt buộc") ECurrency currency) {
}
