package com.walletflow.transaction.dto;

import com.walletflow.common.money.ECurrency;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.UUID;

/**
 * Lệnh chuyển tiền. Người gửi là người dùng hiện tại (header {@code X-User-Id} từ gateway); ví nguồn
 * phải thuộc về họ — kiểm ở {@code TransferService}, không tin body.
 *
 * @param amountMinor số tiền theo đơn vị nhỏ nhất (ADR-005), luôn dương — chiều tiền do ví nguồn/đích
 *                    quyết, không do dấu
 */
public record TransferRequest(
        @NotNull(message = "fromWalletId là bắt buộc") UUID fromWalletId,
        @NotNull(message = "toWalletId là bắt buộc") UUID toWalletId,
        @NotNull(message = "amountMinor là bắt buộc") @Positive(message = "amountMinor phải > 0") Long amountMinor,
        @NotNull(message = "currency là bắt buộc") ECurrency currency,
        @Size(max = 140, message = "note tối đa 140 ký tự") String note
) {
}
