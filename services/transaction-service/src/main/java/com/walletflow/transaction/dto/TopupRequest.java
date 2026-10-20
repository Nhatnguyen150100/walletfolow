package com.walletflow.transaction.dto;

import com.walletflow.common.money.ECurrency;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.UUID;

/**
 * Lệnh nạp tiền vào ví. Người nạp là người dùng hiện tại (header {@code X-User-Id} từ gateway); ví
 * đích phải thuộc về họ — kiểm ở {@code TopupService}, không tin body.
 *
 * <p>Không có ví nguồn: tiền đến từ ngân hàng qua cổng thanh toán. Cũng không có {@code note} — lần
 * nạp được đối soát bằng mã tham chiếu của provider, không bằng lời người dùng viết.
 *
 * @param amountMinor số tiền theo đơn vị nhỏ nhất (ADR-005), luôn dương
 */
public record TopupRequest(
        @NotNull(message = "walletId là bắt buộc") UUID walletId,
        @NotNull(message = "amountMinor là bắt buộc") @Positive(message = "amountMinor phải > 0") Long amountMinor,
        @NotNull(message = "currency là bắt buộc") ECurrency currency
) {
}
