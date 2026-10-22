package com.walletflow.bank.provider;

import com.walletflow.common.money.ECurrency;

import java.time.Duration;
import java.util.Objects;

/**
 * @param idempotencyKey khoá gửi kèm cho provider, tất định theo lần thu — gọi lại với cùng khoá là
 *                       nhận lại đúng phiên cũ
 * @param amountMinor    số tiền theo đơn vị nhỏ nhất (ADR-005)
 * @param clientIp       IP người dùng (tuỳ chọn) — provider dùng để chống gian lận; {@code null} thì provider
 *                       tự dùng IP máy chủ. Dữ liệu cá nhân: không log
 * @param sessionTtl     hạn của phiên thanh toán — sau chừng này provider không nhận tiền cho phiên nữa
 */
public record ChargeRequest(String idempotencyKey, long amountMinor, ECurrency currency, String clientIp,
                            Duration sessionTtl) {

    public ChargeRequest {
        Objects.requireNonNull(sessionTtl, "sessionTtl");
    }
}
