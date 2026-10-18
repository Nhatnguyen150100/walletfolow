package com.walletflow.bank.provider;

import com.walletflow.common.money.ECurrency;

/**
 * @param idempotencyKey khoá gửi kèm cho provider, tất định theo lần thu — gọi lại với cùng khoá là
 *                       nhận lại đúng phiên cũ
 * @param amountMinor    số tiền theo đơn vị nhỏ nhất (ADR-005)
 */
public record ChargeRequest(String idempotencyKey, long amountMinor, ECurrency currency) {
}
