package com.walletflow.bank.provider;

import com.walletflow.common.money.ECurrency;

/**
 * @param idempotencyKey khoá tất định theo lần hoàn — gọi lại là nhận lại đúng lần hoàn cũ
 * @param providerRef    mã phía provider của lần THU cần hoàn
 */
public record RefundRequest(String idempotencyKey, String providerRef, long amountMinor, ECurrency currency) {
}
