package com.walletflow.common.ledger;

import java.util.UUID;

/**
 * Một vế nợ/có của bút toán.
 *
 * @param accountRef  mã tài khoản người đọc được ({@code WALLET-...}, {@code EXTERNAL_BANK}...).
 *                    {@code accountId} một mình là một UUID không nói lên điều gì lúc 2 giờ sáng;
 *                    giữ cả hai vì một cái để người đọc, một cái để tra tiếp bằng máy.
 * @param amountMinor mang dấu: âm = ghi nợ, dương = ghi có (ADR-005)
 */
public record EntryLine(UUID accountId, String accountRef, long amountMinor) {
}
