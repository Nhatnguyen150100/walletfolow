package com.walletflow.common.event;

import com.walletflow.common.money.ECurrency;

import java.util.UUID;

/**
 * Ledger xác nhận tài khoản của ví đã tồn tại — wallet-service chuyển ví từ {@code PENDING} sang
 * trạng thái mang trong event.
 *
 * <p>Phát lại MỖI lần nhận {@link OpenAccountCommand}, kể cả khi tài khoản đã có từ trước, với
 * {@code eventId} tất định theo {@code (accountId, status)}. Bản phát lại y hệt thì consumer dedup
 * được; trạng thái đã đổi thì là event mới, không bị dedup nuốt. Producer luôn nhận được câu trả
 * lời khi gửi lại lệnh — xem {@code LedgerCommandHandler.openAccount}.
 *
 * @param status trạng thái HIỆN TẠI của tài khoản; mở lại một tài khoản đã bị đóng băng trả về
 *               {@code FROZEN}, không phải {@code ACTIVE}
 */
public record AccountOpenedEvent(
    UUID accountId,
    String accountRef,
    ECurrency currency,
    EAccountStatus status
) {
}
