package com.walletflow.common.event;

import com.walletflow.common.money.ECurrency;

/**
 * Lệnh mở tài khoản sổ cái cho một ví, do wallet-service phát tới ledger (roadmap 1.0b).
 *
 * <p>Ledger KHÔNG tự tạo tài khoản khi gặp một ref lạ trong bút toán: nếu làm vậy, một lệnh chuyển
 * tới ví gõ sai mã sẽ "thành công" vào một tài khoản ma và tiền mất mà không ai biết. Tài khoản chỉ
 * ra đời qua lệnh này.
 *
 * <p>Tenant đi trong envelope, không nằm ở đây — giống mọi message khác.
 *
 * @param accountRef id của ví, dạng UUID chuẩn (chữ thường). Ledger từ chối mọi ref khác: UUID
 *                   không bao giờ va với tên tài khoản hệ thống ({@code EXTERNAL_BANK}...) và không
 *                   chứa thông tin cá nhân (roadmap 5.4)
 * @param currency   loại tiền của tài khoản
 */
public record OpenAccountCommand(
    String accountRef,
    ECurrency currency
) {
}
