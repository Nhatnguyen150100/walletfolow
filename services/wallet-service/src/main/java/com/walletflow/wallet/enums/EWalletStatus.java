package com.walletflow.wallet.enums;

import com.walletflow.common.event.EAccountStatus;

/**
 * Trạng thái ví = trạng thái tài khoản sổ cái, cộng thêm {@link #PENDING}.
 *
 * <p>Ledger là nơi CƯỠNG CHẾ trạng thái (ADR-011); ở đây chỉ phản chiếu để API trả lời được "ví
 * này dùng được chưa" mà không phải hỏi ledger.
 */
public enum EWalletStatus {
    /** Đã gửi lệnh mở tài khoản, ledger chưa xác nhận. Chưa có gì để nhận hay chi. */
    PENDING,
    ACTIVE,
    FROZEN,
    CLOSED;

    public static EWalletStatus from(EAccountStatus status) {
        return switch (status) {
            case ACTIVE -> ACTIVE;
            case FROZEN -> FROZEN;
            case CLOSED -> CLOSED;
        };
    }
}
