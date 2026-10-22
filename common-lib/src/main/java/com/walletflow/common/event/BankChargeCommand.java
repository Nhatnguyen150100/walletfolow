package com.walletflow.common.event;

import com.walletflow.common.money.ECurrency;

import java.util.UUID;

/**
 * Lệnh thu tiền qua cổng thanh toán cho một giao dịch nạp tiền. Topic {@code txn.command.bank-charge.v1},
 * producer transaction-service (saga top-up, 2.4), consumer bank-adapter (ADR-016).
 *
 * <p>Idempotent theo {@code (tenant, transactionId)}: gửi lại bao nhiêu lần cũng chỉ có MỘT phiên thanh
 * toán ở provider, và lần nào cũng nhận lại câu trả lời ứng với trạng thái hiện tại. Gửi lại cùng
 * {@code transactionId} mà khác ví/số tiền/loại tiền là bug phía producer → DLT.
 *
 * @param transactionId giao dịch nạp tiền ở transaction-service — khoá idempotency
 * @param walletId      ví sẽ được cộng tiền sau khi thu xong; cũng là partition key
 * @param amountMinor   số tiền theo đơn vị nhỏ nhất (ADR-005), luôn dương
 * @param currency      loại tiền
 * @param clientIp      IP của người dùng lúc tạo lệnh nạp (tuỳ chọn, thêm ở 2.2b — ADR-017). Cổng thanh toán
 *                      dùng nó để chống gian lận (VNPay bắt buộc {@code vnp_IpAddr}); thiếu thì adapter dùng IP
 *                      máy chủ. KHÔNG thuộc danh tính của lần thu: gửi lại khác IP vẫn là cùng một lần thu.
 *                      Là dữ liệu cá nhân — không log.
 */
public record BankChargeCommand(
    UUID transactionId,
    UUID walletId,
    long amountMinor,
    ECurrency currency,
    String clientIp
) {

    /** Lệnh không mang IP — đúng hình dạng message v1 trước 2.2b. */
    public BankChargeCommand(UUID transactionId, UUID walletId, long amountMinor, ECurrency currency) {
        this(transactionId, walletId, amountMinor, currency, null);
    }
}
