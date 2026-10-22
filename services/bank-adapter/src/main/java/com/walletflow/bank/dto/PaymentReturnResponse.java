package com.walletflow.bank.dto;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderTxnStatus;

import java.util.UUID;

/**
 * Trạng thái một lần thu, trả cho trang người dùng quay về sau khi trả tiền (roadmap 2.3b).
 *
 * <p><b>Đây là trạng thái adapter ĐANG GHI NHẬN, không phải kết quả đọc từ tham số redirect.</b> Người dùng
 * quay về gần như luôn sớm hơn lúc kết quả được chốt, nên {@code PENDING} là câu trả lời bình thường và
 * đúng — màn hình phải hiện "đang xử lý" rồi hỏi lại, chứ không được hiểu thành thất bại.
 *
 * @param transactionId giao dịch bên transaction-service — để client đối chiếu với lệnh nó đã gửi
 * @param status        trạng thái lần thu trong adapter
 * @param failureCode   lý do thất bại, {@code null} nếu chưa/không thất bại
 * @param settled       đã chốt chưa; {@code false} nghĩa là client nên hỏi lại sau
 */
public record PaymentReturnResponse(UUID transactionId,
                                    EProviderTxnStatus status,
                                    String failureCode,
                                    boolean settled) {

    public static PaymentReturnResponse of(ProviderTransaction txn) {
        EProviderTxnStatus status = txn.getStatus();
        boolean settled = status == EProviderTxnStatus.SUCCEEDED || status == EProviderTxnStatus.FAILED;
        return new PaymentReturnResponse(txn.getTransactionId(), status, txn.getFailureCode(), settled);
    }
}
