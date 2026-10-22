package com.walletflow.bank.enums;

/**
 * Vì sao một webhook đã nhận ngừng được xử lý (roadmap 2.3b, V5). Chỉ để vận hành đọc — không luồng nghiệp
 * vụ nào rẽ theo giá trị này, và không bao giờ là căn cứ ghi tiền.
 */
public enum EWebhookOutcome {

    /** Đã tra ra lần thu và hỏi lại provider xong. Kết quả (nếu có) do {@code ChargeStatusService} ghi. */
    RESOLVED,

    /**
     * Chữ ký hợp lệ nhưng mã tham chiếu không thuộc hệ thống này — hoặc payload không mang mã nào.
     *
     * <p>Không phải ca đua: hàng {@code provider_transactions} luôn tồn tại TRƯỚC khi người dùng được gửi
     * sang provider, nên provider không thể biết một mã mà ta chưa ghi. Thực tế đây là webhook của môi
     * trường khác dùng chung tài khoản merchant, hoặc của người đang gõ cửa. Đóng hàng lại thay vì hỏi
     * mãi: nó sẽ không bao giờ tra ra.
     */
    UNRESOLVED_REF,

    /**
     * Lần thu tra ra được, nhưng nó thuộc TÀI KHOẢN MERCHANT khác tài khoản đang chạy (sau khi xoay
     * {@code VNPAY_TMN_CODE}). Chỉ tài khoản đã tạo phiên mới tra được phiên đó, nên hỏi bằng tài khoản mới
     * là vô nghĩa — và sẽ luôn vô nghĩa, dù thử lại bao nhiêu lần.
     *
     * <p>Đóng ngay thay vì để nó thử đủ lượt rồi vào {@code ABANDONED}: đó là lỗi NGHIỆP VỤ, không phải lỗi
     * kỹ thuật (roadmap §5.7), và alert {@code BankWebhookAbandoned} nói "provider không trả lời" sẽ dẫn
     * người trực đi truy một sự cố provider không tồn tại.
     */
    FOREIGN_ACCOUNT,

    /**
     * Hết trần số lần thử mà vẫn chưa hỏi được provider. Thôi hỏi và cộng counter
     * {@code walletflow.bank.webhook.abandoned} — ca cần người đối soát, chứ không phải bị thử lại mãi mãi
     * mà không ai biết (cùng lý lẽ với {@code expired_unconfirmed} ở ADR-017).
     */
    ABANDONED
}
