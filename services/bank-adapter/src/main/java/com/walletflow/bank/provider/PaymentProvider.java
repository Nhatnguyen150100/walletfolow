package com.walletflow.bank.provider;

import java.time.Instant;

/**
 * Một cổng thanh toán (roadmap 2.2, ADR-016). Mỗi provider một implementation; phần còn lại của service
 * không biết đang nói chuyện với ai.
 *
 * <p><b>Hợp đồng phân loại lỗi — bắt buộc với mọi implementation:</b>
 * <ul>
 *   <li>Câu trả lời DỨT KHOÁT của provider (từ chối, phiên hết hạn) là GIÁ TRỊ trả về
 *       ({@link ChargeResult.Declined}, {@link ProviderStatus} {@code FAILED}...).</li>
 *   <li>Không biết chuyện gì đã xảy ra (timeout, 5xx, mất kết nối, trả lời không đọc được) là
 *       {@link ProviderUnavailableException}. Timeout KHÔNG có nghĩa là provider chưa nhận lệnh.</li>
 * </ul>
 * Lẫn hai loại này là cách mất tiền phổ biến nhất (roadmap §5.7): coi một timeout là "bị từ chối" thì
 * người dùng đã trả tiền mà ví không bao giờ được cộng.
 *
 * <p><b>Idempotency:</b> {@link #charge} và {@link #refund} gọi lại với CÙNG idempotency key phải trả về
 * đúng phiên/lần hoàn đã có, không tạo cái thứ hai. Đây là thứ cho phép thử lại sau một timeout mơ hồ.
 *
 * <p><b>Timeout của chính HTTP client</b> (connect/read) phải NGẮN hơn
 * {@code walletflow.bank.resilience.timeout}. Timeout của {@link ResilientPaymentProvider} chỉ thả người
 * gọi ra, không ngắt được lời gọi đang treo — client không tự timeout thì mỗi lời gọi treo giữ chết một
 * thread của bulkhead.
 */
public interface PaymentProvider {

    /** Tên ổn định, lưu vào DB và event ({@code mock}, {@code vnpay}...). */
    String name();

    /**
     * Tài khoản merchant mà instance này dùng (VNPay: {@code vnp_TmnCode}). Lưu cùng mỗi lần thu: phiên chỉ tra
     * được dưới đúng tài khoản đã tạo nó, nên đổi tài khoản khi còn lần thu dở dang thì KHÔNG được hỏi tiếp —
     * "không tìm thấy" từ tài khoản mới sẽ biến lần thu đã trả thành hết hạn (ADR-017).
     */
    String account();

    /**
     * Tạo phiên thanh toán. Người dùng trả tiền ở {@code paymentUrl}; kết quả hỏi lại bằng {@link #getStatus}.
     *
     * <p>Chỉ gọi cho lần thu còn {@code NEW}. Mã tham chiếu phải tất định theo khoá, nhưng các field khác của
     * {@link ChargeResult.Created} thì không nhất thiết (VNPay ký URL tại chỗ với {@code vnp_CreateDate} = lúc
     * gọi). Bất biến đứng được vì {@code ChargeService} chỉ ghi câu trả lời của luồng thắng {@code NEW -> PENDING}
     * và luôn phát URL đọc lại từ DB.
     */
    ChargeResult charge(ChargeRequest request);

    /**
     * {@code false} nếu {@link #charge} không gọi mạng (VNPay chỉ ký URL tại chỗ). Khi đó lớp bọc resilience không
     * cho nó đi qua circuit breaker: hỏi trạng thái lỗi liên tục không được chặn luôn việc tạo URL nạp tiền.
     */
    default boolean chargeUsesNetwork() {
        return true;
    }

    /**
     * Trạng thái hiện tại của một phiên, theo lời provider.
     *
     * @param sessionCreatedAt đúng giá trị {@link ChargeResult.Created#sessionCreatedAt()} lúc tạo phiên —
     *                         có provider (VNPay) chỉ tìm được giao dịch theo cặp mã + ngày tạo; {@code null} với
     *                         hàng tạo trước 2.2b
     */
    ProviderStatus getStatus(String providerRef, Instant sessionCreatedAt);

    /** Hoàn tiền cho một lần thu đã thành công (bù trừ ở saga top-up, 2.4). */
    RefundResult refund(RefundRequest request);
}
