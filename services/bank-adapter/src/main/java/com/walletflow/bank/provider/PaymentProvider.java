package com.walletflow.bank.provider;

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

    /** Tạo phiên thanh toán. Người dùng trả tiền ở {@code paymentUrl}; kết quả hỏi lại bằng {@link #getStatus}. */
    ChargeResult charge(ChargeRequest request);

    /** Trạng thái hiện tại của một phiên, theo lời provider. */
    ProviderStatus getStatus(String providerRef);

    /** Hoàn tiền cho một lần thu đã thành công (bù trừ ở saga top-up, 2.4). */
    RefundResult refund(RefundRequest request);
}
