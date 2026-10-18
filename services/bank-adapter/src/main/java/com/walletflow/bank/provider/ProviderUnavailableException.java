package com.walletflow.bank.provider;

/**
 * Lỗi KỸ THUẬT khi nói chuyện với provider: timeout, 5xx, mất kết nối, circuit đang mở, bulkhead đầy.
 *
 * <p>Nghĩa của nó là "không biết", không phải "thất bại" — provider có thể đã nhận lệnh. Không bao giờ đổi
 * nó thành event {@code bank.event.failed}; để nó nổ ra cho Kafka thử lại rồi vào DLT (roadmap §5.7).
 * Cố ý KHÔNG kế thừa {@code AppException}: listener coi {@code AppException} là lệnh hỏng (vào DLT ngay).
 */
public class ProviderUnavailableException extends RuntimeException {

    public ProviderUnavailableException(String message) {
        super(message);
    }

    public ProviderUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
