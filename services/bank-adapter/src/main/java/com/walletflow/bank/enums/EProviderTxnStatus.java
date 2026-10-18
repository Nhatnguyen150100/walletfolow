package com.walletflow.bank.enums;

/**
 * Trạng thái một thao tác với provider, như bank-adapter biết (ADR-016).
 *
 * <pre>
 *   NEW --(provider tạo phiên)--> PENDING --(getStatus xác nhận)--> SUCCEEDED | FAILED
 *   NEW --(provider từ chối ngay)--> FAILED
 * </pre>
 */
public enum EProviderTxnStatus {
    /**
     * Đã giành quyền xử lý nhưng CHƯA BIẾT provider đã nhận hay chưa — lần gọi trước có thể đã timeout
     * sau khi provider tạo phiên. Thử lại với cùng idempotency key là an toàn; đoán "chưa có gì" rồi sinh
     * key mới thì không.
     */
    NEW,
    /** Provider đã tạo phiên, đang chờ người dùng trả tiền. */
    PENDING,
    /** Provider xác nhận đã thu được tiền. Trạng thái cuối. */
    SUCCEEDED,
    /** Provider từ chối hoặc xác nhận phiên hỏng/hết hạn. Trạng thái cuối. */
    FAILED
}
