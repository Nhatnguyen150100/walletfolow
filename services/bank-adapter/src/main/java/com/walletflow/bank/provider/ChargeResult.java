package com.walletflow.bank.provider;

import java.util.Objects;

/**
 * Câu trả lời DỨT KHOÁT của provider khi tạo phiên thu. Lỗi kỹ thuật không phải giá trị của kiểu này —
 * xem {@link PaymentProvider}.
 */
public sealed interface ChargeResult {

    /** Câu trả lời thô của provider, lưu nguyên văn để đối soát. */
    String rawResponse();

    /**
     * Provider đã tạo phiên; người dùng trả tiền ở {@code paymentUrl}. Cả hai mã đều bắt buộc: một phiên
     * không có mã thì không hỏi lại được, không có URL thì người dùng không trả được — implementation nhận
     * câu trả lời như vậy phải coi là lỗi kỹ thuật, không được tạo giá trị này.
     */
    record Created(String providerRef, String paymentUrl, String rawResponse) implements ChargeResult {
        public Created {
            Objects.requireNonNull(providerRef, "providerRef");
            Objects.requireNonNull(paymentUrl, "paymentUrl");
        }
    }

    /** Provider từ chối tạo phiên (sai số tiền, loại tiền không hỗ trợ...). Thử lại vẫn bị từ chối. */
    record Declined(String failureCode, String rawResponse) implements ChargeResult {
        public Declined {
            Objects.requireNonNull(failureCode, "failureCode");
        }
    }
}
