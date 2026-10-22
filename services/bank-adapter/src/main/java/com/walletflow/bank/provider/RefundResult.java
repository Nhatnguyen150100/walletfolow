package com.walletflow.bank.provider;

/** Câu trả lời DỨT KHOÁT của provider cho một yêu cầu hoàn tiền. */
public sealed interface RefundResult {

    String rawResponse();

    record Refunded(String refundRef, String rawResponse) implements RefundResult {
    }

    /** Provider từ chối hoàn (lần thu chưa thành công, hoàn vượt số đã thu...). */
    record Declined(String failureCode, String rawResponse) implements RefundResult {
    }
}
