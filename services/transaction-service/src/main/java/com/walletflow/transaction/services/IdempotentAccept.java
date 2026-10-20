package com.walletflow.transaction.services;

import com.walletflow.common.exception.AppException;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.transaction.entity.IdempotencyRecord;
import org.springframework.http.HttpStatus;

import java.util.regex.Pattern;

/**
 * Luật dùng chung của {@code Idempotency-Key} cho mọi lệnh tiền (ADR-013). Ở chung một chỗ vì hai
 * luồng (chuyển tiền, nạp tiền) phải hành xử GIỐNG HỆT nhau: một bản sao lệch đi dù chỉ ở chỗ "key
 * dùng lại cho lệnh khác" là một đường trừ tiền hai lần.
 */
final class IdempotentAccept {

    /** Ký tự an toàn cho log/header; đủ cho UUID, ULID và mọi định dạng key phổ biến. */
    private static final Pattern KEY_FORMAT = Pattern.compile("[A-Za-z0-9_\\-:.]{1,64}");

    private IdempotentAccept() {
    }

    static String requireValidKey(String key) {
        if (key == null || key.isBlank()) {
            throw new BadRequestException("IDEMPOTENCY_KEY_REQUIRED",
                    "Thiếu header Idempotency-Key — lệnh tiền không được nhận nếu không gửi lại an toàn được");
        }
        if (!KEY_FORMAT.matcher(key).matches()) {
            throw new BadRequestException("IDEMPOTENCY_KEY_INVALID",
                    "Idempotency-Key chỉ gồm chữ, số, '_', '-', ':', '.' và tối đa 64 ký tự");
        }
        return key;
    }

    /**
     * Key đã có câu trả lời. Cùng nội dung → trả lại nguyên văn. Khác nội dung → 422: client đang dùng
     * lại key cho một lệnh KHÁC, đó là bug phía client và phải được báo to — lặng lẽ trả lời của lệnh
     * cũ sẽ khiến client tin lệnh mới đã được nhận.
     */
    static AcceptResult replay(IdempotencyRecord record, String hash) {
        if (!record.getRequestHash().equals(hash)) {
            throw new AppException(HttpStatus.UNPROCESSABLE_ENTITY, "IDEMPOTENCY_KEY_REUSED",
                    "Idempotency-Key này đã được dùng cho một lệnh khác");
        }
        return new AcceptResult(record.getResponseStatus(), record.getResponseBody(),
                record.getTransactionId(), true);
    }
}
