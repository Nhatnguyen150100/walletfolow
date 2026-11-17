package com.walletflow.transaction.dto;

import com.walletflow.common.reconciliation.CloseBreakRequest;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Thân của {@code POST /api/v1/admin/reconciliation/breaks/{source}/{id}/close} (ADR-025 §3).
 *
 * <p>Tách khỏi {@link CloseBreakRequest} của common-lib tuy cùng một trường: đây là hợp đồng với
 * NGƯỜI (validate bằng Jakarta, message tiếng Việt), còn kia là hợp đồng giữa hai service. Gộp
 * chúng sẽ kéo Jakarta Validation vào common-lib — vốn không có nó trên classpath.
 *
 * @param reason vì sao đóng. Bắt buộc: đóng break là tuyên bố "đã hiểu nguyên nhân", và một tuyên
 *               bố không giải thích được thì không nên xảy ra.
 */
public record CloseBreakBody(
        @NotBlank(message = "Phải ghi lý do đóng chỗ lệch này")
        @Size(max = CloseBreakRequest.MAX_NOTE, message = "Lý do tối đa 1000 ký tự")
        String reason) {
}
