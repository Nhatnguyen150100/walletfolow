package com.walletflow.transaction.dto;

import com.walletflow.transaction.enums.EResolutionAction;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Đề xuất một thao tác xử lý ca cần can thiệp (roadmap 2.5 PR2).
 *
 * @param reason   bắt buộc: một thao tác đụng tiền không giải thích được thì không nên xảy ra
 * @param evidence bằng chứng — thường là mã lần hoàn ở portal của provider. Lần hoàn tay không để
 *                 lại dấu vết nào ở bank-adapter (ADR-020), nên đây là chỗ duy nhất ghi lại nó
 */
public record ResolveRequest(
        @NotNull(message = "Thiếu thao tác cần thực hiện")
        EResolutionAction action,

        @NotBlank(message = "Phải nêu lý do xử lý")
        @Size(max = 1000, message = "Lý do tối đa 1000 ký tự")
        String reason,

        @Size(max = 255, message = "Bằng chứng tối đa 255 ký tự")
        String evidence
) {
}
