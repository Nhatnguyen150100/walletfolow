package com.walletflow.transaction.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Quyết định của người DUYỆT trên một yêu cầu 4-eyes (roadmap 2.5 PR2).
 *
 * @param approve true = duyệt và THỰC THI ngay trong cùng transaction
 * @param note    ghi chú của người duyệt, nhất là khi từ chối
 */
public record DecisionRequest(
        @NotNull(message = "Phải nêu rõ duyệt hay từ chối")
        Boolean approve,

        @Size(max = 1000, message = "Ghi chú tối đa 1000 ký tự")
        String note
) {
}
