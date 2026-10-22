package com.walletflow.bank.provider;

import com.walletflow.bank.enums.EProviderState;
import com.walletflow.common.money.ECurrency;

import java.util.Objects;

/**
 * @param failureCode         chỉ có khi {@code state = FAILED}
 * @param capturedAmountMinor số tiền provider THỰC THU (có nghĩa khi {@code SUCCEEDED}). Adapter đối chiếu
 *                            với số đã yêu cầu trước khi báo thành công — ledger sẽ cộng đúng số trong event,
 *                            nên số đó phải là số provider nói, không phải số mình mong
 * @param capturedCurrency    loại tiền provider thực thu
 * @param rawResponse         câu trả lời thô của provider, lưu nguyên văn để đối soát
 */
public record ProviderStatus(EProviderState state, String failureCode, long capturedAmountMinor,
                             ECurrency capturedCurrency, String rawResponse) {

    public ProviderStatus {
        Objects.requireNonNull(state, "state");
    }
}
