package com.walletflow.bank.services;

import com.walletflow.bank.dto.ProviderOperationRow;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.common.bank.ProviderContext;
import com.walletflow.common.bank.ProviderOperationView;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.logging.SensitiveDataMasker;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Ngữ cảnh provider của một lệnh tiền, phục vụ người trực (roadmap 2.5 PR3, ADR-021).
 *
 * <p>Chỉ đọc. Đây là đường DUY NHẤT đưa {@code raw_response} ra khỏi bank-adapter, nên việc che dữ
 * liệu nhạy cảm đặt ở đây chứ không ở controller: một endpoint thứ hai thêm sau này sẽ phải đi qua
 * service này, còn nếu che ở controller thì nó sẽ lặng lẽ không che.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProviderContextService {

    private final ProviderTransactionRepository providerTransactionRepository;

    /**
     * Mọi lần thu / lần hoàn bank-adapter đã làm cho {@code transactionId}.
     *
     * <p><b>Không 404 khi rỗng</b>, cùng lý do với ledger: "bank-adapter chưa từng nhận lệnh nào cho
     * giao dịch này" là một câu trả lời đúng, và với người đang xử lý ca thì nó khác hẳn "không hỏi
     * được bank-adapter".
     */
    @Transactional(readOnly = true)
    public ProviderContext contextOf(UUID transactionId) {
        UUID tenantId = TenantContext.require();
        List<ProviderOperationView> operations = providerTransactionRepository
                .operationsOf(tenantId, transactionId)
                .stream()
                .map(ProviderContextService::toView)
                .toList();
        return new ProviderContext(transactionId, operations);
    }

    /**
     * <b>Che dữ liệu nhạy cảm ở đây là bắt buộc, không phải phòng xa.</b> {@code raw_response} là
     * nguyên văn thứ cổng thanh toán gửi về: ta không kiểm soát được nó chứa gì, và lịch sử của
     * ngành cho thấy nó có lúc chứa cả số thẻ lẫn chữ ký. Tới trước PR3 nó chưa từng rời DB, nên
     * việc để nó đi ra qua một API là lần đầu tiên rủi ro đó thành thật.
     *
     * <p>Dùng chính {@code SensitiveDataMasker} của đường log (ADR-017) thay vì viết bộ lọc thứ hai:
     * hai bộ quy tắc che khác nhau nghĩa là một trong hai sẽ bị bỏ quên khi provider đổi định dạng.
     */
    private static ProviderOperationView toView(ProviderOperationRow row) {
        return new ProviderOperationView(
                row.getId(),
                EBankOperation.valueOf(row.getOperation()),
                row.getProvider(),
                row.getProviderRef(),
                row.getAmountMinor(),
                ECurrency.valueOf(row.getCurrency()),
                row.getStatus(),
                row.getFailureCode(),
                row.getCreatedAt(),
                row.getUpdatedAt(),
                row.getRefundOf(),
                SensitiveDataMasker.mask(row.getRawResponse()));
    }
}
