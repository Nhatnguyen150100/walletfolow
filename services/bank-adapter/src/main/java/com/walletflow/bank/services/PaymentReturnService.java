package com.walletflow.bank.services;

import com.walletflow.bank.dto.PaymentReturnResponse;
import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.VerifiedWebhook;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.exception.UnauthorizedException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Optional;

/**
 * Trang người dùng quay về sau khi trả tiền (roadmap 2.3b, ADR-016 §1).
 *
 * <p><b>Tham số redirect KHÔNG BAO GIỜ là kết quả.</b> Nó đi qua trình duyệt của người dùng — tức qua tay
 * người có động cơ sửa nó nhất. Chữ ký chặn việc chế ra một kết quả mới, nhưng không chặn việc phát lại một
 * redirect thành công CŨ; và ngay cả một redirect thật cũng chỉ nói "người dùng đã rời trang thanh toán",
 * không nói "tiền đã về". Vì vậy lớp này chỉ đọc trạng thái adapter đang ghi nhận và trả về.
 *
 * <p>Việc duy nhất nó ghi là kéo lần hỏi provider kế tiếp về ngay bây giờ: người dùng vừa quay về là tín
 * hiệu đáng tin về THỜI ĐIỂM (dù không đáng tin về KẾT QUẢ), và nhịp backoff lúc đó có thể đang ở 10 phút.
 */
@Slf4j
@Service
public class PaymentReturnService {

    private final PaymentProvider provider;
    private final ProviderTransactionRepository repository;
    private final Counter rejected;

    public PaymentReturnService(PaymentProvider provider,
                                ProviderTransactionRepository repository,
                                MeterRegistry meterRegistry) {
        this.provider = provider;
        this.repository = repository;
        // Đăng ký lúc khởi động để series tồn tại ở 0
        this.rejected = meterRegistry.counter("walletflow.bank.payment_return.rejected");
    }

    /**
     * @param providerName tên provider lấy từ đường dẫn — phải khớp provider đang chạy, nếu không một
     *                     redirect VNPay tới hệ thống đang chạy mock sẽ được kiểm bằng khoá của mock
     * @throws UnauthorizedException      chữ ký sai/thiếu, hoặc sai provider
     * @throws ResourceNotFoundException  mã tham chiếu không có trong hệ thống
     */
    @Transactional
    public PaymentReturnResponse resolve(String providerName, Map<String, String> params) {
        if (!provider.name().equals(providerName)) {
            rejected.increment();
            log.debug("Bank: trang quay về gửi cho provider '{}' nhưng đang chạy '{}' — từ chối",
                    providerName, provider.name());
            throw new UnauthorizedException("INVALID_SIGNATURE", "Chữ ký không hợp lệ");
        }
        Optional<VerifiedWebhook> verified = provider.verifyWebhook(params);
        if (verified.isEmpty()) {
            rejected.increment();
            // KHÔNG log params: của người lạ, và endpoint này công khai (xem ADR-018 — nguồn alert là counter)
            log.debug("Bank: trang quay về {} có chữ ký KHÔNG hợp lệ — từ chối", providerName);
            throw new UnauthorizedException("INVALID_SIGNATURE", "Chữ ký không hợp lệ");
        }

        String providerRef = verified.get().providerRef();
        ProviderTransaction txn = Optional.ofNullable(providerRef)
                .flatMap(ref -> repository.findByProviderAndProviderRefAndOperation(
                        providerName, ref, EBankOperation.CHARGE))
                // Cùng một câu trả lời cho "không có" và "có nhưng không đọc được": khác nhau là để người
                // ngoài dò xem mã nào có thật (CLAUDE.md — chống dò id, ADR-018 §5)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy lần thu"));

        if (txn.getStatus() == EProviderTxnStatus.PENDING) {
            // Kéo lịch hỏi lại về ngay. KHÔNG gọi provider tại chỗ: người dùng đang chờ màn hình, còn
            // getStatus là một lời gọi mạng tới resilience.timeout — việc đó thuộc về worker/job đối chiếu.
            // Hàng đã chốt thì không có gì để kéo: F5 trên một trang đã xong không được sinh ra một
            // transaction ghi mỗi lần.
            repository.expediteCheck(txn.getTenantId(), txn.getId());
        }
        return PaymentReturnResponse.of(txn);
    }
}
