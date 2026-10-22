package com.walletflow.bank.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.RefundRequest;
import com.walletflow.bank.provider.RefundResult;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.common.event.BankRefundCommand;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ConflictException;
import com.walletflow.common.tenant.TenantContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

/**
 * Hoàn tiền một lần thu đã thành công — bước bù trừ của saga nạp tiền (roadmap 2.4b, ADR-020).
 *
 * <p>Cùng khuôn với {@link ChargeService}: giành quyền (hàng {@code REFUND} ở {@code NEW}, commit ngay) → gọi
 * provider NGOÀI transaction với khoá tất định → ghi câu trả lời + xếp event vào outbox trong một transaction.
 * Crash giữa chừng để lại hàng {@code NEW}; lần thử sau gửi lại ĐÚNG khoá cũ nên provider trả lại lần hoàn đã
 * có, không hoàn lần hai. Lệnh gửi lại sau khi đã có câu trả lời thì phát LẠI câu trả lời đó (ADR-016).
 *
 * <p><b>Chỉ hoàn trọn, chỉ hoàn lần thu đã thành công.</b> Lệnh lệch ví/số tiền/loại tiền với lần thu, không có
 * lần thu nào, hoặc lần thu chưa {@code SUCCEEDED} → bug phía producer → DLT, KHÔNG ghi hàng nào. Saga chỉ gửi
 * lệnh hoàn sau khi đã nghe {@code bank.event.completed} của lần thu, nên lần thu chưa thành công ở đây là bug —
 * và {@code PENDING} là "chưa biết", không phải "chưa thu" (roadmap §5.7). Ghi một hàng REFUND {@code FAILED}
 * cho nó thì hàng đó là vĩnh viễn: mọi lệnh gửi lại sau khi lần thu thành công chỉ phát lại lời từ chối.
 */
@Slf4j
@Service
public class RefundService {

    private static final String DEFAULT_DECLINE_CODE = "REFUND_DECLINED";

    private final ProviderTransactionRepository repository;
    private final PaymentProvider provider;
    private final BankEventPublisher events;
    private final ProviderResponses responses;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final Counter refundConflicts;

    public RefundService(ProviderTransactionRepository repository,
                         PaymentProvider provider,
                         BankEventPublisher events,
                         ProviderResponses responses,
                         ObjectMapper objectMapper,
                         PlatformTransactionManager transactionManager,
                         MeterRegistry meterRegistry) {
        this.repository = repository;
        this.provider = provider;
        this.events = events;
        this.responses = responses;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.refundConflicts = meterRegistry.counter("walletflow.bank.refund_conflict");
    }

    /**
     * @throws BadRequestException / ConflictException lệnh hỏng — thử lại vô ích (listener đẩy vào DLT)
     * @throws com.walletflow.bank.provider.ProviderUnavailableException không biết provider đã hoàn chưa — để
     *         Kafka thử lại; hàng vẫn ở {@code NEW} và lần sau dùng đúng khoá cũ
     */
    public ProviderTransaction refund(BankRefundCommand command) {
        validate(command);
        Claim claim = transactionTemplate.execute(status -> claim(command));
        ProviderTransaction refund = claim.refund();

        if (refund.getStatus() != EProviderTxnStatus.NEW) {
            log.info("Bank: lệnh hoàn cho giao dịch {} đã có câu trả lời ({}) — phát lại",
                    command.transactionId(), refund.getStatus());
            transactionTemplate.executeWithoutResult(status -> events.publishOutcome(refund));
            return refund;
        }

        ProviderTransaction charge = claim.charge();
        RefundResult result = provider.refund(new RefundRequest(refund.getIdempotencyKey(), charge.getProviderRef(),
                refund.getAmountMinor(), refund.getCurrency()));
        return transactionTemplate.execute(status -> record(refund, result));
    }

    private Claim claim(BankRefundCommand command) {
        UUID tenantId = TenantContext.require();
        ProviderTransaction charge = repository.findByTenantIdAndTransactionIdAndOperation(
                        tenantId, command.transactionId(), EBankOperation.CHARGE)
                .orElseThrow(() -> new ConflictException("BANK_REFUND_NO_CHARGE",
                        "Giao dịch " + command.transactionId() + " không có lần thu nào để hoàn"));

        if (!charge.coversRefund(command)) {
            throw new ConflictException("BANK_REFUND_MISMATCH", "Lệnh hoàn của giao dịch "
                    + command.transactionId() + " lệch ví/số tiền/loại tiền với lần thu — chỉ hoàn trọn");
        }
        if (!charge.getProvider().equals(provider.name()) || !charge.belongsToAccount(provider.account())) {
            // Lần hoàn chỉ làm được ở đúng nơi đã thu: mã tham chiếu của lần thu vô nghĩa ở provider/tài khoản khác
            throw new ConflictException("BANK_PROVIDER_MISMATCH", "Lần thu của giao dịch "
                    + command.transactionId() + " thuộc provider " + charge.getProvider()
                    + " / tài khoản merchant khác");
        }
        if (charge.getStatus() != EProviderTxnStatus.SUCCEEDED) {
            throw new ConflictException("BANK_REFUND_CHARGE_NOT_SUCCEEDED", "Lần thu của giao dịch "
                    + command.transactionId() + " đang " + charge.getStatus() + " — chỉ hoàn lần thu đã thành công");
        }

        repository.insertRefundIfAbsent(UUID.randomUUID(), tenantId, command.transactionId(), charge.getProvider(),
                provider.account(), ChargeService.idempotencyKey(tenantId, command.transactionId(),
                        EBankOperation.REFUND),
                charge.getWalletId(), charge.getAmountMinor(), charge.getCurrency().name(), toJson(command),
                charge.getId());

        ProviderTransaction refund = repository.findByTenantIdAndTransactionIdAndOperation(
                        tenantId, command.transactionId(), EBankOperation.REFUND)
                .orElseThrow(() -> new IllegalStateException(
                        "Vừa giành quyền mà không đọc lại được lần hoàn của giao dịch " + command.transactionId()
                                + " (xung đột ở một khoá unique khác của provider_transactions?)"));
        return new Claim(charge, refund);
    }

    private ProviderTransaction record(ProviderTransaction refund, RefundResult result) {
        int changed = switch (result) {
            case RefundResult.Refunded refunded -> repository.markRefunded(refund.getTenantId(), refund.getId(),
                    refunded.refundRef(), responses.json(refunded.rawResponse()));
            case RefundResult.Declined declined -> repository.markFailed(refund.getTenantId(), refund.getId(),
                    EProviderTxnStatus.NEW.name(),
                    ProviderResponses.failureCode(declined.failureCode(), DEFAULT_DECLINE_CODE),
                    responses.json(declined.rawResponse()));
        };
        // Đọc lại thay vì suy ra: luồng khác có thể đã chuyển hàng trước (UPDATE trả 0)
        ProviderTransaction current = repository.findByTenantIdAndId(refund.getTenantId(), refund.getId())
                .orElseThrow();
        EProviderTxnStatus told = result instanceof RefundResult.Refunded
                ? EProviderTxnStatus.SUCCEEDED : EProviderTxnStatus.FAILED;

        if (changed == 1) {
            if (told == EProviderTxnStatus.SUCCEEDED) {
                log.info("Bank: đã hoàn tiền cho giao dịch {}", refund.getTransactionId());
            } else {
                // Tiền của người dùng vẫn đang ở chỗ ta — saga chuyển người xử lý; mức WARN để thấy được
                log.warn("Bank: KHÔNG hoàn được tiền cho giao dịch {}: {}", refund.getTransactionId(),
                        current.getFailureCode());
            }
        } else if (current.getStatus() != told) {
            // Luồng khác đã ghi câu trả lời NGƯỢC với câu provider vừa nói cho cùng khoá. Không lật lại hàng (câu
            // trả lời đầu thắng, như lần thu), nhưng một lần hoàn thật có thể đang bị bỏ sót khỏi sổ của adapter
            // — đối soát phải nhìn thấy, nên kêu to.
            refundConflicts.increment();
            log.error("Bank: provider vừa trả lời {} cho lần hoàn của giao dịch {} nhưng hàng đã {} — cần người "
                    + "đối chiếu với portal provider", told, refund.getTransactionId(), current.getStatus());
        }
        events.publishOutcome(current);
        return current;
    }

    private static void validate(BankRefundCommand command) {
        if (command.transactionId() == null || command.walletId() == null || command.currency() == null) {
            throw new BadRequestException("BANK_REFUND_INVALID", "Lệnh hoàn thiếu transactionId/walletId/currency");
        }
        if (command.amountMinor() <= 0) {
            throw new BadRequestException("BANK_REFUND_INVALID", "Số tiền hoàn phải dương");
        }
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Không serialize được lệnh hoàn", e);
        }
    }

    private record Claim(ProviderTransaction charge, ProviderTransaction refund) {
    }
}
