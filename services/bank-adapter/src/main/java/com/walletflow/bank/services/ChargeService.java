package com.walletflow.bank.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.bank.provider.ChargeRequest;
import com.walletflow.bank.provider.ChargeResult;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ConflictException;
import com.walletflow.common.tenant.TenantContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

/**
 * Thu tiền nạp ví qua provider — idempotent theo khoá của CHÍNH adapter (roadmap 2.2, ADR-016).
 *
 * <p><b>Ba bước, hai transaction, lời gọi provider nằm giữa và NGOÀI transaction DB:</b>
 * <ol>
 *   <li>Giành quyền: chèn hàng {@code NEW} (hoặc thấy hàng đã có). Commit ngay.</li>
 *   <li>Gọi provider với idempotency key tất định của hàng đó.</li>
 *   <li>Ghi câu trả lời + xếp event vào outbox trong cùng transaction.</li>
 * </ol>
 * Giữ transaction DB mở trong lúc chờ provider là giữ connection pool làm con tin cho một hệ thống bên
 * ngoài (CLAUDE.md). Đổi lại, có một khe giữa bước 1 và 3: crash ở đó để lại hàng {@code NEW}. Khe đó an
 * toàn vì lần thử sau gửi lại ĐÚNG khoá cũ — provider trả lại phiên đã tạo, không tạo phiên thứ hai.
 *
 * <p><b>Cố ý không dedup theo eventId</b> (khác ledger): lệnh gửi lại sau khi đã có câu trả lời thì
 * phát LẠI câu trả lời đó. Saga gửi lại lệnh chính là vì nó chưa nghe được câu trả lời — bỏ qua im lặng
 * là để saga treo (bài học ADR-014).
 */
@Slf4j
@Service
public class ChargeService {

    private static final String DEFAULT_DECLINE_CODE = "DECLINED";

    private final ProviderTransactionRepository repository;
    private final PaymentProvider provider;
    private final BankEventPublisher events;
    private final ProviderResponses responses;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final long recheckSeconds;
    private final Duration sessionTtl;

    public ChargeService(ProviderTransactionRepository repository,
                         PaymentProvider provider,
                         BankEventPublisher events,
                         ProviderResponses responses,
                         ObjectMapper objectMapper,
                         PlatformTransactionManager transactionManager,
                         @Value("${walletflow.bank.status-check.recheck-after}") Duration recheckAfter,
                         @Value("${walletflow.bank.session.ttl}") Duration sessionTtl) {
        if (sessionTtl.isNegative() || sessionTtl.isZero()) {
            throw new IllegalStateException("walletflow.bank.session.ttl phải dương: " + sessionTtl);
        }
        this.repository = repository;
        this.provider = provider;
        this.events = events;
        this.responses = responses;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.recheckSeconds = recheckAfter.toSeconds();
        this.sessionTtl = sessionTtl;
    }

    /**
     * @throws BadRequestException / ConflictException lệnh hỏng — thử lại vô ích (listener đẩy vào DLT)
     * @throws com.walletflow.bank.provider.ProviderUnavailableException không biết provider đã nhận chưa —
     *         để Kafka thử lại; hàng vẫn ở {@code NEW} và lần sau dùng đúng khoá cũ
     */
    public ProviderTransaction charge(BankChargeCommand command) {
        validate(command);
        ProviderTransaction claimed = transactionTemplate.execute(status -> claim(command));

        if (claimed.getStatus() != EProviderTxnStatus.NEW) {
            log.info("Bank: lệnh thu cho giao dịch {} đã có câu trả lời ({}) — phát lại",
                    command.transactionId(), claimed.getStatus());
            transactionTemplate.executeWithoutResult(status -> events.publishOutcome(claimed));
            return claimed;
        }

        // IP lấy từ lệnh đang xử lý, không từ hàng đã lưu: IP không thuộc danh tính của lần thu (ADR-017)
        ChargeResult result = provider.charge(new ChargeRequest(claimed.getIdempotencyKey(),
                claimed.getAmountMinor(), claimed.getCurrency(), command.clientIp(), sessionTtl));
        return transactionTemplate.execute(status -> record(claimed, result));
    }

    /**
     * Khoá gửi cho provider: tất định theo {@code (tenant, giao dịch, thao tác)}. Có tenant trong khoá vì
     * hai tenant có thể tình cờ trùng transactionId nhưng ở provider chỉ có MỘT không gian khoá.
     */
    static String idempotencyKey(UUID tenantId, UUID transactionId, EBankOperation operation) {
        String name = "bank:" + operation + ":" + tenantId + ":" + transactionId;
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private ProviderTransaction claim(BankChargeCommand command) {
        UUID tenantId = TenantContext.require();
        repository.insertIfAbsent(UUID.randomUUID(), tenantId, command.transactionId(),
                EBankOperation.CHARGE.name(), provider.name(), provider.account(),
                idempotencyKey(tenantId, command.transactionId(), EBankOperation.CHARGE),
                command.walletId(), command.amountMinor(), command.currency().name(), toJson(command));

        ProviderTransaction txn = repository.findByTenantIdAndTransactionIdAndOperation(
                        tenantId, command.transactionId(), EBankOperation.CHARGE)
                .orElseThrow(() -> new IllegalStateException(
                        "Vừa giành quyền mà không đọc lại được lần thu của giao dịch " + command.transactionId()));

        if (!txn.sameChargeAs(command)) {
            throw new ConflictException("BANK_CHARGE_MISMATCH", "Giao dịch " + command.transactionId()
                    + " đã có lệnh thu với ví/số tiền/loại tiền khác");
        }
        if (!txn.getProvider().equals(provider.name()) || !txn.belongsToAccount(provider.account())) {
            // Đổi provider/tài khoản merchant khi còn lần thu dở dang: khoá idempotency và mã tham chiếu chỉ có
            // nghĩa ở nơi đã tạo phiên (ADR-017)
            throw new ConflictException("BANK_PROVIDER_MISMATCH", "Lần thu của giao dịch "
                    + command.transactionId() + " thuộc provider " + txn.getProvider()
                    + " / tài khoản merchant khác");
        }
        return txn;
    }

    private ProviderTransaction record(ProviderTransaction txn, ChargeResult result) {
        switch (result) {
            case ChargeResult.Created created -> repository.markPending(txn.getTenantId(), txn.getId(),
                    created.providerRef(), created.paymentUrl(), responses.json(created.rawResponse()),
                    recheckSeconds, created.sessionCreatedAt(), created.expiresAt());
            case ChargeResult.Declined declined -> {
                repository.markFailed(txn.getTenantId(), txn.getId(), EProviderTxnStatus.NEW.name(),
                        ProviderResponses.failureCode(declined.failureCode(), DEFAULT_DECLINE_CODE),
                        responses.json(declined.rawResponse()));
                log.info("Bank: provider từ chối thu cho giao dịch {}: {}", txn.getTransactionId(),
                        declined.failureCode());
            }
        }
        // Đọc lại thay vì suy ra: nếu luồng khác đã chuyển hàng trước (UPDATE trả 0) thì câu trả lời đúng
        // là trạng thái nó đã ghi, không phải thứ luồng này vừa nhận.
        ProviderTransaction current = repository.findByTenantIdAndId(txn.getTenantId(), txn.getId())
                .orElseThrow();
        events.publishOutcome(current);
        return current;
    }

    private static void validate(BankChargeCommand command) {
        if (command.transactionId() == null || command.walletId() == null || command.currency() == null) {
            throw new BadRequestException("BANK_CHARGE_INVALID", "Lệnh thu thiếu transactionId/walletId/currency");
        }
        if (command.amountMinor() <= 0) {
            throw new BadRequestException("BANK_CHARGE_INVALID", "Số tiền thu phải dương");
        }
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Không serialize được lệnh thu", e);
        }
    }
}
