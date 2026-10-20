package com.walletflow.transaction.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.common.wallet.WalletInfo;
import com.walletflow.transaction.client.WalletDirectory;
import com.walletflow.transaction.dto.TopupAccepted;
import com.walletflow.transaction.dto.TopupRequest;
import com.walletflow.transaction.entity.IdempotencyRecord;
import com.walletflow.transaction.enums.ETransactionStatus;
import com.walletflow.transaction.repositories.IdempotencyRepository;
import com.walletflow.transaction.repositories.TransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

/**
 * Nhận lệnh nạp tiền — idempotent theo {@code Idempotency-Key} (roadmap 2.4a, ADR-019).
 *
 * <p>Ba pha giống hệt {@link TransferService} (và vì lý do giống hệt — xem javadoc ở đó), chỉ khác ở
 * bước cuối: lệnh đi vào outbox là lệnh <b>thu tiền</b> gửi bank-adapter, không phải lệnh ghi sổ.
 *
 * <p><b>Không có bút toán nào được tạo ở đây.</b> Ví chỉ được cộng sau khi provider xác nhận đã thu
 * được tiền và ledger ghi sổ xong (ADR-016 §1, ADR-019) — toàn bộ quãng đó do {@link SagaService} lái.
 * Nhận lệnh nạp mà cộng ví luôn là cách tự tạo tiền từ không khí.
 */
@Slf4j
@Service
public class TopupService {

    private final IdempotencyRepository idempotencyRepository;
    private final TransactionRepository transactionRepository;
    private final WalletDirectory walletDirectory;
    private final BankCommandSender commandSender;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final Duration keyTtl;
    private final Duration chargeDeadline;

    public TopupService(
            IdempotencyRepository idempotencyRepository,
            TransactionRepository transactionRepository,
            WalletDirectory walletDirectory,
            BankCommandSender commandSender,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager,
            @Value("${walletflow.transaction.idempotency-ttl:PT24H}") Duration keyTtl,
            @Value("${walletflow.transaction.topup.charge-deadline:PT5M}") Duration chargeDeadline) {
        this.idempotencyRepository = idempotencyRepository;
        this.transactionRepository = transactionRepository;
        this.walletDirectory = walletDirectory;
        this.commandSender = commandSender;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.keyTtl = keyTtl;
        this.chargeDeadline = chargeDeadline;
    }

    public AcceptResult accept(UUID userId, String idempotencyKey, TopupRequest request) {
        UUID tenantId = TenantContext.require();
        String key = IdempotentAccept.requireValidKey(idempotencyKey);
        String hash = requestHash(request);

        Optional<IdempotencyRecord> answered = idempotencyRepository.findLive(tenantId, userId, key);
        if (answered.isPresent()) {
            return IdempotentAccept.replay(answered.get(), hash);
        }

        try {
            validateWallet(userId, request);
        } catch (RuntimeException rejected) {
            Optional<IdempotencyRecord> acceptedMeanwhile = idempotencyRepository.findLive(tenantId, userId, key);
            if (acceptedMeanwhile.isPresent()) {
                return IdempotentAccept.replay(acceptedMeanwhile.get(), hash);
            }
            throw rejected;
        }

        return transactionTemplate.execute(status -> acceptOnce(tenantId, userId, key, hash, request));
    }

    private AcceptResult acceptOnce(UUID tenantId, UUID userId, String key, String hash,
                                    TopupRequest request) {
        UUID transactionId = UUID.randomUUID();
        String body = acceptedBody(transactionId, request);

        int claimed = idempotencyRepository.claim(tenantId, userId, key, hash, transactionId,
                HttpStatus.ACCEPTED.value(), body, keyTtl.toSeconds());
        if (claimed == 0) {
            IdempotencyRecord winner = idempotencyRepository.findLive(tenantId, userId, key)
                    .orElseThrow(() -> new IllegalStateException("Mất câu trả lời của key " + key));
            return IdempotentAccept.replay(winner, hash);
        }

        transactionRepository.insertTopup(transactionId, tenantId, userId, request.walletId(),
                request.amountMinor(), request.currency().name());

        UUID commandEventId = UUID.randomUUID();
        transactionRepository.insertSagaStart(transactionId, tenantId, SagaService.STEP_BANK_CHARGE,
                commandEventId, chargeDeadline.toSeconds());
        // clientIp để null: sau gateway, IP mà service này nhìn thấy là IP của gateway, không phải của
        // người dùng — gửi đi là nói dối provider. Lấy đúng IP cần chốt số proxy tin cậy trước
        // (ADR-018, mục còn mở). Thiếu IP thì bank-adapter dùng IP máy chủ, đúng hợp đồng v1.
        commandSender.sendCharge(transactionId, request.walletId(), request.amountMinor(),
                request.currency(), null, commandEventId);

        log.info("Transaction: nhận lệnh nạp {} {} vào ví {} (giao dịch {})",
                request.amountMinor(), request.currency(), request.walletId(), transactionId);
        return new AcceptResult(HttpStatus.ACCEPTED.value(), body, transactionId, false);
    }

    /**
     * Ví đích phải tồn tại, thuộc về người gọi, đúng loại tiền và đang hoạt động.
     *
     * <p>Ví ĐÓNG BĂNG không nạp được — khác ví nhận trong chuyển tiền (ADR-011 cho nhận tiền vào ví
     * đóng băng). Ở đây người bị đóng băng chính là người bỏ tiền ra: để họ trả tiền cho một ví họ
     * không rút ra được là tự tạo ra một ca hoàn tiền.
     */
    private void validateWallet(UUID userId, TopupRequest request) {
        // Ví không tồn tại và ví của người khác trả CÙNG một lỗi: phân biệt hai trường hợp là cho
        // phép dò xem một wallet id có tồn tại hay không.
        WalletInfo wallet = walletDirectory.find(request.walletId())
                .filter(w -> w.userId().equals(userId))
                .orElseThrow(() -> new ResourceNotFoundException("Không có ví " + request.walletId()));
        if (wallet.currency() != request.currency()) {
            throw unprocessable("CURRENCY_MISMATCH", "Ví không dùng loại tiền " + request.currency());
        }
        if (wallet.status() != EWalletStatus.ACTIVE) {
            throw unprocessable("WALLET_NOT_ACTIVE", "Ví đang ở trạng thái " + wallet.status());
        }
    }

    private String acceptedBody(UUID transactionId, TopupRequest request) {
        TopupAccepted accepted = new TopupAccepted(transactionId, ETransactionStatus.PROCESSING,
                request.walletId(), request.amountMinor(), request.currency(), Instant.now());
        BaseResponse<TopupAccepted> response = BaseResponse.<TopupAccepted>builder()
                .success(true)
                .status(HttpStatus.ACCEPTED.value())
                .message("Đã nhận lệnh nạp tiền")
                .data(accepted)
                .build();
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Không serialize được câu trả lời", e);
        }
    }

    /**
     * SHA-256 của nội dung lệnh ở dạng chuẩn hoá — xem {@link TransferService#requestHash}. Tiền tố
     * {@code topup} để cùng một key dùng cho lệnh nạp và lệnh chuyển "tình cờ giống nhau" vẫn ra hai
     * hash khác nhau, tức là bị bắt là dùng lại key chứ không lặng lẽ phát lại nhầm câu trả lời.
     */
    static String requestHash(TopupRequest request) {
        String canonical = String.join("\n",
                "topup",
                request.walletId().toString(),
                Long.toString(request.amountMinor()),
                request.currency().name());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static AppException unprocessable(String code, String message) {
        return new AppException(HttpStatus.UNPROCESSABLE_ENTITY, code, message);
    }
}
