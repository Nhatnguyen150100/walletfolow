package com.walletflow.transaction.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.outbox.OutboxPublisher;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.common.wallet.WalletInfo;
import com.walletflow.transaction.client.WalletDirectory;
import com.walletflow.transaction.dto.TransactionView;
import com.walletflow.transaction.dto.TransferAccepted;
import com.walletflow.transaction.dto.TransferRequest;
import com.walletflow.transaction.entity.IdempotencyRecord;
import com.walletflow.transaction.enums.ETransactionStatus;
import com.walletflow.transaction.repositories.IdempotencyRepository;
import com.walletflow.transaction.repositories.TransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Nhận lệnh chuyển tiền — idempotent theo {@code Idempotency-Key} (roadmap 1.2a, ADR-013).
 *
 * <p>Ba pha, theo đúng thứ tự này:
 * <ol>
 *   <li><b>Key đã có câu trả lời?</b> Trả lại nguyên văn — KHÔNG kiểm tra lại gì. Lệnh đã được nhận
 *       thì vẫn là đã được nhận, kể cả khi ví vừa bị đóng băng sau đó: trả lỗi cho lần gửi lại sẽ
 *       khiến client tưởng lệnh thất bại trong khi tiền có thể đang đi.</li>
 *   <li><b>Kiểm tra ví</b> qua wallet-service — NGOÀI transaction DB, không giữ kết nối hay khoá nào
 *       trong lúc chờ HTTP. Kiểm tra thất bại thì xem lại key một lần nữa: request trùng có thể đã
 *       được nhận trong lúc ta chờ.</li>
 *   <li><b>Một transaction cục bộ:</b> giành key + ghi câu trả lời + transactions + saga_state +
 *       lệnh ghi sổ vào outbox. Cùng sống hoặc cùng chết. Giành key thất bại (request trùng chạy song
 *       song đã thắng) thì không ghi gì cả, trả lại câu trả lời của bên thắng.</li>
 * </ol>
 *
 * <p>KHÔNG kiểm tra số dư: read model có thể trễ so với sổ cái, chỉ ledger (dưới {@code FOR UPDATE})
 * được quyết "đủ tiền" (roadmap 1.2). Thiếu tiền thì ledger từ chối và giao dịch thành FAILED (1.2b).
 */
@Slf4j
@Service
public class TransferService {

    /** Ký tự an toàn cho log/header; đủ cho UUID, ULID và mọi định dạng key phổ biến. */
    private static final Pattern KEY_FORMAT = Pattern.compile("[A-Za-z0-9_\\-:.]{1,64}");

    private final IdempotencyRepository idempotencyRepository;
    private final TransactionRepository transactionRepository;
    private final WalletDirectory walletDirectory;
    private final OutboxPublisher outboxPublisher;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final Duration keyTtl;
    private final Duration sagaDeadline;

    public TransferService(
            IdempotencyRepository idempotencyRepository,
            TransactionRepository transactionRepository,
            WalletDirectory walletDirectory,
            OutboxPublisher outboxPublisher,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager,
            @Value("${walletflow.transaction.idempotency-ttl:PT24H}") Duration keyTtl,
            @Value("${walletflow.transaction.saga-deadline:PT2M}") Duration sagaDeadline) {
        this.idempotencyRepository = idempotencyRepository;
        this.transactionRepository = transactionRepository;
        this.walletDirectory = walletDirectory;
        this.outboxPublisher = outboxPublisher;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.keyTtl = keyTtl;
        this.sagaDeadline = sagaDeadline;
    }

    /**
     * Câu trả lời cho một lần gọi. {@code body} là JSON nguyên văn — lần gửi lại nhận về đúng từng
     * byte của lần đầu.
     */
    public record AcceptResult(int status, String body, UUID transactionId, boolean replayed) {
    }

    public AcceptResult accept(UUID userId, String idempotencyKey, TransferRequest request) {
        UUID tenantId = TenantContext.require();
        String key = requireValidKey(idempotencyKey);
        String hash = requestHash(request);

        // 1 — Key đã có câu trả lời: trả lại, không kiểm tra gì thêm
        Optional<IdempotencyRecord> answered = idempotencyRepository.findLive(tenantId, userId, key);
        if (answered.isPresent()) {
            return replay(answered.get(), hash);
        }

        // 2 — Kiểm tra ví, ngoài mọi transaction DB
        try {
            validateWallets(userId, request);
        } catch (RuntimeException rejected) {
            // Một request TRÙNG key có thể vừa được nhận trong lúc ta kiểm tra (nó qua bước 1 trước
            // khi ta commit... hoặc ngược lại). Nếu key đã có câu trả lời thì lệnh ĐÃ được nhận: trả
            // câu trả lời đó, không trả lỗi. Trả 503/422 ở đây khiến client tin lệnh thất bại, gửi lại
            // với key MỚI — và tiền bị trừ hai lần.
            Optional<IdempotencyRecord> acceptedMeanwhile = idempotencyRepository.findLive(tenantId, userId, key);
            if (acceptedMeanwhile.isPresent()) {
                return replay(acceptedMeanwhile.get(), hash);
            }
            throw rejected;
        }

        // 3 — Nhận lệnh trong MỘT transaction
        return transactionTemplate.execute(status -> acceptOnce(tenantId, userId, key, hash, request));
    }

    @Transactional(readOnly = true)
    public TransactionView get(UUID userId, UUID transactionId) {
        return transactionRepository
                .findByTenantIdAndIdAndInitiatedBy(TenantContext.require(), transactionId, userId)
                .map(TransactionView::of)
                .orElseThrow(() -> new ResourceNotFoundException("Không có giao dịch " + transactionId));
    }

    private AcceptResult acceptOnce(UUID tenantId, UUID userId, String key, String hash,
                                    TransferRequest request) {
        UUID transactionId = UUID.randomUUID();
        String body = acceptedBody(transactionId, request);

        int claimed = idempotencyRepository.claim(tenantId, userId, key, hash, transactionId,
                HttpStatus.ACCEPTED.value(), body, keyTtl.toSeconds());
        if (claimed == 0) {
            // Một request cùng key đã commit trong lúc ta kiểm tra ví. Không ghi gì — trả lời của nó.
            IdempotencyRecord winner = idempotencyRepository.findLive(tenantId, userId, key)
                    .orElseThrow(() -> new IllegalStateException("Mất câu trả lời của key " + key));
            return replay(winner, hash);
        }

        transactionRepository.insertTransfer(transactionId, tenantId, userId, request.fromWalletId(),
                request.toWalletId(), request.amountMinor(), request.currency().name(), request.note());

        UUID commandEventId = UUID.randomUUID();
        transactionRepository.insertSagaStart(transactionId, tenantId, commandEventId, sagaDeadline.toSeconds());

        // Key = ví bị ghi nợ: mọi lệnh của một ví đi cùng partition, đúng thứ tự (WalletTopics)
        outboxPublisher.publish(
                WalletTopics.CMD_POST_JOURNAL,
                request.fromWalletId().toString(),
                commandEventId,
                "PostJournal",
                new PostJournalCommand(transactionId, EJournalType.TRANSFER, null, request.currency(),
                        List.of(new Leg(request.fromWalletId().toString(), -request.amountMinor()),
                                new Leg(request.toWalletId().toString(), request.amountMinor()))));

        log.info("Transaction: nhận lệnh chuyển {} {} từ ví {} sang ví {} (giao dịch {})",
                request.amountMinor(), request.currency(), request.fromWalletId(),
                request.toWalletId(), transactionId);
        return new AcceptResult(HttpStatus.ACCEPTED.value(), body, transactionId, false);
    }

    /**
     * Key đã có câu trả lời. Cùng nội dung → trả lại nguyên văn. Khác nội dung → 422: client đang dùng
     * lại key cho một lệnh KHÁC, đó là bug phía client và phải được báo to — lặng lẽ trả lời của lệnh
     * cũ sẽ khiến client tin lệnh mới đã được nhận.
     */
    private static AcceptResult replay(IdempotencyRecord record, String hash) {
        if (!record.getRequestHash().equals(hash)) {
            throw new AppException(HttpStatus.UNPROCESSABLE_ENTITY, "IDEMPOTENCY_KEY_REUSED",
                    "Idempotency-Key này đã được dùng cho một lệnh khác");
        }
        return new AcceptResult(record.getResponseStatus(), record.getResponseBody(),
                record.getTransactionId(), true);
    }

    private void validateWallets(UUID userId, TransferRequest request) {
        if (request.fromWalletId().equals(request.toWalletId())) {
            throw new BadRequestException("SAME_WALLET", "Ví nguồn và ví đích phải khác nhau");
        }

        // Ví không tồn tại và ví của người khác trả CÙNG một lỗi: phân biệt hai trường hợp là cho
        // phép dò xem một wallet id có tồn tại hay không.
        WalletInfo source = walletDirectory.find(request.fromWalletId())
                .filter(wallet -> wallet.userId().equals(userId))
                .orElseThrow(() -> new ResourceNotFoundException("Không có ví nguồn " + request.fromWalletId()));
        if (source.currency() != request.currency()) {
            throw unprocessable("CURRENCY_MISMATCH", "Ví nguồn không dùng loại tiền " + request.currency());
        }
        if (source.status() != EWalletStatus.ACTIVE) {
            throw unprocessable("WALLET_NOT_ACTIVE", "Ví nguồn đang ở trạng thái " + source.status());
        }

        WalletInfo target = walletDirectory.find(request.toWalletId())
                .orElseThrow(() -> unprocessable("RECIPIENT_NOT_FOUND", "Không có ví nhận " + request.toWalletId()));
        if (target.currency() != request.currency()) {
            throw unprocessable("CURRENCY_MISMATCH", "Ví nhận không dùng loại tiền " + request.currency());
        }
        // Ví nhận bị ĐÓNG BĂNG vẫn nhận được tiền (ADR-011) — chỉ chặn ví chưa mở xong hoặc đã đóng.
        // Không nói lý do cụ thể: trạng thái ví của người khác không phải việc của người gửi.
        if (target.status() == EWalletStatus.PENDING || target.status() == EWalletStatus.CLOSED) {
            throw unprocessable("RECIPIENT_UNAVAILABLE", "Ví nhận hiện không nhận được tiền");
        }
    }

    private String acceptedBody(UUID transactionId, TransferRequest request) {
        TransferAccepted accepted = new TransferAccepted(transactionId, ETransactionStatus.PROCESSING,
                request.fromWalletId(), request.toWalletId(), request.amountMinor(), request.currency(),
                Instant.now());
        BaseResponse<TransferAccepted> response = BaseResponse.<TransferAccepted>builder()
                .success(true)
                .status(HttpStatus.ACCEPTED.value())
                .message("Đã nhận lệnh chuyển tiền")
                .data(accepted)
                .build();
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Không serialize được câu trả lời", e);
        }
    }

    private static String requireValidKey(String key) {
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
     * SHA-256 của nội dung lệnh ở dạng CHUẨN HOÁ — không phụ thuộc thứ tự field hay khoảng trắng của
     * JSON, nên hai lần gửi cùng một lệnh luôn cho cùng một hash. {@code note} đứng cuối vì là trường
     * tự do duy nhất: mọi trường trước nó có định dạng cố định, nên không có cách ghép nào nhập nhằng.
     */
    static String requestHash(TransferRequest request) {
        String canonical = String.join("\n",
                request.fromWalletId().toString(),
                request.toWalletId().toString(),
                Long.toString(request.amountMinor()),
                request.currency().name(),
                request.note() == null ? "" : request.note());
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
