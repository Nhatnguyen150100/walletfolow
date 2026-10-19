package com.walletflow.bank.repositories;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.common.event.EBankOperation;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Mọi chuyển trạng thái là một câu UPDATE có điều kiện trên trạng thái NGUỒN và trả về số hàng đổi:
 * 1 = luồng này thắng, 0 = luồng khác đã chuyển trước. Không có đọc-rồi-ghi nào để hai luồng cùng thắng.
 */
public interface ProviderTransactionRepository extends Repository<ProviderTransaction, UUID> {

    Optional<ProviderTransaction> findByTenantIdAndId(UUID tenantId, UUID id);

    Optional<ProviderTransaction> findByTenantIdAndTransactionIdAndOperation(
            UUID tenantId, UUID transactionId, EBankOperation operation);

    /**
     * Giành quyền thu cho một giao dịch. Hàng đã có (lệnh gửi lại, hai consumer đua nhau) thì không đụng
     * gì — Postgres bắt câu INSERT thứ hai chờ ở unique key tới khi câu thứ nhất commit, nên không bao giờ
     * có hai hàng, và không có lần nào vỡ unique key thành lỗi.
     *
     * @return 1 nếu vừa tạo, 0 nếu đã có
     */
    @Modifying
    @Query(value = """
            INSERT INTO provider_transactions
                   (id, tenant_id, transaction_id, operation, provider, provider_account, idempotency_key,
                    wallet_id, amount_minor, currency, status, raw_request, created_at, updated_at)
            VALUES (:id, :tenantId, :transactionId, :operation, :provider, :providerAccount, :idempotencyKey,
                    :walletId, :amountMinor, :currency, 'NEW', CAST(:rawRequest AS jsonb), now(), now())
            ON CONFLICT (tenant_id, transaction_id, operation) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("id") UUID id,
                       @Param("tenantId") UUID tenantId,
                       @Param("transactionId") UUID transactionId,
                       @Param("operation") String operation,
                       @Param("provider") String provider,
                       @Param("providerAccount") String providerAccount,
                       @Param("idempotencyKey") String idempotencyKey,
                       @Param("walletId") UUID walletId,
                       @Param("amountMinor") long amountMinor,
                       @Param("currency") String currency,
                       @Param("rawRequest") String rawRequest);

    /**
     * NEW -> PENDING: provider đã tạo phiên. Lần hỏi lại trạng thái đầu tiên sau {@code recheckSeconds}; bộ
     * đếm backoff bắt đầu từ 0. Mốc tạo phiên và hạn phiên lấy từ câu trả lời của provider.
     */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE provider_transactions
               SET status = 'PENDING', provider_ref = :providerRef, payment_url = :paymentUrl,
                   raw_response = CAST(:rawResponse AS jsonb),
                   provider_created_at = :providerCreatedAt, expires_at = :expiresAt, check_attempts = 0,
                   next_check_at = now() + make_interval(secs => :recheckSeconds), updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id AND status = 'NEW'
            """, nativeQuery = true)
    int markPending(@Param("tenantId") UUID tenantId,
                    @Param("id") UUID id,
                    @Param("providerRef") String providerRef,
                    @Param("paymentUrl") String paymentUrl,
                    @Param("rawResponse") String rawResponse,
                    @Param("recheckSeconds") long recheckSeconds,
                    @Param("providerCreatedAt") Instant providerCreatedAt,
                    @Param("expiresAt") Instant expiresAt);

    /** PENDING -> SUCCEEDED: provider xác nhận đã thu được tiền. */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE provider_transactions
               SET status = 'SUCCEEDED', raw_response = CAST(:rawResponse AS jsonb),
                   next_check_at = NULL, updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id AND status = 'PENDING'
            """, nativeQuery = true)
    int markSucceeded(@Param("tenantId") UUID tenantId,
                      @Param("id") UUID id,
                      @Param("rawResponse") String rawResponse);

    /** {@code fromStatus} -> FAILED: provider từ chối (từ NEW) hoặc xác nhận phiên hỏng (từ PENDING). */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE provider_transactions
               SET status = 'FAILED', failure_code = :failureCode,
                   raw_response = CAST(:rawResponse AS jsonb), next_check_at = NULL, updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id AND status = :fromStatus
            """, nativeQuery = true)
    int markFailed(@Param("tenantId") UUID tenantId,
                   @Param("id") UUID id,
                   @Param("fromStatus") String fromStatus,
                   @Param("failureCode") String failureCode,
                   @Param("rawResponse") String rawResponse);

    /**
     * Lô hàng PENDING tới hạn hỏi lại, XUYÊN tenant — job đối chiếu tự đặt tenant theo từng hàng.
     * {@code SKIP LOCKED}: nhiều instance chạy cùng lúc mỗi cái lấy một lô khác nhau.
     *
     * <p>Chỉ hàng của {@code provider} VÀ tài khoản merchant đang chạy: phiên do provider/tài khoản khác tạo
     * (trước khi đổi {@code BANK_PROVIDER} hay {@code VNPAY_TMN_CODE}) phải được hỏi đúng nơi tạo ra nó. Bị
     * bỏ lại, chúng hiện ở gauge quá hạn — không bao giờ bị hiểu "không tìm thấy" thành hết hạn.
     */
    @Query(value = """
            SELECT * FROM provider_transactions
             WHERE status = 'PENDING' AND provider = :provider
               AND (provider_account IS NULL OR provider_account = :account)
               AND next_check_at <= now()
             ORDER BY next_check_at
             LIMIT :limit
               FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<ProviderTransaction> lockDuePendingAcrossTenants(@Param("provider") String provider,
                                                          @Param("account") String account,
                                                          @Param("limit") int limit);

    /**
     * Lùi lần hỏi lại kế tiếp — gọi NGAY khi lấy lô, trước khi gọi provider. Provider lỗi thì hàng đó
     * vẫn đã được lùi, nên không bị hỏi lại liên tục mỗi lượt quét và không chặn các hàng phía sau.
     *
     * <p>Giãn theo cấp số: {@code base × 2^số_lần_đã_hỏi}, trần {@code maxSeconds} (ADR-017). Phiên bị bỏ dở,
     * hàng lệch số tiền, hàng UNRESOLVED không dội provider đều đặn mãi; người dùng vừa trả tiền thì webhook
     * (2.3) gọi hỏi lại ngay, không phải chờ nhịp này. Vế phải của SET đọc {@code check_attempts} CŨ.
     */
    @Modifying
    @Query(value = """
            UPDATE provider_transactions
               SET next_check_at = now() + make_interval(secs => LEAST(:maxSeconds,
                                        :baseSeconds * power(2, LEAST(check_attempts, 30)))),
                   check_attempts = check_attempts + 1, updated_at = now()
             WHERE id IN (:ids) AND status = 'PENDING'
            """, nativeQuery = true)
    int deferChecks(@Param("ids") Collection<UUID> ids,
                    @Param("baseSeconds") long baseSeconds,
                    @Param("maxSeconds") long maxSeconds);

    /**
     * PENDING -> FAILED(failureCode): phiên đã quá hạn + grace, theo ĐỒNG HỒ DB (cùng đồng hồ với mọi mốc
     * khác của hàng). Chỉ được gọi khi provider vừa KHẲNG ĐỊNH chưa thu (ADR-017) — câu này không tự biết
     * điều đó. Hàng không có hạn phiên không bao giờ hết hạn.
     *
     * <p>{@code next_check_at} KHÔNG về NULL: đặt lịch một lần hỏi xác nhận sau {@code confirmAfterSeconds} để
     * phát hiện tiền về muộn (xem {@link #lockDueExpiredConfirmations}).
     *
     * @return 1 nếu vừa hết hạn; 0 nếu chưa tới hạn, hoặc luồng khác đã chuyển hàng trước
     */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE provider_transactions
               SET status = 'FAILED', failure_code = :failureCode,
                   raw_response = CAST(:rawResponse AS jsonb),
                   next_check_at = now() + make_interval(secs => :confirmAfterSeconds), updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id AND status = 'PENDING'
               AND expires_at <= now() - make_interval(secs => :graceSeconds)
            """, nativeQuery = true)
    int markExpired(@Param("tenantId") UUID tenantId,
                    @Param("id") UUID id,
                    @Param("graceSeconds") long graceSeconds,
                    @Param("confirmAfterSeconds") long confirmAfterSeconds,
                    @Param("failureCode") String failureCode,
                    @Param("rawResponse") String rawResponse);

    /**
     * Lô hàng FAILED(EXPIRED) tới hạn hỏi XÁC NHẬN, xuyên tenant, đúng provider + tài khoản. Hết hạn là kết luận
     * dựa trên một câu trả lời "chưa thu" tại một thời điểm; tiền vẫn có thể về muộn (ngân hàng chậm). Lần hỏi
     * này chỉ để PHÁT HIỆN và kêu — trạng thái không bao giờ bị đổi tự động (ADR-017).
     */
    @Query(value = """
            SELECT * FROM provider_transactions
             WHERE status = 'FAILED' AND failure_code = :expiredCode AND provider = :provider
               AND (provider_account IS NULL OR provider_account = :account)
               AND next_check_at <= now()
             ORDER BY next_check_at
             LIMIT :limit
               FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<ProviderTransaction> lockDueExpiredConfirmations(@Param("provider") String provider,
                                                          @Param("account") String account,
                                                          @Param("expiredCode") String expiredCode,
                                                          @Param("limit") int limit);

    /** Lùi lần hỏi xác nhận — gọi ngay khi lấy lô; provider không trả lời thì lần sau hỏi lại. */
    @Modifying
    @Query(value = """
            UPDATE provider_transactions
               SET next_check_at = now() + make_interval(secs => :seconds), updated_at = now()
             WHERE id IN (:ids) AND status = 'FAILED'
            """, nativeQuery = true)
    int deferExpiredConfirmations(@Param("ids") Collection<UUID> ids, @Param("seconds") long seconds);

    /** Đã có câu trả lời xác nhận (dù tốt hay xấu): không hỏi nữa. */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE provider_transactions
               SET next_check_at = NULL, updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id AND status = 'FAILED'
            """, nativeQuery = true)
    int finishExpiredConfirmation(@Param("tenantId") UUID tenantId, @Param("id") UUID id);

    /**
     * Số lần thu PENDING đã quá hạn + grace, XUYÊN tenant và provider. Lẽ ra chúng đã kết thúc — còn lại tức
     * là provider không trả lời được, báo trạng thái lơ lửng (UNRESOLVED), lệch số tiền, hoặc thuộc provider
     * cũ không còn ai hỏi. Mỗi hàng cần người xem (alert PendingChargeOverdue).
     */
    @Query(value = """
            SELECT count(*) FROM provider_transactions
             WHERE status = 'PENDING' AND expires_at <= now() - make_interval(secs => :graceSeconds)
            """, nativeQuery = true)
    long countOverdue(@Param("graceSeconds") long graceSeconds);
}
