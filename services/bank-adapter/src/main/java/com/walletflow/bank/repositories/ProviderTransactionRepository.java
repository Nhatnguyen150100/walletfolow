package com.walletflow.bank.repositories;

import com.walletflow.bank.entity.ProviderTransaction;
import com.walletflow.common.event.EBankOperation;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

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
                   (id, tenant_id, transaction_id, operation, provider, idempotency_key,
                    wallet_id, amount_minor, currency, status, raw_request, created_at, updated_at)
            VALUES (:id, :tenantId, :transactionId, :operation, :provider, :idempotencyKey,
                    :walletId, :amountMinor, :currency, 'NEW', CAST(:rawRequest AS jsonb), now(), now())
            ON CONFLICT (tenant_id, transaction_id, operation) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("id") UUID id,
                       @Param("tenantId") UUID tenantId,
                       @Param("transactionId") UUID transactionId,
                       @Param("operation") String operation,
                       @Param("provider") String provider,
                       @Param("idempotencyKey") String idempotencyKey,
                       @Param("walletId") UUID walletId,
                       @Param("amountMinor") long amountMinor,
                       @Param("currency") String currency,
                       @Param("rawRequest") String rawRequest);

    /** NEW -> PENDING: provider đã tạo phiên. Lần hỏi lại trạng thái đầu tiên sau {@code recheckSeconds}. */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE provider_transactions
               SET status = 'PENDING', provider_ref = :providerRef, payment_url = :paymentUrl,
                   raw_response = CAST(:rawResponse AS jsonb),
                   next_check_at = now() + make_interval(secs => :recheckSeconds), updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id AND status = 'NEW'
            """, nativeQuery = true)
    int markPending(@Param("tenantId") UUID tenantId,
                    @Param("id") UUID id,
                    @Param("providerRef") String providerRef,
                    @Param("paymentUrl") String paymentUrl,
                    @Param("rawResponse") String rawResponse,
                    @Param("recheckSeconds") long recheckSeconds);

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
     * <p>Chỉ hàng của {@code provider} đang chạy: phiên do provider khác tạo (trước khi đổi
     * {@code BANK_PROVIDER}) phải được hỏi đúng provider đó, không phải provider hiện tại.
     */
    @Query(value = """
            SELECT * FROM provider_transactions
             WHERE status = 'PENDING' AND provider = :provider AND next_check_at <= now()
             ORDER BY next_check_at
             LIMIT :limit
               FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<ProviderTransaction> lockDuePendingAcrossTenants(@Param("provider") String provider,
                                                          @Param("limit") int limit);

    /**
     * Lùi lần hỏi lại kế tiếp — gọi NGAY khi lấy lô, trước khi gọi provider. Provider lỗi thì hàng đó
     * vẫn đã được lùi, nên không bị hỏi lại liên tục mỗi lượt quét và không chặn các hàng phía sau.
     */
    @Modifying
    @Query(value = """
            UPDATE provider_transactions
               SET next_check_at = now() + make_interval(secs => :recheckSeconds), updated_at = now()
             WHERE id IN (:ids) AND status = 'PENDING'
            """, nativeQuery = true)
    int deferChecks(@Param("ids") Collection<UUID> ids, @Param("recheckSeconds") long recheckSeconds);
}
