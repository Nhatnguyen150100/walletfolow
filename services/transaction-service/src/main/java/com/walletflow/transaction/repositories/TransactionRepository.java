package com.walletflow.transaction.repositories;

import com.walletflow.transaction.entity.Transaction;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

/**
 * Không có {@code save}/{@code delete} tuỳ ý: hàng chỉ ra đời trong transaction nhận lệnh, và trạng
 * thái chỉ đổi qua các câu UPDATE có điều kiện của saga (1.2b).
 */
public interface TransactionRepository extends Repository<Transaction, UUID> {

    /** Chỉ người tạo lệnh xem được lệnh — người khác (kể cả cùng tenant) nhận 404 như không tồn tại. */
    Optional<Transaction> findByTenantIdAndIdAndInitiatedBy(UUID tenantId, UUID id, UUID initiatedBy);

    Optional<Transaction> findByTenantIdAndId(UUID tenantId, UUID id);

    /**
     * Khoá hàng giao dịch trước khi saga quyết định chuyển trạng thái. Hai event của cùng một giao
     * dịch (posted và rejected đến từ hai topic, không có thứ tự) bị tuần tự hoá ở đây: bên đến sau
     * nhìn thấy trạng thái bên đến trước đã ghi.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT t FROM Transaction t WHERE t.tenantId = :tenantId AND t.id = :id")
    Optional<Transaction> lockForSaga(@Param("tenantId") UUID tenantId, @Param("id") UUID id);

    /**
     * Chuyển trạng thái. Người gọi đã khoá hàng và đã quyết định theo máy trạng thái của
     * {@code SagaService} — câu này chỉ ghi kết quả.
     */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE transactions
               SET status = :status,
                   -- CAST: hai tham số này thường null, và null không rõ kiểu có thể bị bind thành bytea
                   failure_code = CAST(:failureCode AS VARCHAR),
                   failure_reason = CAST(:failureReason AS TEXT),
                   updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id
            """, nativeQuery = true)
    int transition(@Param("tenantId") UUID tenantId,
                   @Param("id") UUID id,
                   @Param("status") String status,
                   @Param("failureCode") String failureCode,
                   @Param("failureReason") String failureReason);

    @Modifying
    @Query(value = """
            INSERT INTO transactions
                   (id, tenant_id, type, status, initiated_by, from_wallet, to_wallet,
                    amount_minor, currency, note)
            VALUES (:id, :tenantId, 'TRANSFER', 'PROCESSING', :initiatedBy, :fromWallet, :toWallet,
                    :amountMinor, :currency, CAST(:note AS VARCHAR))
            """, nativeQuery = true)
    // CAST tường minh: note thường là null, và Hibernate bind một tham số null không rõ kiểu có thể
    // thành bytea -> Postgres từ chối "column note is of type character varying".
    int insertTransfer(@Param("id") UUID id,
                       @Param("tenantId") UUID tenantId,
                       @Param("initiatedBy") UUID initiatedBy,
                       @Param("fromWallet") UUID fromWallet,
                       @Param("toWallet") UUID toWallet,
                       @Param("amountMinor") long amountMinor,
                       @Param("currency") String currency,
                       @Param("note") String note);

    /**
     * Lệnh nạp tiền. Không có ví nguồn (CHECK của V3 bắt buộc null) và không có note: nội dung lệnh do
     * người dùng nhập không đi kèm luồng tiền từ ngân hàng.
     */
    @Modifying
    @Query(value = """
            INSERT INTO transactions
                   (id, tenant_id, type, status, initiated_by, from_wallet, to_wallet,
                    amount_minor, currency)
            VALUES (:id, :tenantId, 'TOPUP', 'PROCESSING', :initiatedBy, NULL, :toWallet,
                    :amountMinor, :currency)
            """, nativeQuery = true)
    int insertTopup(@Param("id") UUID id,
                    @Param("tenantId") UUID tenantId,
                    @Param("initiatedBy") UUID initiatedBy,
                    @Param("toWallet") UUID toWallet,
                    @Param("amountMinor") long amountMinor,
                    @Param("currency") String currency);

    /**
     * Ghi phiên thanh toán và chuyển sang AWAITING_PAYMENT. Điều kiện {@code status = 'PROCESSING'} là
     * phần quan trọng: {@code BankChargePending} và {@code BankCompleted} đi trên HAI topic nên không
     * có thứ tự — người dùng trả tiền ngay có thể khiến kết quả cuối tới trước lời báo "đã tạo phiên".
     * Không có điều kiện này, một event đến muộn kéo giao dịch đã POSTING/COMPLETED ngược về phòng chờ.
     *
     * <p>Vẫn ghi phiên (provider/ref/url) kể cả khi không còn ở PROCESSING thì sai: url của một phiên
     * đã xong không có ích, còn provider_ref thì chính đường kết quả cuối đã mang theo.
     */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE transactions
               SET status       = 'AWAITING_PAYMENT',
                   provider     = :provider,
                   provider_ref = :providerRef,
                   payment_url  = :paymentUrl,
                   updated_at   = now()
             WHERE tenant_id = :tenantId AND id = :id AND status = 'PROCESSING'
            """, nativeQuery = true)
    int markAwaitingPayment(@Param("tenantId") UUID tenantId,
                            @Param("id") UUID id,
                            @Param("provider") String provider,
                            @Param("providerRef") String providerRef,
                            @Param("paymentUrl") String paymentUrl);

    /**
     * Provider đã xác nhận thu được tiền: sang POSTING và ghi lại mã tham chiếu của lần thu. Ghi đè
     * {@code provider_ref} ở đây là cố ý — kết quả cuối là nguồn chính xác hơn lời báo tạo phiên, và
     * với ca kết quả tới TRƯỚC thì đây là lần đầu ta biết mã.
     *
     * <p>Điều kiện trạng thái là thừa khi người gọi đã khoá hàng và đã đi qua máy trạng thái, nhưng nó
     * miễn phí và khiến câu UPDATE này không thể kéo một giao dịch đã đóng ngược về POSTING dù có bị
     * gọi nhầm chỗ về sau.
     */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE transactions
               SET status       = 'POSTING',
                   provider     = :provider,
                   provider_ref = :providerRef,
                   updated_at   = now()
             WHERE tenant_id = :tenantId AND id = :id
               AND status IN ('PROCESSING', 'AWAITING_PAYMENT')
            """, nativeQuery = true)
    int markPosting(@Param("tenantId") UUID tenantId,
                    @Param("id") UUID id,
                    @Param("provider") String provider,
                    @Param("providerRef") String providerRef);

    /**
     * Bước đầu tiên của saga, ghi CÙNG transaction với lệnh: crash ngay sau commit thì job quét saga
     * treo (1.2b) vẫn biết có một lệnh đang chờ, với hạn chót cụ thể.
     *
     * @param step bước đầu của loại giao dịch này: {@code POST_JOURNAL} với chuyển tiền (ghi sổ ngay),
     *             {@code BANK_CHARGE} với nạp tiền (phải thu được tiền thật rồi mới ghi sổ)
     */
    @Modifying
    @Query(value = """
            INSERT INTO saga_state
                   (transaction_id, tenant_id, current_step, step_status, attempt_count,
                    command_event_id, deadline_at)
            VALUES (:transactionId, :tenantId, :step, 'SENT', 1,
                    :commandEventId, now() + make_interval(secs => :deadlineSeconds))
            """, nativeQuery = true)
    int insertSagaStart(@Param("transactionId") UUID transactionId,
                        @Param("tenantId") UUID tenantId,
                        @Param("step") String step,
                        @Param("commandEventId") UUID commandEventId,
                        @Param("deadlineSeconds") long deadlineSeconds);
}
