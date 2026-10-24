package com.walletflow.transaction.repositories;

import com.walletflow.transaction.entity.Transaction;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.List;
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
     * Hàng đợi cần người can thiệp (roadmap 2.5): những giao dịch mà máy đã dừng lại và chờ người,
     * cộng những giao dịch máy đang tự xử lý nhưng người trực vẫn cần nhìn thấy
     * ({@code REFUNDING} — đứng quá lâu ở đó là một sự cố, runbook topup-stuck mục E).
     *
     * <p>Cũ nhất TRƯỚC, không phải mới nhất: ở đây mỗi phút trôi qua là một phút khách hàng mất
     * tiền mà không có gì, nên thứ tự đọc đúng của hàng đợi là thứ tự nó phải được xử lý. Chốt thêm
     * bằng {@code id} để trang kế tiếp không nhảy cóc khi hai hàng cùng {@code created_at}.
     *
     * <p>Lọc tenant như mọi query khác (ADR-010): người vận hành của tenant này không nhìn thấy ca
     * của tenant khác.
     *
     * <p><b>Tập trạng thái là HẰNG trong câu lệnh, không phải tham số</b> — cùng lý do với
     * {@code findChargeByProviderRef} của bank-adapter (ADR-020 *Amendments* B1). Index riêng phần
     * {@code idx_transactions_needs_review} (V5) chỉ dùng được khi Postgres chứng minh được mệnh đề
     * WHERE của câu lệnh hàm ý mệnh đề của index; với tham số bind ({@code status IN ?}) nó chứng
     * minh được ở kế hoạch *custom* nhưng KHÔNG ở *generic*, mà pgjdbc tự chuyển sang server-side
     * prepared sau vài lần gọi trên cùng connection. Hậu quả sẽ là quét toàn bảng
     * {@code transactions} — bảng chỉ mọc thêm — mà không có gì báo.
     *
     * <p>Hệ quả: tập trạng thái này sống ở ĐÚNG hai chỗ và chúng phải khớp nhau — câu lệnh ở đây và
     * mệnh đề của index trong V5. Thêm một trạng thái vào hàng đợi là một migration, không phải một
     * dòng Java.
     */
    @Query(value = """
            SELECT * FROM transactions
             WHERE tenant_id = :tenantId AND status IN ('MANUAL_REVIEW', 'REFUNDING')
             ORDER BY created_at ASC, id ASC
             LIMIT :limit
            """, nativeQuery = true)
    List<Transaction> findNeedingReview(@Param("tenantId") UUID tenantId, @Param("limit") int limit);

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

    /**
     * Chốt một ca cần can thiệp bằng tay (2.5 PR2): đặt trạng thái cuối VÀ đóng dấu
     * {@code resolved_at} trong cùng một câu.
     *
     * <p>Điều kiện {@code status = 'MANUAL_REVIEW' AND resolved_at IS NULL} là hàng rào chống hai
     * người cùng chốt một ca, và chống chốt một ca mà saga đã tự xử lý xong trong lúc người vận hành
     * đang gõ. Trả 0 ⇒ người gọi phải từ chối, không được ghi đè.
     *
     * @return 1 nếu vừa chốt, 0 nếu ca đã đổi trạng thái hoặc đã có người chốt trước
     */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE transactions
               SET status = :status,
                   failure_code = CAST(:failureCode AS VARCHAR),
                   failure_reason = CAST(:failureReason AS TEXT),
                   resolved_at = now(),
                   updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id
               AND status = 'MANUAL_REVIEW' AND resolved_at IS NULL
            """, nativeQuery = true)
    int resolveManualReview(@Param("tenantId") UUID tenantId,
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
