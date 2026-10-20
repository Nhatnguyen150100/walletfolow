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
     * Tra lần thu từ mã tham chiếu của provider — đường DUY NHẤT suy ra tenant của một webhook, vì webhook
     * tới từ Internet và tenant do bên ngoài gửi thì không bao giờ được tin (ADR-018 §6).
     *
     * <p>Đây là query DUY NHẤT trên bảng này không lọc tenant, và là ngoại lệ có chủ ý: nó CHÍNH LÀ bước suy
     * ra tenant. {@code uk_provider_txn_provider_ref} là UNIQUE {@code (provider, provider_ref)} nên cặp này
     * xác định đúng một hàng. Caller phải đặt {@code TenantContext} theo tenant ĐỌC ĐƯỢC từ hàng trước khi
     * làm bất cứ việc gì tiếp theo; mọi query sau đó lọc tenant như thường.
     *
     * <p>Ràng {@code operation} để một IPN của lần HOÀN TIỀN (2.4) không bị dẫn vào đường của lần thu — cùng
     * một {@code provider_ref} có thể xuất hiện ở nhiều thao tác.
     */
    Optional<ProviderTransaction> findByProviderAndProviderRefAndOperation(
            String provider, String providerRef, EBankOperation operation);

    /**
     * Kéo lần hỏi lại kế tiếp về ngay bây giờ. Dùng khi có tín hiệu người dùng vừa trả tiền xong (trang
     * quay về, 2.3b): nhịp backoff lúc đó có thể đang ở 10 phút, chờ hết nhịp là bắt người dùng nhìn màn
     * hình "đang xử lý" thêm 10 phút cho một thứ đã xong.
     *
     * <p>Chỉ là GỢI Ý về thời điểm hỏi — không đổi trạng thái, không đổi {@code check_attempts}, nên gọi
     * bao nhiêu lần cũng vô hại và không bao giờ tự nó ghi tiền (ADR-016 §1).
     *
     * <p>Lọc tenant dù caller đã cầm sẵn hàng: đây là đường ghi từ một endpoint CÔNG KHAI, và bất biến
     * "mọi query lọc theo tenant" (ADR-010) tồn tại đúng để chỗ như thế này không trở thành ghi chéo tenant
     * vào ngày ai đó nới khoá unique trên {@code provider_ref}.
     *
     * @return 1 nếu vừa kéo lịch, 0 nếu hàng không còn PENDING (đã chốt rồi) hoặc đã tới hạn sẵn
     */
    @Modifying
    @Query(value = """
            UPDATE provider_transactions
               SET next_check_at = now(), updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id AND status = 'PENDING'
               AND next_check_at > now()
            """, nativeQuery = true)
    int expediteCheck(@Param("tenantId") UUID tenantId, @Param("id") UUID id);

    /**
     * Giành quyền thu cho một giao dịch. Hàng đã có (lệnh gửi lại, hai consumer đua nhau) thì không đụng
     * gì — Postgres bắt câu INSERT thứ hai chờ ở unique key tới khi câu thứ nhất commit, nên không bao giờ
     * có hai hàng, và không có lần nào vỡ unique key thành lỗi.
     *
     * <p><b>{@code ON CONFLICT DO NOTHING} không chỉ định khoá — cố ý.</b> Hai khoá unique luôn trùng nhau theo
     * dữ liệu: {@code (tenant, giao dịch, thao tác)} và {@code (provider, idempotency_key)} (khoá sinh tất định từ
     * bộ ba kia). Chỉ định một khoá thì chỉ khoá đó (arbiter) được xử lý "không làm gì"; khoá unique khác được
     * kiểm như INSERT thường — luồng thua chờ luồng thắng commit rồi nổ {@code 23505}. Lệnh trùng khi đó không
     * nhận được câu trả lời mà bị thử lại rồi vào DLT (từng làm đỏ master, {@code ChargeIdempotencyIT}). Không
     * chỉ định thì xung đột ở khoá nào cũng là "đã có".
     *
     * <p>Không nuốt nhầm gì: xung đột thật chỉ ở {@code idempotency_key} với bộ ba khác là không thể (khoá băm từ
     * chính bộ ba); {@code uk_provider_txn_provider_ref} không bao giờ đụng lúc INSERT ({@code provider_ref}
     * NULL, NULLS DISTINCT); {@code id} ngẫu nhiên. Nếu một xung đột lạ xảy ra, {@code claim} đọc lại theo bộ ba
     * không thấy hàng → lỗi kỹ thuật → DLT (fail-closed), không bao giờ thu hai lần. <b>Thêm unique index mới vào
     * bảng này thì nó cũng bị "nuốt" theo cách đó — cân nhắc lại câu này khi thêm.</b>
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
            ON CONFLICT DO NOTHING
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
     * <p>Bộ đếm lượt hỏi xác nhận nằm ở cột RIÊNG {@code confirm_attempts} (V3), mặc định 0, nên không phải
     * đặt lại gì ở đây: {@code check_attempts} giữ nguyên nghĩa "số lần đã hỏi lúc còn PENDING".
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

    /**
     * Lùi lần hỏi xác nhận — gọi ngay khi lấy lô; provider không trả lời thì lần sau hỏi lại, nhịp giãn theo
     * cấp số như lô PENDING ({@code baseSeconds × 2^confirm_attempts}, trần {@code maxSeconds}). Tiền về muộn
     * do ngân hàng chốt sổ chậm có thể tới sau nhiều giờ, nên cửa sổ hỏi phải rộng hơn nhịp PENDING rất nhiều.
     *
     * <p>Vế phải của SET đọc {@code confirm_attempts} CŨ.
     */
    @Modifying
    @Query(value = """
            UPDATE provider_transactions
               SET next_check_at = now() + make_interval(secs => LEAST(:maxSeconds,
                                        :baseSeconds * power(2, LEAST(confirm_attempts, 30)))),
                   confirm_attempts = confirm_attempts + 1, updated_at = now()
             WHERE id IN (:ids) AND status = 'FAILED' AND failure_code = :expiredCode
            """, nativeQuery = true)
    int deferExpiredConfirmations(@Param("ids") Collection<UUID> ids,
                                  @Param("expiredCode") String expiredCode,
                                  @Param("baseSeconds") long baseSeconds,
                                  @Param("maxSeconds") long maxSeconds);

    /**
     * Hàng đã dùng hết hạn mức hỏi xác nhận MÀ VẪN chưa có câu trả lời: {@code next_check_at} về NULL, thôi
     * hỏi vĩnh viễn. Không có trần thì một hàng provider không bao giờ trả lời được sẽ bị hỏi lại mỗi
     * {@code max-confirm-recheck-after} mãi mãi — mà nó đã là {@code FAILED} nên {@link #countOverdue} không
     * đếm và không alert nào kêu.
     *
     * <p>Gọi SAU khi đã hỏi xong cả lô, và chỉ chạm hàng còn {@code next_check_at}: hàng vừa nhận được câu
     * trả lời trong chính lượt này đã được {@link #finishExpiredConfirmation} đưa về NULL nên KHÔNG bị tính
     * là "không xác nhận được". Đây là ca biên thật: câu trả lời tới đúng ở lượt chót.
     *
     * @return số hàng vừa hết lượt; caller cộng vào counter {@code walletflow.bank.expired_unconfirmed}
     */
    @Modifying
    @Query(value = """
            UPDATE provider_transactions
               SET next_check_at = NULL, updated_at = now()
             WHERE id IN (:ids) AND status = 'FAILED' AND failure_code = :expiredCode
               AND next_check_at IS NOT NULL AND confirm_attempts >= :maxAttempts
            """, nativeQuery = true)
    int capExpiredConfirmations(@Param("ids") Collection<UUID> ids,
                                @Param("expiredCode") String expiredCode,
                                @Param("maxAttempts") int maxAttempts);

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
