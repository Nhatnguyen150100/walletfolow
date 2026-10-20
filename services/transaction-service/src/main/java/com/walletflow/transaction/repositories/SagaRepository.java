package com.walletflow.transaction.repositories;

import com.walletflow.transaction.entity.SagaState;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface SagaRepository extends Repository<SagaState, UUID> {

    Optional<SagaState> findByTenantIdAndTransactionId(UUID tenantId, UUID transactionId);

    /**
     * Khoá hàng saga — LUÔN trước hàng giao dịch, ở mọi đường đi (nhận câu trả lời của ledger lẫn job
     * quét). Thứ tự khoá cố định là điều kiện để không deadlock; và vì job quét dùng
     * {@code SKIP LOCKED}, saga đang được trả lời sẽ bị job quét BỎ QUA thay vì bị gửi lại.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM SagaState s WHERE s.tenantId = :tenantId AND s.transactionId = :transactionId")
    Optional<SagaState> lock(@Param("tenantId") UUID tenantId, @Param("transactionId") UUID transactionId);

    /**
     * Giành MỘT saga đã quá hạn để xử lý — ở MỌI tenant, khoá hàng tới hết transaction.
     *
     * <p>{@code SKIP LOCKED}: nhiều instance chạy job quét cùng lúc thì mỗi saga chỉ rơi vào tay MỘT
     * instance — không gửi lại hai lần, và không instance nào đứng chờ instance kia. Mỗi lần gọi chỉ
     * lấy một hàng để mỗi saga có transaction riêng: một saga lỗi không kéo cả lô rollback theo.
     */
    @Query(value = """
            SELECT * FROM saga_state
             WHERE step_status = 'SENT' AND deadline_at < now()
             ORDER BY deadline_at
             LIMIT 1
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    Optional<SagaState> lockNextOverdueAcrossTenants();

    /**
     * Ghi nhận một lần gửi lại: eventId MỚI, đếm lần thử, hạn chót mới. Điều kiện
     * {@code step_status = 'SENT'} để không bao giờ "hồi sinh" một saga đã đóng.
     */
    @Modifying
    @Query(value = """
            UPDATE saga_state
               SET command_event_id = :eventId,
                   attempt_count    = attempt_count + 1,
                   deadline_at      = now() + make_interval(secs => :deadlineSeconds),
                   updated_at       = now()
             WHERE transaction_id = :transactionId AND step_status = 'SENT'
            """, nativeQuery = true)
    int recordRetry(@Param("transactionId") UUID transactionId,
                    @Param("eventId") UUID eventId,
                    @Param("deadlineSeconds") long deadlineSeconds);

    /**
     * Chuyển saga sang bước kế tiếp: eventId MỚI, hạn chót mới, đếm lần thử về 1 (bước mới có ngân
     * sách thử lại của riêng nó). Dùng cho nạp tiền khi provider đã xác nhận thu được tiền:
     * {@code BANK_CHARGE -> POST_JOURNAL}.
     *
     * <p>Điều kiện {@code current_step = :fromStep AND step_status = 'SENT'} khiến câu này chỉ ăn MỘT
     * lần: {@code BankCompleted} lặp lại gặp bước đã đổi và không làm gì, nên không có lệnh ghi sổ thứ
     * hai nào được xếp vào outbox.
     */
    @Modifying
    @Query(value = """
            UPDATE saga_state
               SET current_step     = :toStep,
                   command_event_id = :eventId,
                   attempt_count    = 1,
                   deadline_at      = now() + make_interval(secs => :deadlineSeconds),
                   updated_at       = now()
             WHERE tenant_id = :tenantId AND transaction_id = :transactionId
               AND current_step = :fromStep AND step_status = 'SENT'
            """, nativeQuery = true)
    int advanceStep(@Param("tenantId") UUID tenantId,
                    @Param("transactionId") UUID transactionId,
                    @Param("fromStep") String fromStep,
                    @Param("toStep") String toStep,
                    @Param("eventId") UUID eventId,
                    @Param("deadlineSeconds") long deadlineSeconds);

    /**
     * Đẩy hạn chót ra sau mà KHÔNG gửi lại gì. Hai chỗ dùng: saga xử lý lỗi (để nó không chặn đầu hàng
     * đợi của job quét) và lệnh nạp vừa vào phòng chờ (hạn chót phải phủ được quãng người dùng ngồi
     * trả tiền, nếu không job quét coi nó là treo).
     */
    @Modifying
    @Query(value = """
            UPDATE saga_state
               SET deadline_at = now() + make_interval(secs => :delaySeconds), updated_at = now()
             WHERE transaction_id = :transactionId AND step_status = 'SENT'
            """, nativeQuery = true)
    int postpone(@Param("transactionId") UUID transactionId, @Param("delaySeconds") long delaySeconds);

    /** Đóng saga: DONE | FAILED | MANUAL_REVIEW. */
    @Modifying
    @Query(value = """
            UPDATE saga_state SET step_status = :status, updated_at = now()
             WHERE transaction_id = :transactionId
            """, nativeQuery = true)
    int close(@Param("transactionId") UUID transactionId, @Param("status") String status);
}
