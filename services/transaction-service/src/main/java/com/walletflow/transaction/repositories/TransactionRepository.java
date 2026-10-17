package com.walletflow.transaction.repositories;

import com.walletflow.transaction.entity.Transaction;
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
     * Bước đầu tiên của saga, ghi CÙNG transaction với lệnh: crash ngay sau commit thì job quét saga
     * treo (1.2b) vẫn biết có một lệnh đang chờ ledger, với hạn chót cụ thể.
     */
    @Modifying
    @Query(value = """
            INSERT INTO saga_state
                   (transaction_id, tenant_id, current_step, step_status, attempt_count,
                    command_event_id, deadline_at)
            VALUES (:transactionId, :tenantId, 'POST_JOURNAL', 'SENT', 1,
                    :commandEventId, now() + make_interval(secs => :deadlineSeconds))
            """, nativeQuery = true)
    int insertSagaStart(@Param("transactionId") UUID transactionId,
                        @Param("tenantId") UUID tenantId,
                        @Param("commandEventId") UUID commandEventId,
                        @Param("deadlineSeconds") long deadlineSeconds);
}
