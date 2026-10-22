package com.walletflow.ledger.repositories;

import com.walletflow.ledger.entity.RejectedTopup;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

/**
 * Hàng rào lệnh nạp đã bị từ chối (ADR-020). APPEND-ONLY như {@link JournalEntryRepository}: chỉ liệt kê đúng
 * thao tác cần dùng, không lộ delete — gỡ hàng rào là mở đường cho người dùng nhận tiền hai lần.
 */
public interface RejectedTopupRepository extends Repository<RejectedTopup, RejectedTopup.Key> {

    @Query(value = """
            SELECT * FROM rejected_topups
             WHERE tenant_id = :tenantId AND transaction_id = :transactionId
            """, nativeQuery = true)
    Optional<RejectedTopup> find(@Param("tenantId") UUID tenantId, @Param("transactionId") UUID transactionId);

    /**
     * Dựng hàng rào. Đã có (lần từ chối lại) thì giữ nguyên mã của LẦN ĐẦU — caller đọc lại để phát đúng mã đó.
     *
     * @return 1 nếu vừa dựng, 0 nếu đã có
     */
    @Modifying
    @Query(value = """
            INSERT INTO rejected_topups (tenant_id, transaction_id, reason_code, reason, rejected_at)
            VALUES (:tenantId, :transactionId, :reasonCode, :reason, now())
            ON CONFLICT (tenant_id, transaction_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("tenantId") UUID tenantId,
                       @Param("transactionId") UUID transactionId,
                       @Param("reasonCode") String reasonCode,
                       @Param("reason") String reason);

    /**
     * Khoá theo giao dịch nạp, tới hết transaction hiện tại ({@code pg_advisory_xact_lock}). Là điểm gặp DUY NHẤT
     * giữa đường ghi sổ và đường dựng hàng rào: đường từ chối không khoá tài khoản nào, nên không có nó thì một
     * lệnh ghi sổ (consumer A) và một lời từ chối cùng giao dịch (consumer B, lúc rebalance) có thể cùng commit
     * — đúng ca "vừa có bút toán vừa có hàng rào" mà ADR-020 cấm. Đụng hash giữa hai giao dịch khác nhau chỉ làm
     * chúng chờ nhau một nhịp, không sai.
     *
     * @return luôn 1 (câu SELECT cần trả về một giá trị)
     */
    @Query(value = """
            SELECT count(*) FROM (SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))) AS locked
            """, nativeQuery = true)
    long lock(@Param("key") String key);
}
