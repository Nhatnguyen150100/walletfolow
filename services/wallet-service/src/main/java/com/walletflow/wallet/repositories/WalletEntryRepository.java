package com.walletflow.wallet.repositories;

import com.walletflow.wallet.entity.WalletEntry;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface WalletEntryRepository extends Repository<WalletEntry, Long> {

    /**
     * Ghi một bút toán vào lịch sử của ví — và đây CHÍNH LÀ bước khử trùng (V1): trả 1 thì đây là
     * lần đầu bút toán này chạm ví, phải cộng số dư; trả 0 thì đã áp rồi, bỏ qua.
     *
     * <p>Không "tìm rồi mới chèn": hai luồng nhận cùng event song song sẽ cùng thấy "chưa có". Ở đây
     * Postgres bắt luồng thứ hai chờ luồng đầu commit rồi trả 0 cho nó.
     */
    @Modifying
    @Query(value = """
            INSERT INTO wallet_entries
                   (tenant_id, wallet_id, entry_id, transaction_id, type, amount_minor, posted_at)
            VALUES (:tenantId, :walletId, :entryId, :transactionId, :type, :amount, :postedAt)
            ON CONFLICT ON CONSTRAINT uk_wallet_entries_wallet_entry DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("tenantId") UUID tenantId,
                       @Param("walletId") UUID walletId,
                       @Param("entryId") UUID entryId,
                       @Param("transactionId") UUID transactionId,
                       @Param("type") String type,
                       @Param("amount") long amount,
                       @Param("postedAt") Instant postedAt);

    /**
     * Trang lịch sử theo cursor: các hàng có {@code seq} NHỎ HƠN cursor, mới nhất trước. Giao dịch
     * mới chỉ thêm hàng có seq lớn hơn, nên không bao giờ xô lệch trang đã đọc.
     */
    @Query("""
            SELECT e FROM WalletEntry e
             WHERE e.tenantId = :tenantId AND e.walletId = :walletId AND e.seq < :before
             ORDER BY e.seq DESC
            """)
    List<WalletEntry> pageBefore(@Param("tenantId") UUID tenantId,
                                 @Param("walletId") UUID walletId,
                                 @Param("before") long before,
                                 Limit limit);

    List<WalletEntry> findByTenantIdAndWalletId(UUID tenantId, UUID walletId);

    /** Đối soát nội bộ: tổng lịch sử của một ví PHẢI bằng số dư của nó. */
    @Query("SELECT COALESCE(SUM(e.amountMinor), 0) FROM WalletEntry e WHERE e.walletId = :walletId")
    long sumOf(@Param("walletId") UUID walletId);

    long countByWalletId(UUID walletId);

    /** Chỉ dùng khi DỰNG LẠI: gỡ các hàng sổ cái không xác nhận. */
    @Modifying
    @Query("DELETE FROM WalletEntry e WHERE e.walletId = :walletId AND e.entryId IN :entryIds")
    int deleteEntries(@Param("walletId") UUID walletId, @Param("entryIds") Collection<UUID> entryIds);
}
