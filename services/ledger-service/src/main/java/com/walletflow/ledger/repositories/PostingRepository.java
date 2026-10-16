package com.walletflow.ledger.repositories;

import com.walletflow.ledger.entity.Posting;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

/**
 * Repository APPEND-ONLY cho các vế nợ/có. Xem ghi chú ở {@link JournalEntryRepository} về lý do
 * không dùng {@code JpaRepository}.
 */
public interface PostingRepository extends Repository<Posting, UUID> {

    <S extends Posting> List<S> saveAll(Iterable<S> postings);

    List<Posting> findByTenantIdAndEntryId(UUID tenantId, UUID entryId);

    /**
     * Số dư THẬT của một tài khoản = tổng mọi posting (plan §5.3).
     *
     * <p>Số dư là giá trị SUY RA, không phải một cột được cập nhật. Nhờ vậy không tồn tại khái niệm
     * "số dư bị sai" ở ledger: nó luôn đúng bằng định nghĩa. Cột {@code balance} ở wallet-service chỉ
     * là cache, và job đối soát (plan §7.7) so nó với hàm này.
     */
    @Query("""
            SELECT COALESCE(SUM(p.amountMinor), 0) FROM Posting p
             WHERE p.tenantId = :tenantId AND p.accountId = :accountId
            """)
    long balanceOf(@Param("tenantId") UUID tenantId, @Param("accountId") UUID accountId);

    /**
     * BẤT BIẾN VÀNG: tổng mọi posting toàn hệ thống phải luôn bằng 0 (plan §7.7).
     * Khác 0 nghĩa là tiền tự sinh hoặc bốc hơi — báo động đỏ.
     */
    @Query("SELECT COALESCE(SUM(p.amountMinor), 0) FROM Posting p WHERE p.tenantId = :tenantId")
    long totalOfTenant(@Param("tenantId") UUID tenantId);

    /**
     * Bất biến vàng tính trên TOÀN HỆ THỐNG.
     *
     * <p>Giữ lại bên cạnh {@link #totalOfTenant} vì hai phép kiểm bắt hai loại lỗi khác nhau, và
     * phép kiểm toàn cục một mình thì KHÔNG đủ: nếu một bút toán ghi nợ tenant A và ghi có tenant B,
     * tổng toàn cục vẫn bằng 0 trong khi sổ của cả hai khách đều đã sai. Đối soát phải chạy cả hai.
     */
    @Query("SELECT COALESCE(SUM(p.amountMinor), 0) FROM Posting p")
    long totalOfSystem();

    long countByTenantId(UUID tenantId);

    long count();
}
