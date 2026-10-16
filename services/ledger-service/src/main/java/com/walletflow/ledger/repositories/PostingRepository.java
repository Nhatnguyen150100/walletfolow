package com.walletflow.ledger.repositories;

import com.walletflow.ledger.dto.StatementRow;
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

    /**
     * Các bút toán chạm một tài khoản, mỗi bút toán MỘT dòng (gộp các vế cùng tài khoản), theo thứ
     * tự ghi sổ — nguồn của sao kê dùng để dựng lại read model.
     *
     * <p>Lọc tenant ở CẢ HAI bảng: chỉ lọc ở {@code postings} là đủ về mặt dữ liệu, nhưng điều kiện
     * thừa trên {@code journal_entries} khiến một lỗi ghi lệch tenant (nếu có) làm dòng biến mất
     * khỏi sao kê thay vì lặng lẽ trộn sổ của hai khách.
     */
    @Query("""
            SELECT new com.walletflow.ledger.dto.StatementRow(
                       j.id, j.transactionId, j.type, SUM(p.amountMinor), j.createdAt)
              FROM Posting p JOIN JournalEntry j ON j.id = p.entryId
             WHERE p.tenantId = :tenantId AND j.tenantId = :tenantId AND p.accountId = :accountId
             GROUP BY j.id, j.transactionId, j.type, j.createdAt
             ORDER BY j.createdAt, j.id
            """)
    List<StatementRow> statementOf(@Param("tenantId") UUID tenantId,
                                   @Param("accountId") UUID accountId);

    long countByTenantId(UUID tenantId);

    long count();
}
