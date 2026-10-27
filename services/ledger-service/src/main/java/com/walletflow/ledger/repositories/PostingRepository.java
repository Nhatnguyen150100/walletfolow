package com.walletflow.ledger.repositories;

import com.walletflow.ledger.dto.EntryLineRow;
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

    /**
     * Mọi vế của mọi bút toán thuộc MỘT lệnh tiền, kèm mã tài khoản người đọc được — nguồn của
     * {@code GET /internal/entries} mà người trực dùng để phân loại một ca kẹt (roadmap 2.5 PR3).
     *
     * <p>Khác {@link #statementOf}: sao kê gộp các vế về một tài khoản, ở đây giữ ĐỦ CẢ HAI VẾ. Câu
     * hỏi khác nhau — "tài khoản này nhận bao nhiêu" so với "tiền đi từ đâu sang đâu" — và chỉ câu
     * thứ hai phân loại được ca.
     *
     * <p>Lọc tenant ở CẢ BA bảng, cùng lý do như {@link #statementOf}: một hàng ghi lệch tenant thì
     * biến mất khỏi kết quả thay vì lặng lẽ hiện ra trên màn hình của khách khác.
     *
     * <p><b>{@code LEFT JOIN Account} chứ không INNER</b>: một vế có {@code account_id} trỏ vào
     * tài khoản đã mất hoặc lệch tenant là dữ liệu hỏng, nhưng INNER JOIN sẽ làm chính cái vế đó
     * BIẾN MẤT khỏi màn hình — người trực thấy một bút toán thiếu vế, trông hệt như "tiền chưa đi
     * đâu". Đó đúng là hiểu nhầm mà cả đường đọc này sinh ra để chặn. Với LEFT JOIN, vế vẫn hiện,
     * {@code accountRef} là {@code null}, {@code sumMinor()} vẫn đúng, và sự bất thường nhìn thấy
     * được thay vì ẩn đi.
     *
     * <p>Sắp theo {@code amountMinor} tăng dần trong một bút toán: vế ghi nợ (âm) lên trước vế ghi
     * có (dương), nên đọc một dòng là thấy ngay tiền đi từ đâu sang đâu. {@code accountId} ở cuối
     * chỉ để thứ tự tất định khi hai vế cùng số tiền.
     *
     * <p>Giữa các bút toán, {@code createdAt} đến từ {@code @CreatedDate} — đồng hồ của JVM, không
     * phải của DB. Hai instance ledger lệch giờ có thể làm {@code REVERSAL} hiện trước bút toán gốc;
     * {@code referenceId} là thứ nói chắc chắn cái nào đảo cái nào, đừng chỉ dựa vào thứ tự.
     */
    @Query("""
            SELECT new com.walletflow.ledger.dto.EntryLineRow(
                       j.id, j.type, j.referenceId, j.createdAt,
                       p.accountId, a.externalRef, p.amountMinor)
              FROM JournalEntry j
              JOIN Posting p ON p.entryId = j.id AND p.tenantId = :tenantId
              LEFT JOIN Account a ON a.id = p.accountId AND a.tenantId = :tenantId
             WHERE j.tenantId = :tenantId AND j.transactionId = :transactionId
             ORDER BY j.createdAt, j.id, p.amountMinor, p.accountId
            """)
    List<EntryLineRow> entryLinesOf(@Param("tenantId") UUID tenantId,
                                    @Param("transactionId") UUID transactionId);

    long countByTenantId(UUID tenantId);

    long count();
}
