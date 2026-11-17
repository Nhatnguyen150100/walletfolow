package com.walletflow.ledger.repositories;

import com.walletflow.ledger.dto.BalanceRow;
import com.walletflow.ledger.dto.EntryLineRow;
import com.walletflow.ledger.dto.ExternalMovementRow;
import com.walletflow.ledger.dto.ImbalanceRow;
import com.walletflow.ledger.dto.StatementRow;
import com.walletflow.ledger.entity.Posting;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
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

    /**
     * Tổng posting của MỌI nhóm {@code (tenant, currency)} — phép kiểm tầng 1 của đối soát
     * (roadmap 3.1, ADR-023 §2). Nhóm nào khác 0 là tiền tự sinh hoặc bốc hơi trong sổ của một khách.
     *
     * <p>Native vì {@code currency} nằm ở {@code accounts}, không có ở {@code postings} lẫn
     * {@code journal_entries} — câu SQL trong roadmap §3.1 join nhầm bảng.
     *
     * <p><b>Cố ý KHÔNG có {@code HAVING SUM(...) <> 0}.</b> Trả về mọi nhóm rồi lọc trong Java tốn
     * thêm vài chục hàng (số nhóm = số tenant × số loại tiền, luôn nhỏ) nhưng đổi lại job biết nó
     * đã soi BAO NHIÊU nhóm chỉ với MỘT lượt quét. Có {@code HAVING} thì {@code checked_count} phải
     * đếm bằng một câu thứ hai — tức là quét {@code postings}, bảng lớn nhất hệ thống, hai lần mỗi
     * giờ cho cùng một câu hỏi.
     *
     * <p>Điều kiện join có {@code a.tenant_id = p.tenant_id}: một vế trỏ vào tài khoản LỆCH TENANT
     * bị loại khỏi phép kiểm này. Đó là lý do phải chạy kèm {@link #totalOfSystem()} — xem javadoc
     * của nó.
     */
    @Query(value = """
            SELECT p.tenant_id AS "tenantId", a.currency AS "currency", SUM(p.amount_minor) AS "totalMinor"
              FROM postings p
              JOIN accounts a ON a.id = p.account_id AND a.tenant_id = p.tenant_id
             GROUP BY p.tenant_id, a.currency
             ORDER BY p.tenant_id, a.currency
            """, nativeQuery = true)
    List<ImbalanceRow> sumByTenantAndCurrency();

    /**
     * Số dư của NHIỀU tài khoản cùng loại tiền trong một lượt — nguồn của
     * {@code POST /internal/accounts/balances}, dùng cho đối soát tầng 2 (ADR-023 §3).
     *
     * <p>{@code LEFT JOIN} chứ không INNER: một tài khoản vừa mở, chưa có posting nào, phải trả về
     * {@code 0} chứ không được BIẾN MẤT khỏi kết quả. Nếu nó biến mất, bên đối soát không phân biệt
     * được "ví này số dư 0, khớp" với "ledger không có tài khoản cho ví này" — hai kết luận hoàn
     * toàn khác nhau, và chỉ cái thứ hai là một break.
     *
     * <p>Lọc tenant ở CẢ HAI bảng, cùng lý do như {@link #statementOf}.
     */
    @Query(value = """
            SELECT a.external_ref AS "accountRef", COALESCE(SUM(p.amount_minor), 0) AS "balanceMinor"
              FROM accounts a
              LEFT JOIN postings p ON p.account_id = a.id AND p.tenant_id = :tenantId
             WHERE a.tenant_id = :tenantId AND a.currency = :currency AND a.external_ref IN (:refs)
             GROUP BY a.external_ref
            """, nativeQuery = true)
    List<BalanceRow> balancesOf(@Param("tenantId") UUID tenantId,
                                @Param("currency") String currency,
                                @Param("refs") Collection<String> refs);

    /**
     * Tiền RÒNG mỗi lệnh tiền đã đưa vào hệ thống qua {@code EXTERNAL_BANK}, theo từng loại tiền —
     * phía sổ cái của đối soát bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024 §2).
     *
     * <p>{@code -SUM} vì một lần nạp ghi NỢ {@code EXTERNAL_BANK}: tiền rời "thế giới bên ngoài" để
     * vào ví. Cố ý KHÔNG lọc theo loại bút toán: {@code REVERSAL} của một lần nạp tự triệt tiêu nó, và
     * bất cứ bút toán nào khác chạm {@code EXTERNAL_BANK} cũng là tiền qua cửa ngoài — tức là đúng thứ
     * phải khớp với provider. Lọc theo loại sẽ giấu đúng bút toán lạ cần bắt.
     *
     * <p>Lệnh không có vế nào ở {@code EXTERNAL_BANK} KHÔNG có mặt trong kết quả: INNER JOIN ở đây là
     * chủ ý, người gọi hiểu "vắng" là 0. Một vế trỏ vào tài khoản đã mất cũng biến mất — ca đó là việc
     * của tầng 1 ({@link #totalOfSystem}), không phải của phép so này.
     *
     * <p>Lọc tenant ở CẢ BA bảng, cùng lý do như {@link #statementOf}. Đi được bằng
     * {@code uk_journal_tenant_txn_type} và {@code idx_postings_tenant_entry} (V7), không cần index mới.
     */
    @Query(value = """
            SELECT j.transaction_id AS "transactionId", a.currency AS "currency",
                   -SUM(p.amount_minor) AS "netInMinor"
              FROM journal_entries j
              JOIN postings p ON p.entry_id = j.id AND p.tenant_id = :tenantId
              JOIN accounts a ON a.id = p.account_id AND a.tenant_id = :tenantId
             WHERE j.tenant_id = :tenantId AND j.transaction_id IN (:transactionIds)
               AND a.external_ref = 'EXTERNAL_BANK' AND a.type = 'SYSTEM'
             GROUP BY j.transaction_id, a.currency
            """, nativeQuery = true)
    List<ExternalMovementRow> externalMovementsOf(@Param("tenantId") UUID tenantId,
                                                  @Param("transactionIds") Collection<UUID> transactionIds);

    long countByTenantId(UUID tenantId);

    long count();
}
