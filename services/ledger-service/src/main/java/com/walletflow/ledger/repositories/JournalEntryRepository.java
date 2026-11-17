package com.walletflow.ledger.repositories;

import com.walletflow.ledger.dto.TopupEntryRow;
import com.walletflow.ledger.entity.JournalEntry;
import com.walletflow.common.event.EJournalType;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository APPEND-ONLY cho bút toán.
 *
 * <p>Cố tình kế thừa {@link Repository} (interface rỗng) và liệt kê TỪNG phương thức cần dùng, thay
 * vì {@code JpaRepository}. Lý do: {@code JpaRepository} lộ ra {@code delete}, {@code deleteAll},
 * {@code deleteById}... nghĩa là bất kỳ ai viết code trong service này cũng có thể xoá bút toán chỉ
 * bằng một dòng, và IDE còn gợi ý sẵn. Bất biến "sổ cái không bao giờ bị sửa/xoá" (plan §3) chỉ đáng
 * tin khi API không cho phép làm điều đó, chứ không phải khi mọi người nhớ là đừng làm.
 *
 * <p>Đây là tầng phòng thủ thứ hai. Tầng đầu là entity không có setter, tầng cuối là trigger ở DB
 * (V2 migration) — kể cả SQL viết tay hay ORM đi đường tắt cũng không vượt qua được.
 */
public interface JournalEntryRepository extends Repository<JournalEntry, UUID> {

    JournalEntry save(JournalEntry entry);

    /** Ghi xuống DB NGAY để vi phạm UNIQUE nổ ra tại chỗ gọi, không phải lúc commit. */
    JournalEntry saveAndFlush(JournalEntry entry);

    Optional<JournalEntry> findById(UUID id);

    /** Tra bút toán đã ghi theo khoá idempotency nghiệp vụ, TRONG PHẠM VI một tenant. */
    Optional<JournalEntry> findByTenantIdAndTransactionIdAndType(
            UUID tenantId, UUID transactionId, EJournalType type);

    Optional<JournalEntry> findByTenantIdAndId(UUID tenantId, UUID id);

    boolean existsByTenantIdAndId(UUID tenantId, UUID id);

    /** Bút toán gốc đã có REVERSAL chưa — mỗi bút toán chỉ được hoàn tác MỘT lần. */
    boolean existsByTenantIdAndReferenceIdAndType(UUID tenantId, UUID referenceId, EJournalType type);

    long countByTenantId(UUID tenantId);

    long count();

    /**
     * Một trang bút toán {@code TOPUP} XUYÊN tenant, theo con trỏ {@code (created_at, id)} — đường quét
     * chiều ngược của đối soát bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024 §3).
     *
     * <p>Xuyên tenant là chủ ý và là ngoại lệ duy nhất của gói này: câu hỏi là "có lần nạp nào mà
     * bank-adapter không biết không", và bank-adapter không thể liệt kê trước tenant của thứ nó không
     * biết. Kết quả mang {@code tenant_id} để người gọi tự hỏi tiếp trong đúng tenant.
     *
     * <p>Native, với literal {@code type = 'TOPUP'}: chỉ khi vị từ khớp NGUYÊN VĂN vị từ của partial
     * index {@code idx_journal_topup_created} (V9) thì planner mới chắc chắn dùng nó — một tham số
     * bind thay cho literal có thể khiến một generic plan bỏ qua index.
     *
     * <p>{@code created_at < :before}: người gọi chỉ hỏi bút toán đã yên. {@code created_at} lấy từ đồng
     * hồ JVM ({@code @CreatedDate}) nên hai instance lệch giờ có thể chèn một hàng "vào quá khứ" sau con
     * trỏ — lượt sau quét lại từ đầu nên nó vẫn được soi, chỉ muộn một lượt.
     */
    @Query(value = """
            SELECT j.tenant_id AS "tenantId", j.id AS "entryId",
                   j.transaction_id AS "transactionId", j.created_at AS "postedAt"
              FROM journal_entries j
             WHERE j.type = 'TOPUP'
               AND j.created_at < :before
               AND (j.created_at, j.id) > (:afterPostedAt, :afterId)
             ORDER BY j.created_at, j.id
             LIMIT :limit
            """, nativeQuery = true)
    List<TopupEntryRow> topupsAfter(@Param("afterPostedAt") Instant afterPostedAt,
                                    @Param("afterId") UUID afterId,
                                    @Param("before") Instant before,
                                    @Param("limit") int limit);
}
