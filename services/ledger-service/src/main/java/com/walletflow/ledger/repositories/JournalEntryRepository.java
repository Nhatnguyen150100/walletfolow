package com.walletflow.ledger.repositories;

import com.walletflow.ledger.entity.JournalEntry;
import com.walletflow.ledger.enums.EJournalType;
import org.springframework.data.repository.Repository;

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

    /** Tra bút toán đã ghi theo khoá idempotency nghiệp vụ. */
    Optional<JournalEntry> findByTransactionIdAndType(UUID transactionId, EJournalType type);

    boolean existsById(UUID id);

    long count();
}
