package com.walletflow.ledger.entity;

import com.walletflow.common.entity.ImmutableEntity;
import com.walletflow.ledger.enums.EJournalType;
import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

/**
 * Một bút toán = một sự kiện tiền, gồm ≥ 2 {@link Posting} có tổng bằng 0 (plan §5.1).
 *
 * <p><b>APPEND-ONLY</b> (kế thừa {@link ImmutableEntity}, không có setter): ghi rồi không sửa,
 * không xoá. Sai sót được sửa bằng một bút toán {@code REVERSAL} trỏ về bút toán gốc qua
 * {@code referenceId} — nhờ vậy lịch sử tiền là một chuỗi chỉ-thêm, kiểm toán được.
 */
@Entity
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "journal_entries", uniqueConstraints = @UniqueConstraint(
        name = "uk_journal_txn_type",
        columnNames = {"transaction_id", "type"}
))
public class JournalEntry extends ImmutableEntity {

    /**
     * Khoá idempotency nghiệp vụ, cùng với {@code type} tạo thành ràng buộc
     * {@code uk_journal_txn_type}: đây là chốt chặn THẬT chống ghi trùng khi command bị Kafka giao
     * lại hoặc client retry (plan §7.2, bài toán P2).
     */
    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    /** Trỏ về bút toán gốc khi đây là {@code REVERSAL}; {@code null} với các loại khác. */
    @Column(name = "reference_id")
    private UUID referenceId;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false, length = 16)
    private EJournalType type;
}
