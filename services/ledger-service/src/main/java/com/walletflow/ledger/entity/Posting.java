package com.walletflow.ledger.entity;

import com.walletflow.common.entity.ImmutableEntity;
import com.walletflow.common.money.Money;
import com.walletflow.ledger.enums.ECurrency;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.*;

import java.util.UUID;

/**
 * Một dòng ghi nợ/có của bút toán (plan §5.2). <b>APPEND-ONLY</b>: không sửa, không xoá.
 *
 * <p>{@code amountMinor} mang dấu: âm = ghi nợ (debit), dương = ghi có (credit). Lưu bằng số nguyên
 * theo đơn vị nhỏ nhất, không bao giờ dùng số thực (plan §6, ADR-005).
 */
@Entity
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "postings", indexes = @Index(
        name = "idx_postings_account",
        columnList = "account_id"
))
public class Posting extends ImmutableEntity {

    @Column(name = "entry_id", nullable = false)
    private UUID entryId;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(name = "amount_minor", nullable = false)
    private long amountMinor;

    /**
     * Số tiền của vế này dưới dạng value object — dùng khi cần cộng/trừ, để phép tính đi qua
     * {@link Money} (chống tràn số, chặn lẫn loại tiền) thay vì toán tử {@code +} trần
     * (ADR-005: "không dùng trực tiếp toán tử +/- trên số tiền").
     *
     * <p>Loại tiền phải truyền vào vì nó thuộc về {@code accounts}, không lặp lại ở mỗi posting.
     */
    public Money amount(ECurrency currency) {
        return Money.of(amountMinor, currency.name());
    }
}
