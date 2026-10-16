package com.walletflow.ledger.entity;

import com.walletflow.common.entity.ImmutableEntity;
import com.walletflow.common.money.Money;
import com.walletflow.common.money.ECurrency;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
@Table(name = "postings")
public class Posting extends ImmutableEntity {

    /**
     * Phi chuẩn hoá có chủ đích (suy ra được qua {@code entryId}). Bất biến vàng
     * {@code SUM(amountMinor) = 0} phải kiểm được theo từng tenant, và đó là truy vấn chạy thường
     * xuyên nhất trên bảng lớn nhất — bắt nó JOIN mỗi lần là trả giá vĩnh viễn để tiết kiệm 16 byte.
     */
    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

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
