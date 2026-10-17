package com.walletflow.wallet.entity;

import com.walletflow.common.event.EJournalType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Một bút toán, nhìn từ phía một ví. Chỉ đọc qua entity này — ghi bằng
 * {@code INSERT ... ON CONFLICT DO NOTHING} trong {@code WalletEntryRepository}, vì việc chèn hàng
 * này CHÍNH LÀ bước khử trùng (xem V1).
 */
@Entity
@Getter
@NoArgsConstructor
@Table(name = "wallet_entries")
public class WalletEntry {

    @Id
    private Long seq;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "wallet_id", nullable = false)
    private UUID walletId;

    @Column(name = "entry_id", nullable = false)
    private UUID entryId;

    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private EJournalType type;

    @Column(name = "amount_minor", nullable = false)
    private long amountMinor;

    @Column(name = "posted_at")
    private Instant postedAt;

    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt;
}
