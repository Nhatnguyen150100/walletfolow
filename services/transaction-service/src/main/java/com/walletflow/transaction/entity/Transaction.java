package com.walletflow.transaction.entity;

import com.walletflow.common.event.EJournalType;
import com.walletflow.common.money.ECurrency;
import com.walletflow.transaction.enums.ETransactionStatus;
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
 * Một lệnh chuyển tiền. Chỉ ĐỌC qua entity này — hàng được tạo bằng câu INSERT trong cùng transaction
 * với khoá idempotency (xem {@code TransactionRepository}), nên không có setter.
 */
@Entity
@Getter
@NoArgsConstructor
@Table(name = "transactions")
public class Transaction {

    @Id
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private EJournalType type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ETransactionStatus status;

    @Column(name = "initiated_by", nullable = false)
    private UUID initiatedBy;

    @Column(name = "from_wallet", nullable = false)
    private UUID fromWallet;

    @Column(name = "to_wallet", nullable = false)
    private UUID toWallet;

    @Column(name = "amount_minor", nullable = false)
    private long amountMinor;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 3)
    private ECurrency currency;

    @Column(length = 140)
    private String note;

    @Column(name = "failure_code", length = 32)
    private String failureCode;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
