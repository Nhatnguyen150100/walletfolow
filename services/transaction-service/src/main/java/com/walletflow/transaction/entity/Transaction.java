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

    /** Null với TOPUP: tiền đến từ ngân hàng, không từ ví nào (V3, ADR-019). */
    @Column(name = "from_wallet")
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

    /** Phiên thanh toán của lệnh nạp (null với chuyển tiền) — xem V3. */
    @Column(length = 32)
    private String provider;

    @Column(name = "provider_ref", length = 128)
    private String providerRef;

    @Column(name = "payment_url")
    private String paymentUrl;

    @Column(name = "failure_code", length = 32)
    private String failureCode;

    @Column(name = "failure_reason")
    private String failureReason;

    /**
     * Lúc một người vận hành chốt ca này (2.5 PR2, ADR-021). Khác null = ĐÃ có người xử lý, nên một
     * bút toán ghi sau thời điểm này là XUNG ĐỘT chứ không phải "ledger thắng" — xem
     * {@code SagaService.onPosted}. Chi tiết ai/vì sao nằm ở {@code audit_events}.
     *
     * <p>Không bao giờ quay về null: "đã có người đụng vào" là sự thật không thu hồi được.
     */
    @Column(name = "resolved_at")
    private Instant resolvedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
