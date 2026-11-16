package com.walletflow.bank.entity;

import com.walletflow.common.entity.BaseEntity;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.reconciliation.EReconciliationBreakStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Một lệnh tiền mà tiền qua cửa ngoài LỆCH giữa provider và sổ cái (roadmap 3.1 PR2a, ADR-024).
 *
 * <p>Không có setter, cùng lý do như {@link ReconciliationRun}. <b>Bản sinh ba</b> với ledger và
 * wallet-service.
 */
@Entity
@Getter
@NoArgsConstructor
@Table(name = "reconciliation_breaks")
public class ReconciliationBreak extends BaseEntity {

    @Column(name = "run_id", nullable = false)
    private UUID runId;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    /** Giá trị của {@code EReconciliationBreakType}, lưu dạng chuỗi — xem enum đó. */
    @Column(name = "type", nullable = false)
    private String type;

    /** Ở bank-adapter luôn là {@code <transactionId>:<currency>} — xem V8. */
    @Column(name = "subject", nullable = false)
    private String subject;

    @Enumerated(EnumType.STRING)
    @Column(name = "currency")
    private ECurrency currency;

    /** Tiền ròng phía PROVIDER theo {@code provider_transactions}: thu thành công - hoàn thành công. */
    @Column(name = "expected_minor", nullable = false)
    private long expectedMinor;

    /** Tiền ròng sổ cái đã ghi qua {@code EXTERNAL_BANK}. */
    @Column(name = "actual_minor", nullable = false)
    private long actualMinor;

    @Column(name = "delta_minor", nullable = false)
    private long deltaMinor;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private EReconciliationBreakStatus status;

    /** Ở bank-adapter luôn false — máy không có số đúng nào để chép về (V8, ADR-024 §4). */
    @Column(name = "auto_repaired", nullable = false)
    private boolean autoRepaired;

    @Column(name = "times_seen", nullable = false)
    private int timesSeen;

    @Column(name = "detail")
    private String detail;

    @Column(name = "first_seen_at", nullable = false)
    private Instant firstSeenAt;

    @Column(name = "detected_at", nullable = false)
    private Instant detectedAt;

    @Column(name = "closed_at")
    private Instant closedAt;

    @Column(name = "closed_by")
    private UUID closedBy;

    @Column(name = "closed_note")
    private String closedNote;
}
