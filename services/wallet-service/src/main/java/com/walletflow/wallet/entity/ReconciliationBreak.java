package com.walletflow.wallet.entity;

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
 * Một ví LỆCH mà đối soát tầng 2 tìm thấy (roadmap 3.1, ADR-023).
 *
 * <p>Không có setter, cùng lý do như {@link ReconciliationRun}: ghi bằng một câu {@code INSERT ...
 * ON CONFLICT DO UPDATE} tường minh, đóng bằng một câu {@code UPDATE} tường minh.
 *
 * <p><b>Bản sinh đôi ở ledgerdb:</b> {@code com.walletflow.ledger.entity.ReconciliationBreak}.
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

    /** Ở tầng 2 luôn là id ví. */
    @Column(name = "subject", nullable = false)
    private String subject;

    @Enumerated(EnumType.STRING)
    @Column(name = "currency")
    private ECurrency currency;

    /** Con số ĐÚNG theo nguồn sự thật: tổng wallet_entries ở 2a, số dư bên ledger ở 2b. */
    @Column(name = "expected_minor", nullable = false)
    private long expectedMinor;

    /** Con số đang có ở read model — {@code wallets.balance_minor}. */
    @Column(name = "actual_minor", nullable = false)
    private long actualMinor;

    @Column(name = "delta_minor", nullable = false)
    private long deltaMinor;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private EReconciliationBreakStatus status;

    /** Máy đã dựng lại read model từ sổ cái. Break VẪN mở — xem ADR-023 §5. */
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
