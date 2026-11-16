package com.walletflow.ledger.entity;

import com.walletflow.common.entity.BaseEntity;
import com.walletflow.common.reconciliation.EReconciliationRunStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Một LƯỢT đối soát (roadmap 3.1, ADR-023).
 *
 * <p>Không có setter: hàng được tạo bằng câu {@code INSERT} tường minh của
 * {@code ReconciliationRunRepository#claim} (đồng thời là cách giành lượt giữa nhiều instance) và
 * kết thúc bằng một câu {@code UPDATE} tường minh. Không có đường ghi tuỳ ý nào — cùng khuôn với
 * {@code WalletRepository}.
 *
 * <p><b>Bản sinh đôi ở walletdb:</b> {@code com.walletflow.wallet.entity.ReconciliationRun}. Hai
 * bảng cố ý cùng hình dạng để báo cáo 3.2 gộp được (ADR-023 §1); sửa một bên thì sửa cả hai. Không
 * gộp về common-lib được vì mọi service đều {@code @EntityScan("com.walletflow")} — một entity ở
 * đó sẽ bị bank-adapter và transaction-service nạp theo, và {@code ddl-auto: validate} nổ vì hai DB
 * đó không có bảng này.
 */
@Entity
@Getter
@NoArgsConstructor
@Table(name = "reconciliation_runs")
public class ReconciliationRun extends BaseEntity {

    @Column(name = "kind", nullable = false)
    private String kind;

    /** Đầu cửa sổ đã làm tròn xuống — KHÁC {@link #startedAt}, xem {@code uk_recon_run_window}. */
    @Column(name = "window_start", nullable = false)
    private Instant windowStart;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private EReconciliationRunStatus status;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "checked_count", nullable = false)
    private long checkedCount;

    @Column(name = "break_count", nullable = false)
    private int breakCount;

    @Column(name = "failure_message")
    private String failureMessage;
}
