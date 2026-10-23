package com.walletflow.transaction.entity;

import com.walletflow.transaction.enums.EResolutionAction;
import com.walletflow.transaction.enums.EResolutionStatus;
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
 * Một yêu cầu xử lý ca cần can thiệp, chờ người THỨ HAI duyệt (4-eyes — GAP-A4, roadmap 2.5 PR2,
 * ADR-021).
 *
 * <p>Chỉ thao tác ĐỤNG TIỀN đi qua đây. Hàng chỉ được INSERT rồi UPDATE đúng một lần (lúc quyết),
 * nên không có setter: hai đường ghi đều là câu UPDATE có điều kiện ở repository.
 */
@Entity
@Table(name = "resolution_requests")
@Getter
@NoArgsConstructor
public class ResolutionRequest {

    @Id
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private EResolutionAction action;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private EResolutionStatus status;

    @Column(name = "maker_id", nullable = false)
    private UUID makerId;

    @Column(name = "maker_email", length = 255)
    private String makerEmail;

    /** Vai của người đề xuất LÚC ĐÓ — vai đổi theo thời gian, đọc lại về sau là sự thật khác. */
    @Column(name = "maker_role", nullable = false, length = 16)
    private String makerRole;

    @Column(name = "checker_id")
    private UUID checkerId;

    @Column(name = "checker_email", length = 255)
    private String checkerEmail;

    @Column(name = "checker_role", length = 16)
    private String checkerRole;

    @Column(nullable = false)
    private String reason;

    /** Bằng chứng — thường là mã lần hoàn ở portal của provider. */
    @Column
    private String evidence;

    @Column(name = "decided_at")
    private Instant decidedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
