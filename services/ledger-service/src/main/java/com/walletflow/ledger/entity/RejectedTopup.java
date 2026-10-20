package com.walletflow.ledger.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Hàng rào của một lệnh NẠP đã bị từ chối (V6, ADR-020): ledger đã nói "không" với giao dịch này thì không bao
 * giờ nói "có" nữa, vì saga dựa vào lời từ chối đó để hoàn tiền về cho người dùng.
 *
 * <p>Chỉ INSERT (bằng câu SQL trong {@code RejectedTopupRepository}), không setter; DB chặn UPDATE/DELETE bằng
 * trigger append-only giống sổ cái.
 */
@Entity
@Getter
@NoArgsConstructor
@Table(name = "rejected_topups")
public class RejectedTopup {

    @EmbeddedId
    private Key id;

    /** Mã từ chối LẦN ĐẦU — mọi lần từ chối lại phát đúng mã này. */
    @Column(name = "reason_code", nullable = false, length = 64)
    private String reasonCode;

    @Column(name = "reason", nullable = false)
    private String reason;

    @Column(name = "rejected_at", nullable = false)
    private Instant rejectedAt;

    @Embeddable
    public record Key(
            @Column(name = "tenant_id", nullable = false) UUID tenantId,
            @Column(name = "transaction_id", nullable = false) UUID transactionId
    ) implements Serializable {
    }
}
