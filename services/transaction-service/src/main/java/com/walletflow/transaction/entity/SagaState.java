package com.walletflow.transaction.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/** Bước hiện tại của saga một giao dịch. Chỉ đọc — mọi thay đổi qua câu UPDATE trong {@code SagaRepository}. */
@Entity
@Getter
@NoArgsConstructor
@Table(name = "saga_state")
public class SagaState {

    @Id
    @Column(name = "transaction_id")
    private UUID transactionId;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "current_step", nullable = false, length = 32)
    private String currentStep;

    @Column(name = "step_status", nullable = false, length = 16)
    private String stepStatus;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "command_event_id", nullable = false)
    private UUID commandEventId;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "deadline_at", nullable = false)
    private Instant deadlineAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
