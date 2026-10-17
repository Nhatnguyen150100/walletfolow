package com.walletflow.transaction.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/** Câu trả lời đã trả cho một Idempotency-Key. Chỉ đọc — ghi bằng {@code IdempotencyRepository.claim}. */
@Entity
@Getter
@NoArgsConstructor
@IdClass(IdempotencyRecord.Key.class)
@Table(name = "idempotency_keys")
public class IdempotencyRecord {

    @Id
    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Id
    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Id
    @Column(name = "idem_key", nullable = false, length = 64)
    private String idemKey;

    @Column(name = "request_hash", nullable = false, length = 64)
    private String requestHash;

    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    @Column(name = "response_status", nullable = false)
    private short responseStatus;

    @Column(name = "response_body", nullable = false, columnDefinition = "text")
    private String responseBody;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class Key implements Serializable {
        private UUID tenantId;
        private UUID userId;
        private String idemKey;
    }
}
