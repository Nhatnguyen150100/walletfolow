package com.walletflow.wallet.entity;

import com.walletflow.common.money.ECurrency;
import com.walletflow.common.wallet.EWalletStatus;
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
 * Ví — READ MODEL. Chỉ ĐỌC qua entity này; mọi thay đổi đi qua các câu UPDATE có điều kiện trong
 * {@code WalletRepository}, nên entity cố ý không có setter.
 *
 * <p>{@code id} cũng là {@code accountRef} bên sổ cái — một UUID, không chứa thông tin cá nhân
 * (roadmap 5.4).
 */
@Entity
@Getter
@NoArgsConstructor
@Table(name = "wallets")
public class Wallet {

    @Id
    private UUID id;

    @Column(name = "tenant_id", nullable = false, updatable = false)
    private UUID tenantId;

    @Column(name = "user_id", nullable = false, updatable = false)
    private UUID userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 3, updatable = false)
    private ECurrency currency;

    @Column(name = "balance_minor", nullable = false)
    private long balanceMinor;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private EWalletStatus status;

    @Column(name = "status_version", nullable = false)
    private int statusVersion;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
