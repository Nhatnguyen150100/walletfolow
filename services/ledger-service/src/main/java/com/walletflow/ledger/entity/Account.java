package com.walletflow.ledger.entity;

import com.walletflow.common.entity.BaseEntity;
import com.walletflow.ledger.enums.ECurrency;
import com.walletflow.ledger.enums.ETypeAccount;
import jakarta.persistence.*;
import lombok.*;

@Entity
@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "accounts", uniqueConstraints = @UniqueConstraint(
        name = "uk_accounts_ref_currency",
        columnNames = {"external_ref", "currency"}
))
public class Account extends BaseEntity {
    @Column(name = "external_ref", nullable = false, length = 64)
    private String externalRef;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false, length = 16)
    private ETypeAccount type;

    @Enumerated(EnumType.STRING)
    @Column(name = "currency", nullable = false, length = 3)
    private ECurrency currency;
}
