package com.walletflow.ledger.entity;

import com.walletflow.common.entity.BaseEntity;
import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.money.ECurrency;
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
        name = "uk_accounts_tenant_ref_currency",
        columnNames = {"tenant_id", "external_ref", "currency"}
))
public class Account extends BaseEntity {

    /**
     * Tenant sở hữu tài khoản. Là thành phần ĐẦU TIÊN của unique key: không có nó, chỉ một tenant
     * trên toàn hệ thống được phép có ví mang tên 'WALLET-001'.
     */
    @Column(name = "tenant_id", nullable = false)
    private java.util.UUID tenantId;

    @Column(name = "external_ref", nullable = false, length = 64)
    private String externalRef;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false, length = 16)
    private ETypeAccount type;

    @Enumerated(EnumType.STRING)
    @Column(name = "currency", nullable = false, length = 3)
    private ECurrency currency;

    /** Ledger cưỡng chế lúc ghi bút toán — xem V5 và {@code LedgerService.validateAccountStatus}. */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    @Builder.Default
    private EAccountStatus status = EAccountStatus.ACTIVE;

    /**
     * Tăng mỗi lần {@link #status} đổi — do TRIGGER ở DB tăng (V5), không phải code. Hibernate không
     * bao giờ ghi cột này.
     */
    @Column(name = "status_version", nullable = false, insertable = false, updatable = false)
    private int statusVersion;
}
