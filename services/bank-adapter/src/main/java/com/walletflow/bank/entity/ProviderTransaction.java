package com.walletflow.bank.entity;

import com.walletflow.bank.enums.EProviderTxnStatus;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.money.ECurrency;
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
 * Một thao tác với provider. Chỉ đọc — mọi thay đổi trạng thái qua câu UPDATE có điều kiện trong
 * {@code ProviderTransactionRepository}, để hai luồng đua nhau (lệnh gửi lại, job đối chiếu, webhook ở
 * 2.3) không bao giờ cùng chuyển một hàng.
 *
 * <p>Cột {@code raw_request}/{@code raw_response}/{@code next_check_at} cố ý không map: chúng chỉ được
 * ghi bằng SQL và chỉ đọc khi đối soát.
 */
@Entity
@Getter
@NoArgsConstructor
@Table(name = "provider_transactions")
public class ProviderTransaction {

    @Id
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private EBankOperation operation;

    @Column(nullable = false, length = 32)
    private String provider;

    @Column(name = "idempotency_key", nullable = false, length = 64)
    private String idempotencyKey;

    @Column(name = "provider_ref", length = 128)
    private String providerRef;

    @Column(name = "wallet_id", nullable = false)
    private UUID walletId;

    @Column(name = "amount_minor", nullable = false)
    private long amountMinor;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 3)
    private ECurrency currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private EProviderTxnStatus status;

    @Column(name = "payment_url")
    private String paymentUrl;

    @Column(name = "failure_code", length = 32)
    private String failureCode;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /**
     * Lệnh gửi lại có đúng là cùng một lần thu không. Cùng {@code transactionId} mà khác ví, số tiền hay
     * loại tiền thì không phải "gửi lại" mà là bug phía producer — thu theo lệnh mới là thu sai số,
     * thu theo hàng cũ là lờ đi một lệnh.
     */
    public boolean sameChargeAs(BankChargeCommand command) {
        return walletId.equals(command.walletId())
                && amountMinor == command.amountMinor()
                && currency == command.currency();
    }
}
