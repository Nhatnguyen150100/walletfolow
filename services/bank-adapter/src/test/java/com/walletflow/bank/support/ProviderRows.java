package com.walletflow.bank.support;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.UUID;

/**
 * Dựng thẳng hàng {@code provider_transactions} cho test đối soát bank-adapter ↔ sổ cái.
 *
 * <p>Dựng bằng SQL vì thứ cần kiểm là phép so tiền ròng trên DỮ LIỆU, không phải đường tạo hàng; và vì
 * {@code updated_at} phải lùi về quá khứ (hàng "đã yên" qua settle-window) — service không cho đặt giá trị đó.
 * Thoả các CHECK của V1/V6: FAILED cần mã lỗi, PENDING/SUCCEEDED cần {@code provider_ref}, REFUND cần
 * {@code refund_of}.
 */
public final class ProviderRows {

    /** Đủ xa để vượt settle-window một giờ của lớp test. */
    public static final long SETTLED_SECONDS_AGO = 2 * 3600;

    private final JdbcTemplate jdbc;

    public ProviderRows(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Lần thu VND đã yên (cập nhật lần cuối cách đây 2 giờ). */
    public UUID charge(UUID tenantId, UUID transactionId, String status, long amountMinor) {
        return charge(UUID.randomUUID(), tenantId, transactionId, status, amountMinor, SETTLED_SECONDS_AGO);
    }

    public UUID charge(UUID id, UUID tenantId, UUID transactionId, String status, long amountMinor,
                       long updatedSecondsAgo) {
        insert(id, tenantId, transactionId, "CHARGE", status, amountMinor, null, updatedSecondsAgo);
        return id;
    }

    /** Lần hoàn của {@code chargeId}, cũng đã yên. */
    public UUID refund(UUID tenantId, UUID transactionId, UUID chargeId, String status, long amountMinor) {
        return refund(tenantId, transactionId, chargeId, status, amountMinor, SETTLED_SECONDS_AGO);
    }

    public UUID refund(UUID tenantId, UUID transactionId, UUID chargeId, String status, long amountMinor,
                       long updatedSecondsAgo) {
        UUID id = UUID.randomUUID();
        insert(id, tenantId, transactionId, "REFUND", status, amountMinor, chargeId, updatedSecondsAgo);
        return id;
    }

    private void insert(UUID id, UUID tenantId, UUID transactionId, String operation, String status,
                        long amountMinor, UUID refundOf, long updatedSecondsAgo) {
        boolean needsRef = !status.equals("NEW") && !status.equals("FAILED");
        jdbc.update("INSERT INTO provider_transactions (id, tenant_id, transaction_id, operation, provider, "
                        + "idempotency_key, provider_ref, wallet_id, amount_minor, currency, status, failure_code, "
                        + "refund_of, raw_request, created_at, updated_at) "
                        + "VALUES (?, ?, ?, ?, 'mock', ?, ?, ?, ?, 'VND', ?, ?, ?, CAST('{}' AS jsonb), "
                        + "now() - make_interval(secs => ?), now() - make_interval(secs => ?))",
                id, tenantId, transactionId, operation, UUID.randomUUID().toString(),
                needsRef ? "ref-" + id : null, UUID.randomUUID(), amountMinor, status,
                status.equals("FAILED") ? "DECLINED" : null, refundOf,
                updatedSecondsAgo + 60, updatedSecondsAgo);
    }
}
