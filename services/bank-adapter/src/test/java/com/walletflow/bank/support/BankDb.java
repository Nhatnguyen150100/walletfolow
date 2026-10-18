package com.walletflow.bank.support;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Đọc thẳng DB của bank-adapter bằng SQL — thứ test cần chứng minh nằm ở dữ liệu (hàng, outbox), không
 * ở giá trị trả về của service. Không đi qua repository của production để một lỗi lọc tenant ở đó
 * không che chính nó.
 */
public final class BankDb {

    private final JdbcTemplate jdbc;

    public BankDb(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Xoá sạch dữ liệu nghiệp vụ giữa các test (không đụng schema/Flyway). */
    public void clean() {
        jdbc.execute("DELETE FROM outbox_event");
        jdbc.execute("DELETE FROM provider_transactions");
    }

    /**
     * Một lần thu dở dang của provider KHÁC (ví dụ trước khi đổi cấu hình) — dựng bằng SQL vì service chỉ
     * ghi được hàng của provider đang chạy.
     */
    public void insertNewRowOfProvider(UUID tenantId, UUID transactionId, UUID walletId, long amountMinor,
                                       String provider) {
        jdbc.update("INSERT INTO provider_transactions (id, tenant_id, transaction_id, operation, provider, "
                        + "idempotency_key, wallet_id, amount_minor, currency, status, raw_request, created_at, updated_at) "
                        + "VALUES (?, ?, ?, 'CHARGE', ?, ?, ?, ?, 'VND', 'NEW', CAST('{}' AS jsonb), now(), now())",
                UUID.randomUUID(), tenantId, transactionId, provider, UUID.randomUUID().toString(),
                walletId, amountMinor);
    }

    /**
     * Dựng thẳng một hàng ở trạng thái bất kỳ (thoả các CHECK của V1: FAILED cần mã lỗi, PENDING/SUCCEEDED cần
     * provider_ref). Hàng PENDING được đặt {@code next_check_at} ở quá khứ, tức là "tới hạn hỏi lại".
     *
     * @return id của hàng
     */
    public UUID insertRow(UUID tenantId, UUID transactionId, UUID walletId, long amountMinor, String provider,
                          String status, String providerRef, String failureCode) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO provider_transactions (id, tenant_id, transaction_id, operation, provider, "
                        + "idempotency_key, provider_ref, wallet_id, amount_minor, currency, status, payment_url, "
                        + "failure_code, raw_request, raw_response, next_check_at, created_at, updated_at) "
                        + "VALUES (?, ?, ?, 'CHARGE', ?, ?, ?, ?, ?, 'VND', ?, ?, ?, CAST('{}' AS jsonb), "
                        + "CAST('{\"seed\":true}' AS jsonb), "
                        + "CASE WHEN CAST(? AS text) = 'PENDING' THEN now() - interval '10 seconds' END, now(), now())",
                id, tenantId, transactionId, provider, UUID.randomUUID().toString(), providerRef, walletId,
                amountMinor, status, providerRef == null ? null : "http://pay.example/" + providerRef,
                failureCode, status);
        return id;
    }

    /** Hàng theo id (không lọc tenant — test đọc thẳng để chứng minh trạng thái thật trong DB). */
    public Map<String, Object> rowById(UUID id) {
        return jdbc.queryForMap("SELECT * FROM provider_transactions WHERE id = ?", id);
    }

    /** Độ dài tối đa khai báo của một cột VARCHAR — để gắn hằng số trong code với schema thật. */
    public int varcharLength(String table, String column) {
        return jdbc.queryForObject("SELECT character_maximum_length FROM information_schema.columns "
                + "WHERE table_name = ? AND column_name = ?", Integer.class, table, column);
    }

    /** Payload (JSON envelope) của các event của một giao dịch trên một topic. */
    public List<String> outboxPayloads(UUID transactionId, String topic) {
        return jdbc.queryForList("SELECT payload FROM outbox_event WHERE topic = ? AND payload LIKE ? "
                + "ORDER BY created_at", String.class, topic, "%" + transactionId + "%");
    }

    public int rowCount(UUID tenantId, UUID transactionId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM provider_transactions WHERE tenant_id = ? AND transaction_id = ?",
                Integer.class, tenantId, transactionId);
    }

    public int rowCountAnyTenant(UUID transactionId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM provider_transactions WHERE transaction_id = ?",
                Integer.class, transactionId);
    }

    public Map<String, Object> row(UUID tenantId, UUID transactionId) {
        return jdbc.queryForMap(
                "SELECT * FROM provider_transactions WHERE tenant_id = ? AND transaction_id = ?",
                tenantId, transactionId);
    }

    public String status(UUID tenantId, UUID transactionId) {
        return (String) row(tenantId, transactionId).get("status");
    }

    public String providerRef(UUID tenantId, UUID transactionId) {
        return (String) row(tenantId, transactionId).get("provider_ref");
    }

    /** Hàng outbox của một giao dịch trên một topic: cột event_id, tenant_id, published, msg_key. */
    public List<Map<String, Object>> outbox(UUID transactionId, String topic) {
        return jdbc.queryForList(
                "SELECT event_id, tenant_id, published, msg_key FROM outbox_event "
                        + "WHERE topic = ? AND payload LIKE ? ORDER BY created_at",
                topic, "%" + transactionId + "%");
    }

    public List<UUID> outboxEventIds(UUID transactionId, String topic) {
        return outbox(transactionId, topic).stream()
                .map(row -> (UUID) row.get("event_id"))
                .toList();
    }

    public int outboxCountAnyTopic(UUID transactionId) {
        return jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE payload LIKE ?",
                Integer.class, "%" + transactionId + "%");
    }

    /** Đẩy hạn hỏi lại về quá khứ để hàng PENDING "tới hạn" mà không phải chờ giờ thật. */
    public void makeDue(UUID transactionId, int secondsAgo) {
        jdbc.update("UPDATE provider_transactions SET next_check_at = now() - make_interval(secs => ?) "
                + "WHERE transaction_id = ? AND status = 'PENDING'", secondsAgo, transactionId);
    }

    /** next_check_at của hàng có còn ở tương lai (so bằng đồng hồ DB, như chính câu SQL của job). */
    public boolean nextCheckIsInFuture(UUID transactionId) {
        Boolean future = jdbc.queryForObject(
                "SELECT next_check_at > now() FROM provider_transactions WHERE transaction_id = ?",
                Boolean.class, transactionId);
        return Boolean.TRUE.equals(future);
    }
}
