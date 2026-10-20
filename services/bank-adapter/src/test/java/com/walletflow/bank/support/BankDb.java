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
        jdbc.execute("DELETE FROM webhook_events");
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

    /** Số lần đã hỏi lại (cơ số của backoff). */
    public int checkAttempts(UUID transactionId) {
        return jdbc.queryForObject("SELECT check_attempts FROM provider_transactions WHERE transaction_id = ?",
                Integer.class, transactionId);
    }

    /**
     * Còn bao nhiêu giây nữa tới lần hỏi lại kế tiếp, tính bằng ĐỒNG HỒ DB (âm = đã tới hạn). Test so với giá trị
     * mong đợi có dung sai vài giây vì giữa lúc job chạy và lúc đọc có độ trễ thật.
     */
    public double secondsUntilNextCheck(UUID transactionId) {
        return jdbc.queryForObject("SELECT EXTRACT(EPOCH FROM (next_check_at - now()))::float8 "
                + "FROM provider_transactions WHERE transaction_id = ?", Double.class, transactionId);
    }

    /**
     * Đẩy hạn phiên về quá khứ: quá hạn + grace {@code graceSeconds} thêm {@code secondsPastGrace} giây
     * ({@code expires_at = now() - grace - X}). X âm = đã quá hạn nhưng CÒN trong grace.
     */
    public void makeOverdue(UUID transactionId, long graceSeconds, long secondsPastGrace) {
        jdbc.update("UPDATE provider_transactions SET expires_at = now() - make_interval(secs => ?) "
                + "WHERE transaction_id = ?", graceSeconds + secondsPastGrace, transactionId);
    }

    /** Đặt hạn phiên cách bây giờ {@code seconds} giây (âm = đã quá hạn); {@code null} = hàng không có hạn (trước 2.2b). */
    public void setExpiresInSeconds(UUID transactionId, Long seconds) {
        if (seconds == null) {
            jdbc.update("UPDATE provider_transactions SET expires_at = NULL WHERE transaction_id = ?", transactionId);
            return;
        }
        jdbc.update("UPDATE provider_transactions SET expires_at = now() + make_interval(secs => ?) "
                + "WHERE transaction_id = ?", seconds, transactionId);
    }

    public String failureCode(UUID tenantId, UUID transactionId) {
        return (String) row(tenantId, transactionId).get("failure_code");
    }

    /** Giờ VN dạng yyyyMMddHHmmss của {@code provider_created_at}, do Postgres tính — độc lập với code Java của service. */
    public String providerCreatedAtAsVnpayDate(UUID transactionId) {
        return jdbc.queryForObject("SELECT to_char(provider_created_at AT TIME ZONE 'Asia/Ho_Chi_Minh', "
                + "'YYYYMMDDHH24MISS') FROM provider_transactions WHERE transaction_id = ?", String.class, transactionId);
    }

    /** Khoảng cách giữa {@code expires_at} và {@code provider_created_at}, theo giây. */
    public long sessionLifetimeSeconds(UUID transactionId) {
        return jdbc.queryForObject("SELECT EXTRACT(EPOCH FROM (expires_at - provider_created_at))::bigint "
                + "FROM provider_transactions WHERE transaction_id = ?", Long.class, transactionId);
    }

    /** Hạn phiên còn cách bây giờ bao nhiêu giây (đồng hồ DB). */
    public double secondsUntilExpiry(UUID transactionId) {
        return jdbc.queryForObject("SELECT EXTRACT(EPOCH FROM (expires_at - now()))::float8 "
                + "FROM provider_transactions WHERE transaction_id = ?", Double.class, transactionId);
    }

    /** Đẩy lịch hỏi XÁC NHẬN của hàng FAILED(EXPIRED) về quá khứ để nó tới hạn mà không phải chờ giờ thật. */
    public void makeExpiredConfirmationDue(UUID transactionId) {
        jdbc.update("UPDATE provider_transactions SET next_check_at = now() - interval '5 seconds' "
                + "WHERE transaction_id = ? AND status = 'FAILED' AND failure_code = 'EXPIRED'", transactionId);
    }

    /** next_check_at của hàng có NULL không (đã hỏi xong / không còn lịch). */
    public boolean nextCheckIsNull(UUID transactionId) {
        return jdbc.queryForObject("SELECT next_check_at IS NULL FROM provider_transactions WHERE transaction_id = ?",
                Boolean.class, transactionId);
    }

    /** Gán tài khoản merchant đã tạo phiên (VNPay: vnp_TmnCode); {@code null} = hàng trước 2.2b (legacy). */
    public void setProviderAccount(UUID transactionId, String account) {
        jdbc.update("UPDATE provider_transactions SET provider_account = ? WHERE transaction_id = ?",
                account, transactionId);
    }

    public String providerAccount(UUID tenantId, UUID transactionId) {
        return (String) row(tenantId, transactionId).get("provider_account");
    }

    /** Ép hàng sang một trạng thái/mã lỗi bằng SQL — dựng hàng FAILED(mã khác) có {@code next_check_at} bất kỳ. */
    public void forceFailed(UUID transactionId, String failureCode, boolean scheduledForConfirmation) {
        jdbc.update("UPDATE provider_transactions SET status = 'FAILED', failure_code = ?, "
                        + "next_check_at = CASE WHEN ? THEN now() - interval '10 seconds' END WHERE transaction_id = ?",
                failureCode, scheduledForConfirmation, transactionId);
    }

    /** Đặt {@code check_attempts} — để dựng hàng "đã bị hỏi nhiều lần". */
    public void setCheckAttempts(UUID transactionId, int attempts) {
        jdbc.update("UPDATE provider_transactions SET check_attempts = ? WHERE transaction_id = ?", attempts, transactionId);
    }

    // ------------------------------------------------------------------
    // webhook_events (2.3)
    // ------------------------------------------------------------------

    public int webhookCount() {
        return jdbc.queryForObject("SELECT count(*) FROM webhook_events", Integer.class);
    }

    public List<Map<String, Object>> webhooks() {
        return jdbc.queryForList("SELECT * FROM webhook_events ORDER BY received_at, id");
    }

    public int webhookCountOfRef(String providerRef) {
        return jdbc.queryForObject("SELECT count(*) FROM webhook_events WHERE provider_ref = ?",
                Integer.class, providerRef);
    }

    /** Hàng webhook theo mã tham chiếu (đọc thẳng, không qua repository của production). */
    public Map<String, Object> webhookOfRef(String providerRef) {
        return jdbc.queryForMap("SELECT * FROM webhook_events WHERE provider_ref = ?", providerRef);
    }

    /**
     * Một webhook của provider KHÁC (ví dụ trước khi đổi cấu hình) — dựng bằng SQL vì service chỉ ghi
     * được webhook của provider đang chạy.
     *
     * @return id của hàng
     */
    public UUID insertWebhookOfProvider(String provider, String providerRef) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO webhook_events (id, provider, provider_event_id, provider_ref, raw_payload, "
                        + "received_at) VALUES (?, ?, ?, ?, 'seed', now())",
                id, provider, UUID.randomUUID().toString().replace("-", "") + "0".repeat(32), providerRef);
        return id;
    }

    public Map<String, Object> webhookById(UUID id) {
        return jdbc.queryForMap("SELECT * FROM webhook_events WHERE id = ?", id);
    }

    /** Đẩy hạn thử lại về quá khứ để webhook "tới hạn" mà không phải chờ giờ thật. */
    public void makeWebhookDue(String providerRef) {
        jdbc.update("UPDATE webhook_events SET next_attempt_at = now() - interval '5 seconds' "
                + "WHERE provider_ref = ? AND processed_at IS NULL", providerRef);
    }

    /**
     * Còn bao nhiêu giây nữa tới lần thử kế tiếp, tính bằng ĐỒNG HỒ DB (âm = đã tới hạn). Test so với giá
     * trị mong đợi có dung sai vì giữa lúc worker chạy và lúc đọc có độ trễ thật.
     */
    public double secondsUntilNextWebhookAttempt(String providerRef) {
        return jdbc.queryForObject("SELECT EXTRACT(EPOCH FROM (next_attempt_at - now()))::float8 "
                + "FROM webhook_events WHERE provider_ref = ?", Double.class, providerRef);
    }
}
