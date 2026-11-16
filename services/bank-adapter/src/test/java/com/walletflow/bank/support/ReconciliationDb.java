package com.walletflow.bank.support;

import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Đọc thẳng {@code reconciliation_runs} / {@code reconciliation_breaks} của bank-adapter bằng SQL — kết luận
 * của đối soát phải kiểm ở dữ liệu người trực sẽ đọc, không ở giá trị trả về của {@code reconcile()}.
 */
public final class ReconciliationDb {

    private final JdbcTemplate jdbc;

    public ReconciliationDb(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Dọn giữa các test: dữ liệu đối soát + hàng provider (cả hai đều do test dựng). */
    public void clean() {
        jdbc.execute("TRUNCATE reconciliation_breaks, reconciliation_runs");
        jdbc.execute("DELETE FROM provider_transactions");
    }

    public List<Map<String, Object>> runs() {
        return jdbc.queryForList("SELECT * FROM reconciliation_runs ORDER BY started_at");
    }

    /** Đúng một lượt đối soát — nếu không thì test đang nói về thứ khác. */
    public Map<String, Object> onlyRun() {
        List<Map<String, Object>> runs = runs();
        assertThat(runs).as("đúng MỘT lượt đối soát").hasSize(1);
        return runs.getFirst();
    }

    public List<Map<String, Object>> breaks() {
        return jdbc.queryForList("SELECT * FROM reconciliation_breaks ORDER BY subject, type");
    }

    public List<Map<String, Object>> breaksOf(UUID tenantId) {
        return jdbc.queryForList("SELECT * FROM reconciliation_breaks WHERE tenant_id = ? ORDER BY subject, type",
                tenantId);
    }

    /** Đúng một break (ở mọi tenant). */
    public Map<String, Object> onlyBreak() {
        List<Map<String, Object>> breaks = breaks();
        assertThat(breaks).as("đúng MỘT break").hasSize(1);
        return breaks.getFirst();
    }

    public int count(String table) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class);
    }

    /** Đầu cửa sổ của lượt duy nhất — giá trị mà gauge {@code last_success_timestamp} phải mang. */
    public Instant onlyRunWindowStart() {
        return jdbc.queryForObject("SELECT window_start FROM reconciliation_runs", OffsetDateTime.class)
                .toInstant();
    }
}
