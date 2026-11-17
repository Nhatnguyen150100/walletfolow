package com.walletflow.bank.repositories;

import com.walletflow.bank.entity.ReconciliationBreak;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Ghi và đóng các chỗ lệch bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024, cơ chế ở ADR-023 §7).
 *
 * <p><b>Bản sinh ba</b> với ledger-service và wallet-service. Không có {@code delete}: một break đã
 * ghi là một sự kiện đã xảy ra, đóng nó bằng {@link #close}.
 */
public interface ReconciliationBreakRepository extends Repository<ReconciliationBreak, UUID> {

    /**
     * Ghi một chỗ lệch, hoặc CẬP NHẬT chỗ lệch đang mở y hệt ({@code uk_recon_break_open}). Một lệnh
     * lệch mãi thì mỗi lượt cập nhật đúng một hàng và {@code times_seen} tăng; {@code first_seen_at}
     * giữ nguyên lần đầu. Javadoc đầy đủ ở bản của wallet-service.
     *
     * @return 1 luôn luôn (chèn mới hoặc cập nhật)
     */
    @Transactional
    @Modifying
    @Query(value = """
            INSERT INTO reconciliation_breaks
                (id, run_id, tenant_id, type, subject, currency, expected_minor, actual_minor,
                 delta_minor, status, auto_repaired, times_seen, detail,
                 first_seen_at, detected_at, created_at, updated_at)
            VALUES (:id, :runId, :tenantId, :type, :subject, :currency, :expected, :actual,
                    :actual - :expected, 'OPEN', false, 1, :detail,
                    now(), now(), now(), now())
            ON CONFLICT (tenant_id, type, subject) WHERE status = 'OPEN'
            DO UPDATE SET run_id        = EXCLUDED.run_id,
                          expected_minor = EXCLUDED.expected_minor,
                          actual_minor   = EXCLUDED.actual_minor,
                          delta_minor    = EXCLUDED.delta_minor,
                          currency       = EXCLUDED.currency,
                          detail         = EXCLUDED.detail,
                          times_seen     = reconciliation_breaks.times_seen + 1,
                          detected_at    = now(),
                          updated_at     = now()
            """, nativeQuery = true)
    int record(@Param("id") UUID id,
               @Param("runId") UUID runId,
               @Param("tenantId") UUID tenantId,
               @Param("type") String type,
               @Param("subject") String subject,
               @Param("currency") String currency,
               @Param("expected") long expectedMinor,
               @Param("actual") long actualMinor,
               @Param("detail") String detail);

    /**
     * Đóng một break (ADR-025 §3). {@code WHERE status = 'OPEN'} để gọi lại không ghi đè người đóng
     * lần đầu — câu lệnh idempotent, lần thứ hai thấy 0 hàng.
     *
     * <p><b>Lọc {@code tenant_id} dù {@code id} đã là khoá chính.</b> Id break là UUID nên "không
     * đoán được", nhưng không-đoán-được chưa bao giờ là mô hình bảo mật của ADR-010 — và đúng loại
     * lỗi này đã phải trả một lần ở {@code SagaRepository} (ADR-021 B5). Tenant sai ⇒ 0 hàng đổi,
     * người gọi dịch thành "không có break đó".
     *
     * @return 1 nếu vừa đóng, 0 nếu không có break đang mở nào khớp (sai id, sai tenant, hoặc đã đóng)
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE reconciliation_breaks
               SET status = 'CLOSED', closed_at = now(), closed_by = :closedBy,
                   closed_note = :note, updated_at = now()
             WHERE id = :id AND tenant_id = :tenantId AND status = 'OPEN'
            """, nativeQuery = true)
    int close(@Param("id") UUID id,
              @Param("tenantId") UUID tenantId,
              @Param("closedBy") UUID closedBy,
              @Param("note") String note);

    /** Hàng đợi của người trực: break đang mở, cũ nhất trước. */
    @Query("""
            SELECT b FROM ReconciliationBreak b
             WHERE b.tenantId = :tenantId
               AND b.status = com.walletflow.common.reconciliation.EReconciliationBreakStatus.OPEN
             ORDER BY b.firstSeenAt, b.id
            """)
    List<ReconciliationBreak> findOpen(@Param("tenantId") UUID tenantId, Limit limit);

    /** Tổng số break đang mở ở MỌI tenant — nguồn của gauge {@code walletflow.reconciliation.breaks}. */
    @Query("""
            SELECT COUNT(b) FROM ReconciliationBreak b
             WHERE b.status = com.walletflow.common.reconciliation.EReconciliationBreakStatus.OPEN
            """)
    long countOpenAcrossTenants();

    /**
     * Số break đang mở của MỘT loại, ở mọi tenant — nguồn của gauge
     * {@code walletflow.reconciliation.provider_ledger.open{type}}.
     *
     * <p>Riêng bank-adapter có câu này vì hai loại của nó cần hai mức alert khác nhau (ADR-024 §6):
     * {@code POSTED_WITHOUT_PROVIDER_MONEY} gọi người ngay, còn {@code PROVIDER_MONEY_NOT_POSTED} có
     * nguyên nhân đã biết (hoàn tay qua portal) nên chỉ là ticket. Gauge tổng không tách được hai thứ đó.
     */
    @Query("""
            SELECT COUNT(b) FROM ReconciliationBreak b
             WHERE b.type = :type
               AND b.status = com.walletflow.common.reconciliation.EReconciliationBreakStatus.OPEN
            """)
    long countOpenAcrossTenantsByType(@Param("type") String type);
}
