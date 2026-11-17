package com.walletflow.bank.repositories;

import com.walletflow.bank.entity.ReconciliationRun;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Vòng đời một lượt đối soát bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024, cơ chế ở ADR-023 §6).
 *
 * <p><b>Bản sinh ba</b> với ledger-service và wallet-service — giữ ba bên giống nhau. Javadoc đầy đủ
 * về vì sao giành lượt bằng {@code UNIQUE} chứ không ShedLock nằm ở bản của wallet-service.
 */
public interface ReconciliationRunRepository extends Repository<ReconciliationRun, UUID> {

    /**
     * Giành lượt cho một cửa sổ — toàn bộ cơ chế chống chạy trùng giữa nhiều instance.
     *
     * @return 1 nếu giành được lượt, 0 nếu instance khác đã nhận cửa sổ này
     */
    @Transactional
    @Modifying
    @Query(value = """
            INSERT INTO reconciliation_runs
                (id, kind, window_start, status, started_at, checked_count, break_count, created_at, updated_at)
            VALUES (:id, :kind, :windowStart, 'RUNNING', now(), 0, 0, now(), now())
            ON CONFLICT ON CONSTRAINT uk_recon_run_window DO NOTHING
            """, nativeQuery = true)
    int claim(@Param("id") UUID id,
              @Param("kind") String kind,
              @Param("windowStart") Instant windowStart);

    /** Chốt một lượt. {@code WHERE status = 'RUNNING'} để chốt lần hai không ghi đè con số lần đầu. */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE reconciliation_runs
               SET status = :status, finished_at = now(), checked_count = :checked,
                   break_count = :breaks, failure_message = :failureMessage, updated_at = now()
             WHERE id = :id AND status = 'RUNNING'
            """, nativeQuery = true)
    int finish(@Param("id") UUID id,
               @Param("status") String status,
               @Param("checked") long checked,
               @Param("breaks") int breaks,
               @Param("failureMessage") String failureMessage);

    Optional<ReconciliationRun> findById(UUID id);

    /** Lượt THÀNH CÔNG gần nhất, theo {@code windowStart} — nguồn của {@code last_success_timestamp}. */
    @Query("""
            SELECT r FROM ReconciliationRun r
             WHERE r.kind = :kind AND r.status = com.walletflow.common.reconciliation.EReconciliationRunStatus.SUCCEEDED
             ORDER BY r.windowStart DESC
             LIMIT 1
            """)
    Optional<ReconciliationRun> findLastSuccess(@Param("kind") String kind);
}
