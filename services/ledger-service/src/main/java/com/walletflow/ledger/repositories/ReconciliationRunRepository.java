package com.walletflow.ledger.repositories;

import com.walletflow.ledger.entity.ReconciliationRun;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Vòng đời một lượt đối soát (roadmap 3.1, ADR-023 §6).
 *
 * <p>Kế thừa {@link Repository} rỗng và liệt kê TỪNG thao tác, như các repository khác của ledger:
 * không có {@code save} tuỳ ý — một lượt chỉ đi được một chiều, {@code RUNNING} → kết cục.
 */
public interface ReconciliationRunRepository extends Repository<ReconciliationRun, UUID> {

    /**
     * Giành lượt cho một cửa sổ. Đây là TOÀN BỘ cơ chế chống chạy trùng giữa nhiều instance.
     *
     * <p>Không có ShedLock, không có advisory lock: {@code uk_recon_run_window} làm việc đó. Hai
     * instance cùng thức dậy cho cùng một cửa sổ thì Postgres cho đúng một cái thắng; cái thua thấy
     * 0 hàng bị ảnh hưởng và bỏ qua lượt này im lặng (KHÔNG phải lỗi — đồng nghiệp của nó đang chạy).
     *
     * <p>{@code ON CONFLICT DO NOTHING} chứ không bắt {@code 23505}: cùng lý lẽ với
     * {@code AccountRepository.insertIfAbsent} — để Postgres phân xử thay vì trả giá bằng một lần
     * rollback cho một thao tác vốn dĩ idempotent.
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

    /**
     * Chốt một lượt đã quét xong.
     *
     * <p>{@code WHERE status = 'RUNNING'} không phải trang trí: nó làm câu lệnh idempotent, nên một
     * lượt bị gọi chốt hai lần (lỗi lập trình, hoặc retry) không ghi đè con số của lần đầu.
     */
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

    /**
     * Lượt THÀNH CÔNG gần nhất của một phép kiểm — nguồn của metric
     * {@code walletflow.reconciliation.last_success_timestamp}.
     *
     * <p>Lấy theo {@code windowStart} chứ không {@code finishedAt}: câu hỏi là "cửa sổ nào đã được
     * phủ", và một lượt chạy bù cho cửa sổ cũ không được làm metric trẻ ra.
     */
    @Query("""
            SELECT r FROM ReconciliationRun r
             WHERE r.kind = :kind AND r.status = com.walletflow.common.reconciliation.EReconciliationRunStatus.SUCCEEDED
             ORDER BY r.windowStart DESC
             LIMIT 1
            """)
    Optional<ReconciliationRun> findLastSuccess(@Param("kind") String kind);
}
