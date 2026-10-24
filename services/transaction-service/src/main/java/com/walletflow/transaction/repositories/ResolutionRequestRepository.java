package com.walletflow.transaction.repositories;

import com.walletflow.transaction.entity.ResolutionRequest;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Không có {@code save} tuỳ ý: hàng chỉ ra đời bằng câu INSERT dưới đây và chỉ đổi đúng một lần,
 * bằng câu UPDATE có điều kiện — cùng khuôn với {@code TransactionRepository}.
 */
public interface ResolutionRequestRepository extends Repository<ResolutionRequest, UUID> {

    /**
     * Đề xuất một thao tác. Khoá riêng phần {@code uk_resolution_one_open} đảm bảo mỗi giao dịch tối
     * đa MỘT yêu cầu đang chờ; {@code ON CONFLICT DO NOTHING} biến lần đề xuất thứ hai thành "không
     * làm gì" thay vì một lỗi kỹ thuật.
     *
     * @return 1 nếu vừa tạo, 0 nếu giao dịch này đã có yêu cầu đang chờ
     */
    @Modifying
    @Query(value = """
            INSERT INTO resolution_requests
                   (id, tenant_id, transaction_id, action, status, maker_id, maker_email, maker_role,
                    reason, evidence, created_at, updated_at)
            VALUES (:id, :tenantId, :transactionId, :action, 'PENDING', :makerId,
                    CAST(:makerEmail AS VARCHAR), :makerRole, :reason, CAST(:evidence AS TEXT),
                    now(), now())
            ON CONFLICT DO NOTHING
            """, nativeQuery = true)
    int propose(@Param("id") UUID id,
                @Param("tenantId") UUID tenantId,
                @Param("transactionId") UUID transactionId,
                @Param("action") String action,
                @Param("makerId") UUID makerId,
                @Param("makerEmail") String makerEmail,
                @Param("makerRole") String makerRole,
                @Param("reason") String reason,
                @Param("evidence") String evidence);

    /**
     * Giao dịch của một yêu cầu, đọc KHÔNG khoá. Dùng để khoá theo đúng thứ tự thống nhất (saga →
     * giao dịch → yêu cầu) trước khi khoá chính hàng yêu cầu — xem {@code ResolutionService.decide}.
     */
    @Query("SELECT r.transactionId FROM ResolutionRequest r WHERE r.tenantId = :tenantId AND r.id = :id")
    Optional<UUID> findTransactionId(@Param("tenantId") UUID tenantId, @Param("id") UUID id);

    /**
     * Khoá yêu cầu trước khi quyết — hai người duyệt cùng lúc thì người sau nhìn thấy kết quả của
     * người trước thay vì cả hai cùng thực thi.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT r FROM ResolutionRequest r WHERE r.tenantId = :tenantId AND r.id = :id")
    Optional<ResolutionRequest> lock(@Param("tenantId") UUID tenantId, @Param("id") UUID id);

    /**
     * Ghi quyết định. Điều kiện {@code status = 'PENDING'} và {@code checker_id <> maker_id} là hàng
     * rào thứ hai sau khoá và sau kiểm ở tầng app — 4-eyes còn được CHECK ở DB cưỡng chế nữa
     * ({@code ck_resolution_four_eyes}), nên một lỗi ở bất kỳ tầng nào cũng không đủ để lọt.
     *
     * @return 1 nếu vừa ghi, 0 nếu đã có người quyết trước
     */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE resolution_requests
               SET status = :status,
                   checker_id = :checkerId,
                   checker_email = CAST(:checkerEmail AS VARCHAR),
                   checker_role = :checkerRole,
                   decided_at = now(),
                   updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id
               AND status = 'PENDING' AND maker_id <> :checkerId
            """, nativeQuery = true)
    int decide(@Param("tenantId") UUID tenantId,
               @Param("id") UUID id,
               @Param("status") String status,
               @Param("checkerId") UUID checkerId,
               @Param("checkerEmail") String checkerEmail,
               @Param("checkerRole") String checkerRole);

    /** Hàng đợi "chờ tôi duyệt", cũ nhất trước — cùng lý do thứ tự với hàng đợi ca cần can thiệp. */
    @Query("""
            SELECT r FROM ResolutionRequest r
             WHERE r.tenantId = :tenantId AND r.status = com.walletflow.transaction.enums.EResolutionStatus.PENDING
             ORDER BY r.createdAt ASC, r.id ASC
            """)
    List<ResolutionRequest> findPending(@Param("tenantId") UUID tenantId);
}
