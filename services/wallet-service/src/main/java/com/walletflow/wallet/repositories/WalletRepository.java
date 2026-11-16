package com.walletflow.wallet.repositories;

import com.walletflow.common.money.ECurrency;
import com.walletflow.wallet.dto.WalletCheckRow;
import com.walletflow.wallet.entity.Wallet;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Kế thừa {@link Repository} rỗng và liệt kê TỪNG thao tác, như các repository của ledger: không có
 * {@code save} tuỳ ý (số dư chỉ đổi bằng câu UPDATE cộng delta), không có {@code delete}.
 *
 * <p>Mọi truy vấn nghiệp vụ nhận {@code tenantId} — trừ {@link #findPendingCreatedBeforeAcrossTenants},
 * job nền xuyên tenant, tên nói rõ điều đó.
 */
public interface WalletRepository extends Repository<Wallet, UUID> {

    Optional<Wallet> findByTenantIdAndId(UUID tenantId, UUID id);

    Optional<Wallet> findByTenantIdAndUserIdAndCurrency(UUID tenantId, UUID userId, ECurrency currency);

    List<Wallet> findByTenantIdAndUserIdOrderByCurrency(UUID tenantId, UUID userId);

    /**
     * Tạo ví nếu người này chưa có ví loại tiền này — trong MỘT câu lệnh. Hai request tạo ví song
     * song: Postgres bắt cái thứ hai chờ cái đầu commit rồi cho nó 0 hàng, không ai vỡ unique key.
     *
     * @return 1 nếu vừa tạo, 0 nếu đã có
     */
    @Transactional
    @Modifying
    @Query(value = """
            INSERT INTO wallets (id, tenant_id, user_id, currency)
            VALUES (:id, :tenantId, :userId, :currency)
            ON CONFLICT ON CONSTRAINT uk_wallets_tenant_user_currency DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("id") UUID id,
                       @Param("tenantId") UUID tenantId,
                       @Param("userId") UUID userId,
                       @Param("currency") String currency);

    /**
     * Khoá các ví của MỘT event ({@code SELECT ... FOR UPDATE}), theo thứ tự id.
     *
     * <p>Thứ tự cố định là điều kiện để không deadlock: lệnh chuyển A→B và B→A xử lý song song đều
     * xin khoá A trước B. Đây cũng là khoá mà {@code WalletRebuildService} lấy ĐẦU TIÊN — cùng một
     * trình tự "khoá ví rồi mới đụng {@code wallet_entries}" ở cả hai phía.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT w FROM Wallet w
             WHERE w.tenantId = :tenantId AND w.id IN :ids AND w.currency = :currency
             ORDER BY w.id
            """)
    List<Wallet> lockAll(@Param("tenantId") UUID tenantId,
                         @Param("ids") Collection<UUID> ids,
                         @Param("currency") ECurrency currency);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT w FROM Wallet w WHERE w.tenantId = :tenantId AND w.id = :id")
    Optional<Wallet> lock(@Param("tenantId") UUID tenantId, @Param("id") UUID id);

    /**
     * Cộng delta vào số dư — atomic ở DB, không đọc-sửa-ghi. Tràn BIGINT thì Postgres báo lỗi chứ
     * không quay vòng, và lỗi đó là lỗi kỹ thuật → retry → DLT, có người xem.
     */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE wallets SET balance_minor = balance_minor + :delta, updated_at = now()
             WHERE id = :id
            """, nativeQuery = true)
    int addToBalance(@Param("id") UUID id, @Param("delta") long delta);

    /** Chỉ dùng khi DỰNG LẠI từ sổ cái — đường cập nhật bình thường là {@link #addToBalance}. */
    @Modifying(clearAutomatically = true)
    @Query(value = "UPDATE wallets SET balance_minor = :balance, updated_at = now() WHERE id = :id",
            nativeQuery = true)
    int overwriteBalance(@Param("id") UUID id, @Param("balance") long balance);

    /**
     * Áp trạng thái CHỈ KHI version lớn hơn bản đang có. Event trạng thái cũ đến muộn (Kafka giao
     * lại, lệnh mở được gửi lại) thành câu UPDATE chạm 0 hàng — không cần bảng dedup.
     */
    @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE wallets SET status = :status, status_version = :version, updated_at = now()
             WHERE tenant_id = :tenantId AND id = :id AND status_version < :version
            """, nativeQuery = true)
    int applyStatusIfNewer(@Param("tenantId") UUID tenantId,
                           @Param("id") UUID id,
                           @Param("status") String status,
                           @Param("version") int version);

    /**
     * Ví còn {@code PENDING} quá lâu — ở MỌI tenant. Chỉ dành cho job nền gửi lại lệnh mở tài khoản;
     * người gọi phải xử lý từng ví trong {@code TenantContext} của chính ví đó.
     */
    @Query("""
            SELECT w FROM Wallet w
             WHERE w.status = com.walletflow.common.wallet.EWalletStatus.PENDING
               AND w.createdAt < :before
             ORDER BY w.createdAt
            """)
    List<Wallet> findPendingCreatedBeforeAcrossTenants(@Param("before") Instant before, Limit limit);

    /**
     * Một lô ví ĐÃ YÊN cùng tổng lịch sử của chúng — nguồn của đối soát tầng 2 (ADR-023 §3, §4).
     * Chạy ở MỌI tenant; người gọi phải xử lý từng ví trong {@code TenantContext} của chính ví đó.
     *
     * <p><b>{@code updated_at < :settledBefore} là hàng rào chống báo động giả.</b> Read model là
     * eventually consistent: một ví vừa nhận bút toán vài giây trước có thể đang ở giữa quãng
     * projector cập nhật, và so nó lúc đó cho ra "lệch" trong khi không có gì sai
     * ({@code WalletRebuildService} đã ghi rõ điều này ở javadoc). Alert kêu oan thì bị tắt, và lúc
     * đó nó không bắt được sự cố thật nữa.
     *
     * <p>Phân trang bằng CON TRỎ trên khoá chính ({@code id > :afterId}) chứ không OFFSET: lượt quét
     * kéo dài và ví mới được tạo trong lúc đó sẽ xô lệch mọi trang sau nếu dùng offset — cùng lý lẽ
     * với cột {@code seq} của {@code wallet_entries}.
     *
     * <p>{@code LEFT JOIN} để ví chưa có giao dịch nào vẫn ra một hàng với tổng 0, chứ không biến
     * mất khỏi phép đối soát.
     *
     * <p>Điều kiện join có {@code e.tenant_id = w.tenant_id} tuy {@code wallet_id} đã đủ định danh:
     * một hàng lịch sử ghi sai tenant sẽ bị LOẠI khỏi tổng thay vì được cộng im lặng — và đó đúng
     * là loại hỏng mà đối soát sinh ra để bắt. Cùng lý lẽ với {@code PostingRepository.balancesOf}
     * bên ledger (ADR-010: mọi query lọc theo tenant).
     */
    @Query(value = """
            SELECT w.id            AS "walletId",
                   w.tenant_id     AS "tenantId",
                   w.currency      AS "currency",
                   w.status        AS "status",
                   w.balance_minor AS "balanceMinor",
                   COALESCE(SUM(e.amount_minor), 0) AS "entriesSumMinor"
              FROM wallets w
              LEFT JOIN wallet_entries e ON e.wallet_id = w.id AND e.tenant_id = w.tenant_id
             WHERE w.updated_at < :settledBefore AND w.id > :afterId
             GROUP BY w.id, w.tenant_id, w.currency, w.status, w.balance_minor
             ORDER BY w.id
             LIMIT :batchSize
            """, nativeQuery = true)
    List<WalletCheckRow> scanSettledAcrossTenants(@Param("settledBefore") Instant settledBefore,
                                                  @Param("afterId") UUID afterId,
                                                  @Param("batchSize") int batchSize);

    /**
     * Đọc lại HAI con số của đúng một ví — lần so thứ hai trước khi ghi break (ADR-023 §4).
     *
     * <p>Hàng rào {@code updated_at} ở {@link #scanSettledAcrossTenants} chặn phần lớn báo động
     * giả, nhưng một ví có thể nhận bút toán đúng vào lúc job đang đọc nó. Đọc lại một lần nữa biến
     * "vừa lệch lúc tôi nhìn" thành "vẫn lệch" — chỉ cái thứ hai mới đáng gọi người.
     *
     * <p>Cố ý KHÔNG lọc {@code updated_at}: ví vừa đổi trong lúc quét vẫn phải đọc lại được, và nếu
     * lần đọc thứ hai cho thấy đã khớp thì đó chính là câu trả lời ta cần.
     */
    @Query(value = """
            SELECT w.id            AS "walletId",
                   w.tenant_id     AS "tenantId",
                   w.currency      AS "currency",
                   w.status        AS "status",
                   w.balance_minor AS "balanceMinor",
                   COALESCE(SUM(e.amount_minor), 0) AS "entriesSumMinor"
              FROM wallets w
              LEFT JOIN wallet_entries e ON e.wallet_id = w.id AND e.tenant_id = w.tenant_id
             WHERE w.id = :walletId
             GROUP BY w.id, w.tenant_id, w.currency, w.status, w.balance_minor
            """, nativeQuery = true)
    Optional<WalletCheckRow> recheck(@Param("walletId") UUID walletId);
}
