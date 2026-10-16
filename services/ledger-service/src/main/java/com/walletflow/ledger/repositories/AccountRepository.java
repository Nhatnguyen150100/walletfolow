package com.walletflow.ledger.repositories;

import com.walletflow.ledger.entity.Account;
import com.walletflow.common.money.ECurrency;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * MỌI phương thức tra cứu đều nhận {@code tenantId} làm tham số đầu tiên, không có ngoại lệ.
 *
 * <p>Cố ý không có biến thể "tiện tay" kiểu {@code findByExternalRef(ref)}: chỉ cần một phương
 * thức như vậy tồn tại là sớm muộn sẽ có người gọi nó, và một truy vấn không lọc tenant trong hệ
 * thống giữ tiền là rò rỉ dữ liệu chéo khách hàng. Cách rẻ nhất để không gọi nhầm là không để nó
 * tồn tại trong API.
 */
public interface AccountRepository extends JpaRepository<Account, UUID> {

    Optional<Account> findByTenantIdAndExternalRefAndCurrency(
            UUID tenantId, String externalRef, ECurrency currency);

    /**
     * Nạp tài khoản và KHOÁ HÀNG ({@code SELECT ... FOR UPDATE}) — xem
     * {@code LedgerService.resolveAccounts} về vai trò "mutex cho tài khoản" của khoá này.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT a FROM Account a
             WHERE a.tenantId = :tenantId
               AND a.externalRef = :ref
               AND a.currency = :currency
            """)
    Optional<Account> findForUpdate(@Param("tenantId") UUID tenantId,
                                    @Param("ref") String ref,
                                    @Param("currency") ECurrency currency);

    List<Account> findByTenantId(UUID tenantId);

    /**
     * Tạo tài khoản nếu chưa có, KHÔNG làm gì nếu đã có — trong MỘT câu lệnh.
     *
     * <p>Vì sao không "tìm, chưa có thì save": hai lệnh mở cùng một ví chạy song song đều thấy "chưa
     * có", đều INSERT, và lệnh thứ hai vỡ ở unique key. Lỗi đó là lỗi kỹ thuật nên sẽ được retry và
     * cuối cùng vẫn đúng, nhưng phải trả giá bằng một lần rollback + backoff cho một thao tác vốn dĩ
     * idempotent. {@code ON CONFLICT DO NOTHING} để chính Postgres phân xử: đúng một hàng thắng, lệnh
     * còn lại thấy 0 hàng bị ảnh hưởng và đọc lại hàng đã có.
     *
     * <p>Không đụng tới hàng đã có — đặc biệt là {@code status}: mở lại một ví đã bị đóng băng
     * KHÔNG được làm nó sống lại.
     *
     * @return 1 nếu vừa tạo, 0 nếu đã tồn tại
     */
    // @Transactional ở đây chứ không chỉ ở service: method query khai trong interface thừa hưởng
    // readOnly=true của SimpleJpaRepository, và INSERT trong transaction read-only bị Postgres từ
    // chối. Đứng một mình thì nó tự mở transaction; được gọi bên trong transaction thì nhập vào đó.
    @Transactional
    @Modifying
    @Query(value = """
            INSERT INTO accounts (id, tenant_id, external_ref, type, currency, status, created_at, updated_at)
            VALUES (gen_random_uuid(), :tenantId, :ref, :type, :currency, 'ACTIVE', now(), now())
            ON CONFLICT ON CONSTRAINT uk_accounts_tenant_ref_currency DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("tenantId") UUID tenantId,
                       @Param("ref") String ref,
                       @Param("type") String type,
                       @Param("currency") String currency);
}
