package com.walletflow.ledger.repositories;

import com.walletflow.ledger.entity.Account;
import com.walletflow.common.money.ECurrency;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

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
}
