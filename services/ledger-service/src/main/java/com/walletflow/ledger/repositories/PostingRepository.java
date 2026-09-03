package com.walletflow.ledger.repositories;

import com.walletflow.ledger.entity.Posting;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

/**
 * Repository APPEND-ONLY cho các vế nợ/có. Xem ghi chú ở {@link JournalEntryRepository} về lý do
 * không dùng {@code JpaRepository}.
 */
public interface PostingRepository extends Repository<Posting, UUID> {

    <S extends Posting> List<S> saveAll(Iterable<S> postings);

    List<Posting> findByEntryId(UUID entryId);

    /**
     * Số dư THẬT của một tài khoản = tổng mọi posting (plan §5.3).
     *
     * <p>Số dư là giá trị SUY RA, không phải một cột được cập nhật. Nhờ vậy không tồn tại khái niệm
     * "số dư bị sai" ở ledger: nó luôn đúng bằng định nghĩa. Cột {@code balance} ở wallet-service chỉ
     * là cache, và job đối soát (plan §7.7) so nó với hàm này.
     */
    @Query("SELECT COALESCE(SUM(p.amountMinor), 0) FROM Posting p WHERE p.accountId = :accountId")
    long balanceOf(@Param("accountId") UUID accountId);

    /**
     * BẤT BIẾN VÀNG: tổng mọi posting toàn hệ thống phải luôn bằng 0 (plan §7.7).
     * Khác 0 nghĩa là tiền tự sinh hoặc bốc hơi — báo động đỏ.
     */
    @Query("SELECT COALESCE(SUM(p.amountMinor), 0) FROM Posting p")
    long totalOfSystem();

    long count();
}
