package com.walletflow.transaction.repositories;

import com.walletflow.transaction.entity.IdempotencyRecord;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface IdempotencyRepository extends Repository<IdempotencyRecord, IdempotencyRecord.Key> {

    /**
     * Câu trả lời CÒN HIỆU LỰC cho key này. Key quá hạn được coi như chưa từng dùng.
     *
     * <p>Native với {@code now()} — CÙNG đồng hồ với {@link #claim}. JPQL {@code CURRENT_TIMESTAMP}
     * thành {@code localtimestamp} trên Postgres; hai cách đọc giờ đó lệch nhau trong giờ lặp lại khi
     * múi giờ phiên có DST, và hai phép so "còn hạn" bất đồng thì nhánh conflict của claim sẽ không
     * tìm thấy câu trả lời nó vừa đụng phải.
     */
    @Query(value = """
            SELECT * FROM idempotency_keys
             WHERE tenant_id = :tenantId AND user_id = :userId AND idem_key = :key
               AND expires_at > now()
            """, nativeQuery = true)
    Optional<IdempotencyRecord> findLive(@Param("tenantId") UUID tenantId,
                                         @Param("userId") UUID userId,
                                         @Param("key") String key);

    /**
     * Giành quyền xử lý key — và ghi luôn câu trả lời — trong MỘT câu lệnh.
     *
     * <ul>
     *   <li>Key mới: chèn hàng, trả 1.</li>
     *   <li>Key đã có nhưng QUÁ HẠN: ghi đè, trả 1 — quá hạn nghĩa là coi như key mới.</li>
     *   <li>Key đã có và còn hạn: không đụng gì, trả 0 — người gọi đọc lại câu trả lời đã lưu.</li>
     * </ul>
     *
     * <p>Hai request cùng key chạy song song: Postgres bắt câu INSERT thứ hai CHỜ ở khoá chính tới khi
     * transaction thứ nhất commit, rồi nó đi nhánh {@code ON CONFLICT} và trả 0. Vì người gọi chỉ ghi
     * transaction + outbox khi nhận được 1, không bao giờ có hai lệnh ghi sổ cho cùng một key — và
     * không có lần nào vỡ unique key thành lỗi 500.
     */
    @Modifying
    @Query(value = """
            INSERT INTO idempotency_keys
                   (tenant_id, user_id, idem_key, request_hash, transaction_id,
                    response_status, response_body, created_at, expires_at)
            VALUES (:tenantId, :userId, :key, :hash, :transactionId,
                    :status, :body, now(), now() + make_interval(secs => :ttlSeconds))
            ON CONFLICT (tenant_id, user_id, idem_key) DO UPDATE
               SET request_hash    = EXCLUDED.request_hash,
                   transaction_id  = EXCLUDED.transaction_id,
                   response_status = EXCLUDED.response_status,
                   response_body   = EXCLUDED.response_body,
                   created_at      = EXCLUDED.created_at,
                   expires_at      = EXCLUDED.expires_at
             WHERE idempotency_keys.expires_at <= now()
            """, nativeQuery = true)
    int claim(@Param("tenantId") UUID tenantId,
              @Param("userId") UUID userId,
              @Param("key") String key,
              @Param("hash") String requestHash,
              @Param("transactionId") UUID transactionId,
              @Param("status") int responseStatus,
              @Param("body") String responseBody,
              @Param("ttlSeconds") long ttlSeconds);
}
