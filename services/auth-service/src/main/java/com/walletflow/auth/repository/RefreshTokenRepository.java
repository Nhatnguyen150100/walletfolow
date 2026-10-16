package com.walletflow.auth.repository;

import com.walletflow.auth.entity.RefreshToken;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface RefreshTokenRepository extends JpaRepository<RefreshToken, UUID> {

  Optional<RefreshToken> findByTokenHash(String tokenHash);

  /**
   * Thu hồi cả một family trong MỘT câu lệnh.
   *
   * <p>Dùng bulk update chứ không nạp từng entity rồi set: khi phát hiện token bị đánh cắp, thời
   * gian từ lúc phát hiện tới lúc mọi token chết phải càng ngắn càng tốt. Đọc-rồi-ghi từng hàng
   * mở ra một khoảng mà kẻ tấn công vẫn refresh được.
   */
  @Modifying(clearAutomatically = true, flushAutomatically = true)
  @Query("""
      UPDATE RefreshToken t
         SET t.revokedAt = :now, t.revokedReason = :reason
       WHERE t.familyId = :familyId AND t.revokedAt IS NULL
      """)
  int revokeFamily(@Param("familyId") UUID familyId,
                   @Param("reason") String reason,
                   @Param("now") Instant now);

  @Modifying(clearAutomatically = true, flushAutomatically = true)
  @Query("""
      UPDATE RefreshToken t
         SET t.revokedAt = :now, t.revokedReason = :reason
       WHERE t.userId = :userId AND t.revokedAt IS NULL
      """)
  int revokeAllForUser(@Param("userId") UUID userId,
                       @Param("reason") String reason,
                       @Param("now") Instant now);

  /** Dọn token đã hết hạn từ lâu — chúng không còn giá trị kể cả để điều tra. */
  @Modifying
  @Query("DELETE FROM RefreshToken t WHERE t.expiresAt < :cutoff")
  int deleteExpiredBefore(@Param("cutoff") Instant cutoff);
}
