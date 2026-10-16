package com.walletflow.auth.service;

import com.walletflow.auth.entity.RefreshToken;
import com.walletflow.auth.repository.RefreshTokenRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

/**
 * Phát hành, xoay vòng và thu hồi refresh token.
 *
 * <p>Refresh token là thứ giữ cho phiên đăng nhập sống lâu, nên nó là mục tiêu có giá trị nhất đối
 * với kẻ tấn công. Hai cơ chế bảo vệ ở đây:
 *
 * <ol>
 *   <li><b>Xoay vòng (rotation):</b> mỗi lần refresh sinh token mới và thu hồi token vừa dùng. Một
 *       token chỉ dùng được đúng một lần, nên cửa sổ khai thác của một token bị lộ rất hẹp.</li>
 *   <li><b>Phát hiện tái sử dụng:</b> nếu một token ĐÃ THU HỒI được dùng lại, đó là bằng chứng nó
 *       đã bị sao chép — người dùng thật đã cầm token mới rồi. Phản ứng là thu hồi TOÀN BỘ family,
 *       buộc cả hai bên đăng nhập lại. Không có bước này thì rotation chỉ làm phiền kẻ tấn công
 *       chứ không chặn được: hắn cứ việc dùng token cũ và xoay tiếp chuỗi của riêng mình.</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RefreshTokenService {

  public static final String REASON_ROTATED = "ROTATED";
  public static final String REASON_LOGOUT = "LOGOUT";
  public static final String REASON_REUSE_DETECTED = "REUSE_DETECTED";

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int TOKEN_BYTES = 32;   // 256 bit — không đoán được

  private final RefreshTokenRepository refreshTokenRepository;

  @Value("${jwt.refresh-ttl-days}")
  private int refreshTtlDays;

  /** Giá trị thô CHỈ tồn tại ở đây và trong response; database chỉ giữ hash của nó. */
  public record Issued(String rawToken, RefreshToken stored) {
  }

  @Transactional
  public Issued issue(UUID userId, UUID familyId, String userAgent, String ip) {
    byte[] bytes = new byte[TOKEN_BYTES];
    RANDOM.nextBytes(bytes);
    String rawToken = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);

    RefreshToken stored = refreshTokenRepository.save(RefreshToken.builder()
        .userId(userId)
        .familyId(familyId)
        .tokenHash(hash(rawToken))
        .expiresAt(Instant.now().plus(Duration.ofDays(refreshTtlDays)))
        .userAgent(truncate(userAgent, 255))
        .ip(truncate(ip, 45))
        .build());

    return new Issued(rawToken, stored);
  }

  public Optional<RefreshToken> findByRawToken(String rawToken) {
    return refreshTokenRepository.findByTokenHash(hash(rawToken));
  }

  @Transactional
  public void revoke(RefreshToken token, String reason) {
    token.setRevokedAt(Instant.now());
    token.setRevokedReason(reason);
    refreshTokenRepository.save(token);
  }

  /**
   * Phản ứng khi phát hiện token bị tái sử dụng: giết cả chuỗi.
   *
   * <p><b>{@link Propagation#REQUIRES_NEW} ở đây là bắt buộc, không phải tuỳ chọn.</b> Nơi gọi
   * phương thức này luôn ném exception ngay sau đó (phát hiện tái dùng → 401, mật khẩu đã đổi →
   * 401). Nếu việc thu hồi nằm chung transaction với luồng request, exception đó sẽ rollback
   * luôn lệnh thu hồi — các token vẫn sống nguyên, trong khi log và response đều nói rằng chuỗi
   * đã bị giết. Cơ chế chống trộm token sẽ không hoạt động, một cách hoàn toàn im lặng.
   *
   * <p>Cùng một cái bẫy với bộ đếm đăng nhập sai trong {@code LoginAttemptService}: mọi tác dụng
   * phụ về BẢO MẬT xảy ra trên đường đi kết thúc bằng exception đều phải có transaction riêng.
   *
   * @return số token bị thu hồi — đưa vào audit để biết quy mô
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public int revokeFamily(UUID familyId, String reason) {
    int revoked = refreshTokenRepository.revokeFamily(familyId, reason, Instant.now());
    log.warn("Đã thu hồi {} refresh token của family {} (lý do: {})", revoked, familyId, reason);
    return revoked;
  }

  /** Cùng lý do với {@link #revokeFamily}: thu hồi phải sống sót kể cả khi luồng gọi thất bại. */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public int revokeAllForUser(UUID userId, String reason) {
    return refreshTokenRepository.revokeAllForUser(userId, reason, Instant.now());
  }

  /**
   * So sánh hash chứ không so sánh token: database không bao giờ giữ giá trị dùng được.
   *
   * <p>SHA-256 không muối là ĐỦ ở đây, khác với mật khẩu. Token là 256 bit ngẫu nhiên từ
   * {@link SecureRandom} nên không có "từ điển" để dò và không có rainbow table nào phủ được không
   * gian đó. Dùng BCrypt ở đây chỉ làm mỗi lần refresh chậm thêm hàng trăm mili giây mà không mua
   * về thêm gì.
   */
  private static String hash(String rawToken) {
    try {
      return HexFormat.of().formatHex(
          MessageDigest.getInstance("SHA-256").digest(rawToken.getBytes()));
    } catch (Exception e) {
      throw new IllegalStateException("Không băm được refresh token", e);
    }
  }

  private static String truncate(String value, int max) {
    if (value == null) {
      return null;
    }
    return value.length() <= max ? value : value.substring(0, max);
  }
}
