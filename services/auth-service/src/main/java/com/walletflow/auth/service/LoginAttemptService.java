package com.walletflow.auth.service;

import com.walletflow.auth.entity.User;
import com.walletflow.auth.enums.EUserStatus;
import com.walletflow.auth.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Đếm số lần đăng nhập sai và khoá tài khoản tạm thời.
 *
 * <p><b>Vì sao phải là một bean riêng với {@link Propagation#REQUIRES_NEW}:</b> đường đi của một
 * lần đăng nhập sai kết thúc bằng một exception. Nếu việc tăng bộ đếm nằm trong cùng transaction
 * với luồng đăng nhập, exception đó sẽ rollback luôn bộ đếm — và bộ đếm vĩnh viễn ở 0, tức là
 * chống brute-force hoàn toàn không hoạt động trong khi code trông vẫn rất đúng. Đây là một lỗi
 * câm kinh điển, và nó chỉ lộ ra nếu có test thử sai mật khẩu nhiều lần rồi kiểm tra DB.
 *
 * <p>Tách bean cũng là bắt buộc về mặt kỹ thuật: gọi một method {@code @Transactional} từ chính
 * bean đó (self-invocation) không đi qua proxy nên annotation bị bỏ qua hoàn toàn.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LoginAttemptService {

  /** Đủ rộng để không làm phiền người gõ nhầm, đủ hẹp để dò mật khẩu là vô vọng. */
  public static final int MAX_FAILED_ATTEMPTS = 5;

  /**
   * Khoá TẠM THỜI chứ không khoá vĩnh viễn: khoá vĩnh viễn biến chính cơ chế bảo vệ thành một công
   * cụ tấn công — kẻ xấu chỉ cần gõ sai 5 lần với email của người khác là khoá được tài khoản đó.
   */
  public static final Duration LOCK_DURATION = Duration.ofMinutes(15);

  private final UserRepository userRepository;
  private final AuditService auditService;

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void recordFailure(UUID userId, String email, String ip) {
    userRepository.findById(userId).ifPresent(user -> {
      user.setFailedLoginCount(user.getFailedLoginCount() + 1);

      if (user.getFailedLoginCount() >= MAX_FAILED_ATTEMPTS) {
        user.setLockedUntil(Instant.now().plus(LOCK_DURATION));
        user.setStatus(EUserStatus.LOCKED);
        user.setFailedLoginCount(0);   // đếm lại cho chu kỳ sau
        userRepository.save(user);
        log.warn("Khoá tài khoản {} trong {} vì sai mật khẩu {} lần",
            email, LOCK_DURATION, MAX_FAILED_ATTEMPTS);
        auditService.record("ACCOUNT_LOCKED", userId, email, ip,
            "Khoá " + LOCK_DURATION.toMinutes() + " phút sau " + MAX_FAILED_ATTEMPTS + " lần sai");
        return;
      }

      userRepository.save(user);
      auditService.record("LOGIN_FAILED", userId, email, ip,
          "Lần sai thứ " + user.getFailedLoginCount());
    });
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void recordSuccess(UUID userId) {
    userRepository.findById(userId).ifPresent(user -> {
      if (user.getFailedLoginCount() != 0 || user.getLockedUntil() != null) {
        user.setFailedLoginCount(0);
        user.setLockedUntil(null);
        user.setStatus(EUserStatus.ACTIVE);
        userRepository.save(user);
      }
    });
  }
}
