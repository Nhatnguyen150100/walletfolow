package com.walletflow.auth.service;

import com.walletflow.auth.dto.AuthResponse;
import com.walletflow.auth.dto.LoginRequest;
import com.walletflow.auth.dto.RegisterRequest;
import com.walletflow.auth.entity.RefreshToken;
import com.walletflow.auth.entity.User;
import com.walletflow.auth.enums.ERole;
import com.walletflow.auth.repository.UserRepository;
import com.walletflow.common.exception.ConflictException;
import com.walletflow.common.exception.UnauthorizedException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {

  private final UserRepository userRepository;
  private final PasswordEncoder passwordEncoder;
  private final JwtService jwtService;
  private final RefreshTokenService refreshTokenService;
  private final LoginAttemptService loginAttemptService;
  private final AuditService auditService;

  /**
   * Tenant mà endpoint đăng ký/đăng nhập công khai phục vụ.
   *
   * <p><b>Lấy từ cấu hình, KHÔNG từ request.</b> Đăng ký và đăng nhập là path công khai — chưa có
   * token nào để tin. Nhận tenant từ header hay body nghĩa là ai cũng tự xưng được là khách của
   * tenant bất kỳ và tạo tài khoản trong đó. Phase 5 thay bằng tenant suy ra từ API key của doanh
   * nghiệp (thứ đã được xác thực), không bao giờ bằng một giá trị client tự khai.
   */
  @Value("${auth.tenant-id}")
  private UUID tenantId;

  @Transactional
  public AuthResponse register(RegisterRequest req, String ip, String userAgent) {
    String email = normalize(req.email());

    if (userRepository.existsByTenantIdAndEmail(tenantId, email)) {
      throw new ConflictException("EMAIL_TAKEN", "Email đã được đăng ký");
    }

    User user = userRepository.save(User.builder()
        .tenantId(tenantId)
        .email(email)
        .password(passwordEncoder.encode(req.password()))
        .fullName(req.fullName())
        .role(ERole.USER)
        .build());

    log.info("Đăng ký user mới: {}", email);
    auditService.record("REGISTER", user.getId(), email, ip, null);

    return issueTokens(user, UUID.randomUUID(), ip, userAgent);
  }

  /**
   * Đăng nhập.
   *
   * <p>Mọi đường thất bại — email không tồn tại, sai mật khẩu, tài khoản bị khoá — đều trả về
   * CÙNG một thông báo. Phân biệt "email không tồn tại" với "sai mật khẩu" biến endpoint này thành
   * công cụ liệt kê tài khoản: kẻ tấn công thử một danh sách email và biết chính xác email nào có
   * trên hệ thống.
   */
  @Transactional
  public AuthResponse login(LoginRequest req, String ip, String userAgent) {
    String email = normalize(req.email());

    User user = userRepository.findByTenantIdAndEmail(tenantId, email)
        .orElseThrow(() -> {
          auditService.record("LOGIN_FAILED_UNKNOWN_EMAIL", null, email, ip, null);
          return invalidCredentials();
        });

    if (user.isLockedNow()) {
      auditService.record("LOGIN_BLOCKED_LOCKED", user.getId(), email, ip, null);
      throw new UnauthorizedException("ACCOUNT_LOCKED",
          "Tài khoản đang tạm khoá do đăng nhập sai nhiều lần. Thử lại sau ít phút.");
    }

    if (!passwordEncoder.matches(req.password(), user.getPassword())) {
      // Chạy ở transaction RIÊNG để bộ đếm sống sót qua exception bên dưới — xem LoginAttemptService
      loginAttemptService.recordFailure(user.getId(), email, ip);
      throw invalidCredentials();
    }

    loginAttemptService.recordSuccess(user.getId());
    auditService.record("LOGIN_SUCCESS", user.getId(), email, ip, null);

    return issueTokens(user, UUID.randomUUID(), ip, userAgent);
  }

  /**
   * Đổi refresh token lấy cặp token mới (rotation).
   *
   * <p>Đây là nơi cơ chế phát hiện token bị đánh cắp hoạt động — xem {@link RefreshTokenService}.
   */
  @Transactional
  public AuthResponse refresh(String rawToken, String ip, String userAgent) {
    RefreshToken token = refreshTokenService.findByRawToken(rawToken)
        .orElseThrow(() -> new UnauthorizedException("INVALID_REFRESH_TOKEN",
            "Refresh token không hợp lệ"));

    // Token đã thu hồi mà vẫn được dùng => đã bị sao chép. Người dùng thật đã có token mới rồi.
    if (token.getRevokedAt() != null) {
      int revoked = refreshTokenService.revokeFamily(
          token.getFamilyId(), RefreshTokenService.REASON_REUSE_DETECTED);
      auditService.record("TOKEN_REUSE_DETECTED", token.getUserId(), null, ip,
          "Thu hồi toàn bộ family " + token.getFamilyId() + " (" + revoked + " token)");
      throw new UnauthorizedException("TOKEN_REUSE_DETECTED",
          "Phiên đăng nhập đã bị thu hồi vì phát hiện dấu hiệu bất thường. Vui lòng đăng nhập lại.");
    }

    if (!token.isUsable()) {
      throw new UnauthorizedException("REFRESH_TOKEN_EXPIRED", "Refresh token đã hết hạn");
    }

    User user = userRepository.findById(token.getUserId())
        .orElseThrow(() -> new UnauthorizedException("Tài khoản không còn tồn tại"));

    if (user.isLockedNow()) {
      throw new UnauthorizedException("ACCOUNT_LOCKED", "Tài khoản đang bị khoá");
    }

    // Đổi mật khẩu phải đá mọi phiên mở trước đó ra ngoài, nếu không thì việc đổi mật khẩu sau khi
    // bị chiếm tài khoản là vô nghĩa — kẻ tấn công vẫn giữ nguyên phiên của hắn.
    if (user.getPasswordChangedAt().isAfter(token.getCreatedAt())) {
      refreshTokenService.revokeFamily(token.getFamilyId(), "PASSWORD_CHANGED");
      throw new UnauthorizedException("PASSWORD_CHANGED",
          "Mật khẩu đã thay đổi, vui lòng đăng nhập lại");
    }

    refreshTokenService.revoke(token, RefreshTokenService.REASON_ROTATED);

    // Giữ nguyên familyId: cả chuỗi vẫn thuộc về một lần đăng nhập, nên phát hiện tái dùng về sau
    // vẫn giết được đúng chuỗi đó.
    return issueTokens(user, token.getFamilyId(), ip, userAgent);
  }

  @Transactional
  public void logout(String rawToken) {
    refreshTokenService.findByRawToken(rawToken).ifPresent(token -> {
      // Thu hồi cả family chứ không riêng token đang cầm: người dùng bấm "đăng xuất" là muốn phiên
      // đó kết thúc, không phải muốn vô hiệu đúng một mắt xích trong chuỗi.
      refreshTokenService.revokeFamily(token.getFamilyId(), RefreshTokenService.REASON_LOGOUT);
      auditService.record("LOGOUT", token.getUserId(), null, null, null);
    });
    // Token không tồn tại cũng trả về thành công: logout phải là thao tác idempotent, và việc
    // báo "token này không tồn tại" chỉ giúp kẻ tấn công dò token hợp lệ.
  }

  @Transactional
  public void logoutAll(UUID userId) {
    int revoked = refreshTokenService.revokeAllForUser(userId, RefreshTokenService.REASON_LOGOUT);
    auditService.record("LOGOUT_ALL", userId, null, null, "Thu hồi " + revoked + " token");
  }

  private AuthResponse issueTokens(User user, UUID familyId, String ip, String userAgent) {
    String accessToken = jwtService.generateAccessToken(user);
    RefreshTokenService.Issued refresh =
        refreshTokenService.issue(user.getId(), familyId, userAgent, ip);

    return AuthResponse.of(
        user.getId(), user.getEmail(), user.getFullName(), user.getRole().name(),
        accessToken, refresh.rawToken(), jwtService.accessTokenTtl().toSeconds());
  }

  /** Email không phân biệt hoa thường — phải khớp với unique index {@code lower(email)}. */
  private static String normalize(String email) {
    return email == null ? null : email.trim().toLowerCase(Locale.ROOT);
  }

  private static UnauthorizedException invalidCredentials() {
    return new UnauthorizedException("INVALID_CREDENTIALS", "Email hoặc mật khẩu không đúng");
  }

  /** Dùng cho test: xác nhận mốc thời gian đổi mật khẩu có hiệu lực thu hồi token. */
  Instant passwordChangedAtOf(UUID userId) {
    return userRepository.findById(userId).map(User::getPasswordChangedAt).orElse(null);
  }
}
