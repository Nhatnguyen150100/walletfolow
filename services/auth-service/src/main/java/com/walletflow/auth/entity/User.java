package com.walletflow.auth.entity;

import com.walletflow.auth.enums.ERole;
import com.walletflow.auth.enums.EUserStatus;
import com.walletflow.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "users")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class User extends BaseEntity {

  /**
   * Tenant sở hữu người dùng — đi vào JWT dưới dạng claim {@code tid}, từ đó thành header
   * {@code X-Tenant-Id} ở gateway và {@code TenantContext} ở service nội bộ. Không đổi được sau khi
   * tạo: chuyển người dùng sang tenant khác nghĩa là ví và sổ sách của họ đổi chủ.
   */
  @Column(name = "tenant_id", nullable = false, updatable = false)
  private UUID tenantId;

  /**
   * Luôn lưu dạng CHỮ THƯỜNG. Unique index là {@code lower(email)}, nên nếu tầng ứng dụng lưu
   * nguyên dạng người dùng gõ thì {@code findByEmail} sẽ trượt với chính email vừa đăng ký.
   */
  @Column(nullable = false)
  private String email;

  @Column(nullable = false)
  private String password;

  private String fullName;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  @Builder.Default
  private ERole role = ERole.USER;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false)
  @Builder.Default
  private EUserStatus status = EUserStatus.ACTIVE;

  /** Đếm số lần đăng nhập sai liên tiếp; về 0 sau mỗi lần đăng nhập thành công. */
  @Column(nullable = false)
  @Builder.Default
  private int failedLoginCount = 0;

  private Instant lockedUntil;

  /**
   * Dùng để vô hiệu hoá refresh token phát trước khi đổi mật khẩu: đổi mật khẩu phải đá mọi phiên
   * đang mở ra ngoài, nếu không thì việc đổi mật khẩu sau khi bị chiếm tài khoản là vô nghĩa.
   */
  @Column(nullable = false)
  @Builder.Default
  private Instant passwordChangedAt = Instant.now();

  /** Tài khoản có đang bị khoá tại thời điểm này không (khoá tạm hết hạn thì tự mở). */
  public boolean isLockedNow() {
    if (status == EUserStatus.DISABLED) {
      return true;
    }
    return lockedUntil != null && lockedUntil.isAfter(Instant.now());
  }
}
