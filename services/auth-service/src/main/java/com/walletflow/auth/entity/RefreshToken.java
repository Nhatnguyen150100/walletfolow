package com.walletflow.auth.entity;

import com.walletflow.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * Một refresh token đã phát ra.
 *
 * <p><b>Lưu hash, không lưu token.</b> Giá trị thật chỉ tồn tại một lần duy nhất trong response trả
 * về client. Nếu database bị đọc trộm (backup rò rỉ, SQL injection, nhân viên cũ), kẻ tấn công cầm
 * được bảng này vẫn không có token dùng được — đúng lý do ta không lưu mật khẩu dạng thô.
 *
 * <p><b>{@code familyId} là cơ chế phát hiện token bị đánh cắp.</b> Mỗi lần refresh sinh token mới
 * và thu hồi token cũ (rotation). Nếu một token ĐÃ BỊ THU HỒI lại được dùng, chỉ có một cách giải
 * thích hợp lý: nó đã bị sao chép, vì người dùng thật đã cầm token mới rồi. Lúc đó thu hồi toàn bộ
 * family — cả kẻ tấn công lẫn người dùng thật đều phải đăng nhập lại, và người dùng thật sẽ biết có
 * chuyện bất thường.
 */
@Entity
@Table(name = "refresh_tokens")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RefreshToken extends BaseEntity {

  @Column(name = "user_id", nullable = false)
  private UUID userId;

  /** SHA-256 hex của token. Không bao giờ lưu giá trị thật. */
  @Column(name = "token_hash", nullable = false, length = 64)
  private String tokenHash;

  /** Gom mọi token sinh ra từ một lần đăng nhập. */
  @Column(name = "family_id", nullable = false)
  private UUID familyId;

  @Column(name = "expires_at", nullable = false)
  private Instant expiresAt;

  @Column(name = "revoked_at")
  private Instant revokedAt;

  @Column(name = "revoked_reason", length = 32)
  private String revokedReason;

  @Column(name = "user_agent", length = 255)
  private String userAgent;

  @Column(length = 45)
  private String ip;

  public boolean isUsable() {
    return revokedAt == null && expiresAt.isAfter(Instant.now());
  }
}
