package com.walletflow.auth.entity;

import com.walletflow.common.entity.ImmutableEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Bản ghi audit, APPEND-ONLY (kế thừa {@link ImmutableEntity}: không setter, không
 * {@code updatedAt}; trigger ở DB chặn UPDATE/DELETE — xem {@code V1__init_auth.sql}).
 *
 * <p>Giá trị của audit log nằm ở chỗ nó không sửa được. Một audit log chỉnh sửa được không trả lời
 * được câu hỏi duy nhất mà người ta cần nó trả lời: "chuyện gì đã thực sự xảy ra?".
 */
@Entity
@Table(name = "audit_events")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditEvent extends ImmutableEntity {

  @Column(name = "actor_id")
  private UUID actorId;

  @Column(name = "actor_email", length = 255)
  private String actorEmail;

  /** Vd {@code LOGIN_SUCCESS}, {@code LOGIN_FAILED}, {@code ACCOUNT_LOCKED}, {@code TOKEN_REUSE_DETECTED}. */
  @Column(nullable = false, length = 64)
  private String action;

  @Column(length = 255)
  private String target;

  @Column(length = 45)
  private String ip;

  /** Ngữ cảnh thêm. KHÔNG được chứa mật khẩu, token, hay dữ liệu nhạy cảm. */
  @Column(columnDefinition = "text")
  private String detail;
}
