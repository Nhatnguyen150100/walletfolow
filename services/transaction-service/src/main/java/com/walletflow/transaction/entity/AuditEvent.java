package com.walletflow.transaction.entity;

import com.walletflow.common.entity.ImmutableEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.util.UUID;

/**
 * Dấu vết một thao tác của người vận hành trên giao dịch (roadmap 2.5, ADR-021). APPEND-ONLY: kế
 * thừa {@link ImmutableEntity} (không setter, không {@code updatedAt}) và trigger
 * {@code trg_audit_events_append_only} ở DB chặn UPDATE/DELETE — xem {@code V5__audit_events.sql}.
 *
 * <p>Khác bản của auth-service ở hai cột, cả hai đều có lý do: {@code tenantId} (mọi hàng nghiệp vụ
 * đều thuộc một tenant — ADR-010, và câu hỏi kiểm toán luôn có dạng "ai đã đụng dữ liệu của tenant
 * nào") và {@code actorRole} (vai LÚC THAO TÁC; vai đổi theo thời gian nên đọc lại từ {@code users}
 * về sau là đọc một sự thật khác).
 */
@Entity
@Table(name = "audit_events")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditEvent extends ImmutableEntity {

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "actor_id", nullable = false)
    private UUID actorId;

    @Column(name = "actor_email", length = 255)
    private String actorEmail;

    /** Vai của người thao tác lúc đó: {@code ADMIN}, {@code OPS}, {@code AUDITOR}. */
    @Column(name = "actor_role", nullable = false, length = 16)
    private String actorRole;

    /** Vd {@code LIST_MANUAL_REVIEW}, {@code VIEW_TRANSACTION}. */
    @Column(nullable = false, length = 64)
    private String action;

    /** Thứ bị tác động — thường là id giao dịch. Null khi thao tác không nhắm vào một hàng cụ thể. */
    @Column(length = 255)
    private String target;

    @Column(length = 45)
    private String ip;

    /**
     * Ngữ cảnh thêm, JSON. KHÔNG được chứa token, mật khẩu, hay dữ liệu thanh toán — bảng này
     * không xoá được, nên thứ lỡ ghi vào đây là ở lại vĩnh viễn.
     *
     * <p>{@code @JdbcTypeCode(JSON)} là bắt buộc: thiếu nó Hibernate bind chuỗi thành
     * {@code varchar} và Postgres từ chối gán vào cột {@code jsonb} ngay lúc INSERT.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String detail;
}
