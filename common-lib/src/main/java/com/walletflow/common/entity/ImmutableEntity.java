package com.walletflow.common.entity;

import jakarta.persistence.Column;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import lombok.Getter;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.Instant;
import java.util.UUID;

/**
 * Entity cơ sở cho dữ liệu <b>APPEND-ONLY</b>: khoá chính UUID + {@code createdAt}, và CHỦ Ý
 * KHÔNG có {@code updatedAt} lẫn setter.
 *
 * <p>Vì sao tách khỏi {@link BaseEntity}: sổ cái là audit trail (plan §3, §14) — bút toán một khi
 * đã ghi thì không bao giờ được sửa hay xoá. Một cột {@code updated_at} trên bảng bất biến là mâu
 * thuẫn tự thân: nó ngụ ý "hàng này có thể đổi", trong khi bất biến bảo toàn tiền dựa trên điều
 * ngược lại. Sai lệch nghiệp vụ được sửa bằng cách ghi thêm một bút toán {@code REVERSAL}, chứ
 * không phải sửa bút toán cũ.
 *
 * <p>Đây là tầng phòng thủ thứ nhất (kiểu dữ liệu). Tầng thứ hai là interface repository chỉ lộ ra
 * {@code save}/đọc, và tầng cuối cùng — chốt chặn thật — là trigger chặn UPDATE/DELETE ở DB.
 */
@Getter
@MappedSuperclass
@EntityListeners(AuditingEntityListener.class)
public abstract class ImmutableEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @CreatedDate
    @Column(updatable = false)
    private Instant createdAt;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ImmutableEntity other)) return false;
        return id != null && id.equals(other.getId());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
