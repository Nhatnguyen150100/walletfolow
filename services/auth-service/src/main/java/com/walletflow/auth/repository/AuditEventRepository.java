package com.walletflow.auth.repository;

import com.walletflow.auth.entity.AuditEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * Chỉ dùng để GHI và ĐỌC. Trigger ở DB chặn UPDATE/DELETE nên các phương thức sửa/xoá kế thừa từ
 * {@link JpaRepository} sẽ ném lỗi nếu ai đó gọi — đúng như mong muốn.
 */
public interface AuditEventRepository extends JpaRepository<AuditEvent, UUID> {
}
