package com.walletflow.transaction.repositories;

import com.walletflow.transaction.entity.AuditEvent;
import org.springframework.data.repository.Repository;

import java.util.UUID;

/**
 * Chỉ GHI. Không có {@code delete}, không có đường sửa — tầng phòng thủ thứ hai cho bảng
 * append-only (tầng một là {@link AuditEvent} không có setter, tầng cuối là trigger ở DB).
 *
 * <p>Chưa có hàm đọc: 2.5 PR1 chưa có màn hình tra cứu audit. Thêm khi có người cần, và lúc đó
 * nhớ lọc tenant.
 */
public interface AuditEventRepository extends Repository<AuditEvent, UUID> {

    AuditEvent save(AuditEvent event);
}
