package com.walletflow.transaction.services;

import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.entity.AuditEvent;
import com.walletflow.transaction.repositories.AuditEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Ghi một bản ghi audit trong transaction CỦA RIÊNG NÓ.
 *
 * <p>Tách thành bean riêng chứ không phải một method của {@link AuditService} — và đó là điểm
 * chính: {@code REQUIRES_NEW} commit khi lời gọi qua PROXY trả về, nên một {@code try/catch} đặt
 * quanh {@code save()} bên trong cùng bean không bao giờ nhìn thấy lỗi lúc commit (ràng buộc DB,
 * kiểu cột sai, trigger). Lỗi đó sẽ ném thẳng lên người gọi và làm hỏng chính việc mà audit chỉ đi
 * kèm. Hai bean thì ranh giới transaction nằm GIỮA chúng, và {@link AuditService} bắt được.
 */
@Component
@RequiredArgsConstructor
public class AuditWriter {

    private final AuditEventRepository repository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void write(Operator actor, String action, String target, String detailJson) {
        repository.save(AuditEvent.builder()
                .tenantId(TenantContext.require())
                .actorId(actor.id())
                .actorEmail(actor.email())
                .actorRole(actor.role())
                .action(action)
                .target(target)
                .ip(actor.ip())
                .detail(detailJson)
                .build());
    }
}
