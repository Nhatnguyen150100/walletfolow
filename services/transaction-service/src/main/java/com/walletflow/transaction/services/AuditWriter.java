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

    /**
     * Ghi dấu vết trong CHÍNH transaction của người gọi (2.5 PR2) — dùng cho thao tác ĐỤNG TIỀN.
     *
     * <p>{@code MANDATORY}: không có transaction đang mở thì ném, chứ không âm thầm mở một cái mới.
     * Đó là điểm của nó — "không có thao tác đụng tiền nào không để lại audit" chỉ là bất biến khi
     * hai việc cùng commit hoặc cùng rollback. Và KHÔNG bắt lỗi: audit hỏng thì tiền cũng không được
     * ghi. Đây là chỗ khác hẳn {@link AuditService}, vốn nuốt lỗi vì nó phục vụ thao tác ĐỌC.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void writeInCurrentTransaction(Operator actor, String action, String target,
                                          String detailJson) {
        save(actor, action, target, detailJson);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void write(Operator actor, String action, String target, String detailJson) {
        save(actor, action, target, detailJson);
    }

    private void save(Operator actor, String action, String target, String detailJson) {
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
