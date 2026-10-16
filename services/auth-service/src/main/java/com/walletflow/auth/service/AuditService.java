package com.walletflow.auth.service;

import com.walletflow.auth.entity.AuditEvent;
import com.walletflow.auth.repository.AuditEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Ghi audit log cho các hành động nhạy cảm về bảo mật.
 *
 * <p>Dùng {@link Propagation#REQUIRES_NEW}: audit phải sống SÓT kể cả khi giao dịch nghiệp vụ
 * rollback. Những sự kiện đáng ghi nhất — đăng nhập thất bại, phát hiện token bị tái dùng — xảy ra
 * đúng trên những đường đi kết thúc bằng một exception. Nếu audit nằm chung transaction, nó sẽ bị
 * cuốn theo rollback và biến mất đúng lúc cần nhất.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

  private final AuditEventRepository auditEventRepository;

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void record(String action, UUID actorId, String actorEmail, String ip, String detail) {
    try {
      auditEventRepository.save(AuditEvent.builder()
          .action(action)
          .actorId(actorId)
          .actorEmail(actorEmail)
          .ip(ip)
          .detail(detail)
          .build());
    } catch (Exception e) {
      // Không để lỗi ghi audit chặn luồng nghiệp vụ, nhưng phải kêu to: mất audit là mất khả năng
      // điều tra, và đó là sự cố theo đúng nghĩa.
      log.error("Không ghi được audit event action={} actor={}", action, actorEmail, e);
    }
  }
}
