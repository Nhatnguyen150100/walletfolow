package com.walletflow.ledger.services;

import com.walletflow.common.money.ECurrency;
import com.walletflow.ledger.enums.ETypeAccount;
import com.walletflow.ledger.repositories.AccountRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Cấp tài khoản hệ thống cho từng tenant (roadmap 1.0b).
 *
 * <p>Tài khoản hệ thống là PER-TENANT (V4): {@code EXTERNAL_BANK} của khách A và của khách B là hai
 * tài khoản khác nhau. Dùng chung thì số dư nostro của hai khách trộn vào nhau — bất biến {@code
 * SUM = 0} vẫn đúng toàn cục trong khi sổ của từng khách đã sai, và không đối soát được.
 *
 * <p>Idempotent: chạy lại bao nhiêu lần cũng chỉ tạo phần còn thiếu. Hiện chạy lúc khởi động cho
 * các tenant trong cấu hình {@code walletflow.ledger.provision-tenants}; Phase 5 gọi lại đúng
 * {@link #provision} từ API onboarding tenant.
 */
@Slf4j
@Service
public class SystemAccountProvisioner {

    /** Tài khoản hệ thống mà mọi tenant phải có, cho MỖI loại tiền. */
    static final List<String> SYSTEM_ACCOUNTS = List.of("EXTERNAL_BANK", "FEE_INCOME", "SETTLEMENT");

    private final AccountRepository accountRepository;
    private final List<UUID> startupTenants;

    public SystemAccountProvisioner(
            AccountRepository accountRepository,
            @Value("${walletflow.ledger.provision-tenants:}") List<UUID> startupTenants) {
        this.accountRepository = accountRepository;
        this.startupTenants = startupTenants;
    }

    /**
     * Nhận {@code tenantId} TƯỜNG MINH thay vì đọc {@code TenantContext}: đây là thao tác quản trị
     * trên một tenant cụ thể, không phải xử lý một request của tenant đó.
     *
     * @return số tài khoản vừa được tạo (0 nếu tenant đã đủ)
     */
    @Transactional
    public int provision(UUID tenantId) {
        int created = 0;
        for (ECurrency currency : ECurrency.values()) {
            for (String ref : SYSTEM_ACCOUNTS) {
                created += accountRepository.insertIfAbsent(
                        tenantId, ref, ETypeAccount.SYSTEM.name(), currency.name());
            }
        }
        if (created > 0) {
            log.info("Ledger: cấp {} tài khoản hệ thống cho tenant {}", created, tenantId);
        }
        return created;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void provisionConfiguredTenants() {
        startupTenants.forEach(this::provision);
    }
}
