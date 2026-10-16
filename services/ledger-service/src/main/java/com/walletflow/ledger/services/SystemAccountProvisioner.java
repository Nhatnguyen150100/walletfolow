package com.walletflow.ledger.services;

import com.walletflow.common.money.ECurrency;
import com.walletflow.ledger.enums.ETypeAccount;
import com.walletflow.ledger.repositories.AccountRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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
public class SystemAccountProvisioner implements SmartInitializingSingleton {

    /** Tài khoản hệ thống mà mọi tenant phải có, cho MỖI loại tiền. */
    static final List<String> SYSTEM_ACCOUNTS = List.of("EXTERNAL_BANK", "FEE_INCOME", "SETTLEMENT");

    private final AccountRepository accountRepository;
    private final TransactionTemplate transactionTemplate;
    private final List<UUID> startupTenants;

    public SystemAccountProvisioner(
            AccountRepository accountRepository,
            PlatformTransactionManager transactionManager,
            @Value("${walletflow.ledger.provision-tenants:}") List<UUID> startupTenants) {
        this.accountRepository = accountRepository;
        // TransactionTemplate thay vì @Transactional: provision() được gọi cả từ chính class này lúc
        // khởi động — lời gọi nội bộ đi vòng qua proxy và @Transactional bị bỏ qua âm thầm.
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.startupTenants = startupTenants;
    }

    /**
     * Cấp đủ bộ tài khoản hệ thống cho một tenant, trong MỘT transaction: tenant hoặc có đủ, hoặc
     * không có gì — không bao giờ có nửa bộ.
     *
     * <p>Nhận {@code tenantId} TƯỜNG MINH thay vì đọc {@code TenantContext}: đây là thao tác quản trị
     * trên một tenant cụ thể, không phải xử lý một request của tenant đó.
     *
     * @return số tài khoản vừa được tạo (0 nếu tenant đã đủ)
     */
    public int provision(UUID tenantId) {
        Integer created = transactionTemplate.execute(status -> {
            int count = 0;
            for (ECurrency currency : ECurrency.values()) {
                for (String ref : SYSTEM_ACCOUNTS) {
                    count += accountRepository.insertIfAbsent(
                            tenantId, ref, ETypeAccount.SYSTEM.name(), currency.name());
                }
            }
            return count;
        });
        if (created != null && created > 0) {
            log.info("Ledger: cấp {} tài khoản hệ thống cho tenant {}", created, tenantId);
        }
        return created == null ? 0 : created;
    }

    /**
     * Chạy khi mọi singleton đã tạo xong (Flyway đã migrate, DataSource/JPA sẵn sàng) nhưng TRƯỚC khi
     * các Kafka listener container khởi động — chúng là {@code SmartLifecycle}, start sau bước này.
     *
     * <p>Không dùng {@code ApplicationReadyEvent}: event đó đến SAU khi listener đã chạy. Trong khe hở
     * đó, một {@code PostJournal} của tenant vừa được thêm vào cấu hình gặp "Account not found:
     * EXTERNAL_BANK" — một {@code AppException}, nên ledger TỪ CHỐI DỨT KHOÁT thay vì thử lại, và một
     * giao dịch hợp lệ thất bại chỉ vì đến sớm vài trăm mili giây.
     */
    @Override
    public void afterSingletonsInstantiated() {
        startupTenants.forEach(this::provision);
    }
}
