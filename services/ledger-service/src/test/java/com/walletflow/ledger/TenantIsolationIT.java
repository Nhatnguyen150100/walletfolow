package com.walletflow.ledger;

import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.dto.Leg;
import com.walletflow.ledger.dto.PostJournalCommand;
import com.walletflow.ledger.entity.Account;
import com.walletflow.ledger.entity.JournalEntry;
import com.walletflow.ledger.enums.ECurrency;
import com.walletflow.ledger.enums.EJournalType;
import com.walletflow.ledger.enums.ETypeAccount;
import com.walletflow.ledger.repositories.AccountRepository;
import com.walletflow.ledger.repositories.JournalEntryRepository;
import com.walletflow.ledger.repositories.PostingRepository;
import com.walletflow.ledger.services.LedgerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Chứng minh hai khách hàng dùng chung một sổ cái KHÔNG nhìn thấy nhau.
 *
 * <p>Đây là test biện minh cho toàn bộ công sức thêm {@code tenant_id}. Cô lập tenant là một tính
 * chất <b>âm</b>: nó nói về điều KHÔNG được xảy ra, nên không bao giờ lộ ra khi chạy thử bằng tay
 * với một khách hàng. Một truy vấn quên mệnh đề {@code WHERE tenant_id = ?} chạy hoàn hảo trong
 * mọi bài test một tenant, và chỉ sai vào đúng ngày có khách hàng thứ hai.
 *
 * <p>Bốn tính chất, mỗi cái ứng với một cách rò rỉ khác nhau:
 * <ol>
 *   <li><b>Trùng tên ví:</b> hai tenant cùng đặt ví tên {@code WALLET-001} phải là hai tài khoản
 *       độc lập. Nếu unique key quên tenant, khách thứ hai không tạo nổi ví.</li>
 *   <li><b>Trùng khoá idempotency:</b> cùng {@code transactionId} ở hai tenant phải sinh ra hai
 *       bút toán. Nếu quên tenant, lệnh của khách B bị coi là "đã xử lý rồi" và <b>lặng lẽ không
 *       được thực thi</b> — thất bại tệ nhất vì không có lỗi nào được báo.</li>
 *   <li><b>Bất biến vàng theo tenant:</b> {@code SUM = 0} phải đúng cho TỪNG tenant, không chỉ
 *       toàn cục. Một bút toán ghi nợ tenant A và ghi có tenant B vẫn giữ tổng toàn cục bằng 0
 *       trong khi sổ của cả hai khách đều đã sai.</li>
 *   <li><b>Đọc chéo:</b> tenant A tra bút toán của B bằng đúng id phải trượt.</li>
 * </ol>
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "management.tracing.enabled=false"
})
@Testcontainers
class TenantIsolationIT {

    private static final UUID TENANT_A = UUID.fromString("0000000a-0000-0000-0000-00000000000a");
    private static final UUID TENANT_B = UUID.fromString("0000000b-0000-0000-0000-00000000000b");

    /** CÙNG tên ví ở cả hai tenant — đây chính là điều phải hoạt động được. */
    private static final String WALLET = "WALLET-001";
    private static final String BANK = "EXTERNAL_BANK";

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("ledgerdb");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    LedgerService ledgerService;
    @Autowired
    AccountRepository accountRepository;
    @Autowired
    JournalEntryRepository journalEntryRepository;
    @Autowired
    PostingRepository postingRepository;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void seedTwoTenants() {
        jdbcTemplate.execute("TRUNCATE postings, journal_entries");
        jdbcTemplate.execute("DELETE FROM outbox_event");
        jdbcTemplate.execute("DELETE FROM processed_event");
        jdbcTemplate.execute("DELETE FROM accounts");

        for (UUID tenant : List.of(TENANT_A, TENANT_B)) {
            accountRepository.save(Account.builder()
                    .tenantId(tenant).externalRef(BANK)
                    .type(ETypeAccount.SYSTEM).currency(ECurrency.VND).build());
            accountRepository.save(Account.builder()
                    .tenantId(tenant).externalRef(WALLET)
                    .type(ETypeAccount.USER).currency(ECurrency.VND).build());
        }
    }

    // ------------------------------------------------------------------
    // 1 + 3 — Số dư và bất biến vàng độc lập theo tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Hai tenant cùng tên ví: số dư độc lập, SUM=0 đúng cho từng tenant")
    void so_du_doc_lap_theo_tenant() {
        post(TENANT_A, topUp(UUID.randomUUID(), 100_000));
        post(TENANT_B, topUp(UUID.randomUUID(), 30_000));

        assertThat(balanceOf(TENANT_A, WALLET))
                .as("ví của A không bị ảnh hưởng bởi giao dịch của B")
                .isEqualTo(100_000);
        assertThat(balanceOf(TENANT_B, WALLET)).isEqualTo(30_000);

        assertThat(postingRepository.totalOfTenant(TENANT_A))
                .as("bất biến vàng phải đúng cho RIÊNG tenant A").isZero();
        assertThat(postingRepository.totalOfTenant(TENANT_B)).isZero();
        assertThat(postingRepository.totalOfSystem())
                .as("và tất nhiên cả toàn cục").isZero();
    }

    // ------------------------------------------------------------------
    // 2 — ⭐ Khoá idempotency không được va chéo tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Cùng transactionId ở hai tenant tạo ra hai bút toán, không bị coi là trùng")
    void khoa_idempotency_khong_va_cheo_tenant() {
        UUID sharedTransactionId = UUID.randomUUID();

        JournalEntry entryA = post(TENANT_A, topUp(sharedTransactionId, 100_000));
        JournalEntry entryB = post(TENANT_B, topUp(sharedTransactionId, 70_000));

        assertThat(entryA.getId())
                .as("nếu quên tenant trong uk_journal_txn_type, lệnh của B bị coi là trùng và "
                        + "LẶNG LẼ không được thực thi — không có lỗi nào được báo")
                .isNotEqualTo(entryB.getId());

        assertThat(balanceOf(TENANT_A, WALLET)).isEqualTo(100_000);
        assertThat(balanceOf(TENANT_B, WALLET)).isEqualTo(70_000);

        // Mà idempotency TRONG một tenant thì vẫn phải chặt: gửi lại trả về đúng bút toán cũ
        JournalEntry replay = post(TENANT_A, topUp(sharedTransactionId, 100_000));
        assertThat(replay.getId()).isEqualTo(entryA.getId());
        assertThat(balanceOf(TENANT_A, WALLET)).isEqualTo(100_000);
    }

    // ------------------------------------------------------------------
    // 4 — Không đọc chéo được, kể cả khi biết chính xác id
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Tenant A không tra được bút toán của B dù cầm đúng id")
    void khong_doc_cheo_duoc_bang_id() {
        JournalEntry entryB = post(TENANT_B, topUp(UUID.randomUUID(), 50_000));

        assertThat(journalEntryRepository.findByTenantIdAndId(TENANT_A, entryB.getId()))
                .as("biết id của người khác không đủ để đọc được dữ liệu của họ")
                .isEmpty();
        assertThat(journalEntryRepository.findByTenantIdAndId(TENANT_B, entryB.getId()))
                .isPresent();

        assertThat(accountRepository
                .findByTenantIdAndExternalRefAndCurrency(TENANT_A, WALLET, ECurrency.VND)
                .orElseThrow().getId())
                .as("cùng tên ví nhưng là hai tài khoản khác nhau")
                .isNotEqualTo(accountRepository
                        .findByTenantIdAndExternalRefAndCurrency(TENANT_B, WALLET, ECurrency.VND)
                        .orElseThrow().getId());
    }

    // ------------------------------------------------------------------
    // 5 — Mặc định là TỪ CHỐI: không có tenant thì không ghi được gì
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Ghi sổ khi chưa có tenant trong ngữ cảnh thì nổ, không im lặng dùng tenant mặc định")
    void thieu_tenant_thi_no_chu_khong_doan() {
        TenantContext.clear();

        assertThatThrownBy(() -> ledgerService.postJournal(topUp(UUID.randomUUID(), 10_000)))
                .as("đoán tenant cho một lệnh tiền là cách ghi sổ của khách này vào sổ khách kia")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Chưa có tenant");

        assertThat(journalEntryRepository.count()).isZero();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private JournalEntry post(UUID tenantId, PostJournalCommand command) {
        return TenantContext.callWith(tenantId, () -> ledgerService.postJournal(command));
    }

    private PostJournalCommand topUp(UUID transactionId, long amountMinor) {
        return new PostJournalCommand(
                transactionId, EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(BANK, -amountMinor), new Leg(WALLET, amountMinor)));
    }

    private long balanceOf(UUID tenantId, String externalRef) {
        UUID accountId = accountRepository
                .findByTenantIdAndExternalRefAndCurrency(tenantId, externalRef, ECurrency.VND)
                .orElseThrow()
                .getId();
        return postingRepository.balanceOf(tenantId, accountId);
    }
}
