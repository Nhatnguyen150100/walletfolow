package com.walletflow.ledger;

import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.exception.AccountNotActiveException;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.entity.Account;
import com.walletflow.ledger.entity.JournalEntry;
import com.walletflow.ledger.enums.ETypeAccount;
import com.walletflow.ledger.repositories.AccountRepository;
import com.walletflow.ledger.repositories.PostingRepository;
import com.walletflow.ledger.services.AccountService;
import com.walletflow.ledger.services.LedgerService;
import com.walletflow.ledger.services.SystemAccountProvisioner;
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Vòng đời tài khoản sổ cái (roadmap 1.0b, ADR-011).
 *
 * <p>Ba nhóm tính chất:
 * <ol>
 *   <li><b>Tài khoản chỉ ra đời qua lệnh mở</b> — chuyển tới một ref lạ bị từ chối, không sinh tài
 *       khoản ma. Mở lại là idempotent, kể cả khi nhiều lệnh mở chạy song song.</li>
 *   <li><b>Trạng thái được cưỡng chế ở sổ cái</b> — {@code FROZEN} chặn ghi nợ nhưng vẫn nhận tiền,
 *       {@code CLOSED} chặn cả hai, và mở lại không làm ví bị đóng băng sống lại.</li>
 *   <li><b>Tài khoản hệ thống được cấp theo tenant</b>, idempotent.</li>
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
class AccountLifecycleIT {

    private static final UUID TENANT = TenantContext.DEFAULT_TENANT;
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
    AccountService accountService;
    @Autowired
    LedgerService ledgerService;
    @Autowired
    SystemAccountProvisioner provisioner;
    @Autowired
    AccountRepository accountRepository;
    @Autowired
    PostingRepository postingRepository;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE postings, journal_entries");
        jdbcTemplate.execute("DELETE FROM outbox_event");
        jdbcTemplate.execute("DELETE FROM processed_event");
        jdbcTemplate.execute("DELETE FROM accounts WHERE type = 'USER'");
    }

    // ------------------------------------------------------------------
    // 1 — Tài khoản chỉ ra đời qua lệnh mở
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Mở tài khoản idempotent: mở lại trả về đúng tài khoản cũ")
    void mo_tai_khoan_idempotent() {
        String wallet = newWalletRef();

        Account first = open(wallet);
        Account again = open(wallet);

        assertThat(again.getId()).isEqualTo(first.getId());
        assertThat(first.getType()).isEqualTo(ETypeAccount.USER);
        assertThat(first.getStatus()).isEqualTo(EAccountStatus.ACTIVE);
        assertThat(countAccounts(wallet)).isEqualTo(1);
    }

    @Test
    @DisplayName("20 lệnh mở cùng một ví chạy song song: đúng MỘT tài khoản, không lệnh nào lỗi")
    void mo_song_song_chi_ra_mot_tai_khoan() throws Exception {
        String wallet = newWalletRef();
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Callable<UUID>> tasks = IntStream.range(0, threads)
                    .<Callable<UUID>>mapToObj(i -> () -> open(wallet).getId())
                    .toList();

            Set<UUID> ids = new HashSet<>();
            for (Future<UUID> result : pool.invokeAll(tasks)) {
                // get() ném lỗi nếu một lệnh nào đó vỡ ở unique key — ON CONFLICT phải chặn được.
                ids.add(result.get());
            }

            assertThat(ids).as("mọi lệnh đều nhận về cùng một tài khoản").hasSize(1);
            assertThat(countAccounts(wallet)).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("Chuyển tới ref chưa mở: bị từ chối, KHÔNG sinh tài khoản ma")
    void chuyen_toi_ref_chua_mo_bi_tu_choi() {
        String source = newWalletRef();
        open(source);
        topUp(source, 100_000);

        String typo = newWalletRef();   // ví không tồn tại — giống một lần gõ sai mã

        assertThatThrownBy(() -> post(transfer(source, typo, 50_000)))
                .isInstanceOf(ResourceNotFoundException.class);

        assertThat(countAccounts(typo))
                .as("tự tạo tài khoản ở đây nghĩa là tiền 'chuyển thành công' vào hư vô")
                .isZero();
        assertThat(balanceOf(source)).as("tiền không rời ví nguồn").isEqualTo(100_000);
    }

    @Test
    @DisplayName("Ref phải là UUID chuẩn chữ thường — chữ hoa sẽ chẻ đôi một ví thành hai tài khoản")
    void ref_khong_phai_uuid_chuan_bi_tu_choi() {
        String wallet = newWalletRef();

        for (String bad : List.of("WALLET-001", wallet.toUpperCase(), BANK, "")) {
            assertThatThrownBy(() -> open(bad))
                    .as("ref '%s'", bad)
                    .isInstanceOf(AppException.class)
                    .extracting(e -> ((AppException) e).getErrorCode())
                    .isEqualTo("INVALID_ACCOUNT_REF");
        }
        assertThat(accountRepository.findByTenantId(TENANT))
                .noneMatch(account -> account.getType() == ETypeAccount.USER);
    }

    @Test
    @DisplayName("Ref trùng tài khoản hệ thống: từ chối, không trả tài khoản hệ thống về như ví")
    void ref_trung_tai_khoan_he_thong_bi_tu_choi() {
        String reserved = newWalletRef();
        jdbcTemplate.update("""
                INSERT INTO accounts (id, tenant_id, external_ref, type, currency, created_at, updated_at)
                VALUES (gen_random_uuid(), ?, ?, 'SYSTEM', 'VND', now(), now())
                """, TENANT, reserved);

        assertThatThrownBy(() -> open(reserved))
                .as("trả nó về như 'mở thành công' là trỏ một ví vào tài khoản được phép âm")
                .isInstanceOf(AppException.class)
                .extracting(e -> ((AppException) e).getErrorCode())
                .isEqualTo("ACCOUNT_REF_RESERVED");

        jdbcTemplate.update("DELETE FROM accounts WHERE external_ref = ?", reserved);
    }

    // ------------------------------------------------------------------
    // 2 — Trạng thái được cưỡng chế ở sổ cái
    // ------------------------------------------------------------------
    @Test
    @DisplayName("FROZEN: chặn ghi nợ, nhưng vẫn nhận được tiền (để hoàn tiền về ví vẫn chạy)")
    void dong_bang_chan_ghi_no_nhung_van_nhan_tien() {
        String frozen = newWalletRef();
        String other = newWalletRef();
        open(frozen);
        open(other);
        topUp(frozen, 100_000);
        topUp(other, 100_000);
        setStatus(frozen, EAccountStatus.FROZEN);

        assertThatThrownBy(() -> post(transfer(frozen, other, 10_000)))
                .isInstanceOf(AccountNotActiveException.class)
                .extracting(e -> ((AppException) e).getErrorCode())
                .isEqualTo("ACCOUNT_FROZEN");

        post(transfer(other, frozen, 30_000));

        assertThat(balanceOf(frozen))
                .as("ví đóng băng vẫn nhận tiền; đóng băng chỉ chặn tiền RỜI ví")
                .isEqualTo(130_000);
        assertThat(postingRepository.totalOfTenant(TENANT)).isZero();
    }

    @Test
    @DisplayName("CLOSED: chặn cả ghi nợ lẫn ghi có")
    void dong_vinh_vien_chan_ca_hai_chieu() {
        String closed = newWalletRef();
        String other = newWalletRef();
        open(closed);
        open(other);
        topUp(other, 100_000);
        setStatus(closed, EAccountStatus.CLOSED);

        assertThatThrownBy(() -> post(transfer(other, closed, 10_000)))
                .isInstanceOf(AccountNotActiveException.class)
                .extracting(e -> ((AppException) e).getErrorCode())
                .isEqualTo("ACCOUNT_CLOSED");

        assertThat(balanceOf(other)).isEqualTo(100_000);
    }

    @Test
    @DisplayName("Mở lại một ví đang bị đóng băng KHÔNG làm nó sống lại")
    void mo_lai_khong_lam_vi_dong_bang_song_lai() {
        String wallet = newWalletRef();
        open(wallet);
        setStatus(wallet, EAccountStatus.FROZEN);

        Account reopened = open(wallet);

        assertThat(reopened.getStatus())
                .as("lệnh mở bị gửi lại không được là một cách gỡ đóng băng")
                .isEqualTo(EAccountStatus.FROZEN);
    }

    @Test
    @DisplayName("REVERSAL vẫn chạy khi ví đã CLOSED — bù trừ phải luôn thành công")
    void reversal_van_chay_khi_vi_da_dong() {
        String wallet = newWalletRef();
        open(wallet);
        topUp(wallet, 100_000);
        JournalEntry withdraw = postEntry(new PostJournalCommand(
                UUID.randomUUID(), EJournalType.WITHDRAW, null, ECurrency.VND,
                List.of(new Leg(wallet, -40_000), new Leg(BANK, 40_000))));

        // Ví bị đóng trong lúc chờ ngân hàng xác nhận; rồi payout thất bại -> phải hoàn tiền
        setStatus(wallet, EAccountStatus.CLOSED);
        post(new PostJournalCommand(
                withdraw.getTransactionId(), EJournalType.REVERSAL, withdraw.getId(), ECurrency.VND,
                List.of(new Leg(wallet, 40_000), new Leg(BANK, -40_000))));

        assertThat(balanceOf(wallet))
                .as("chặn bù trừ ở đây là để tiền kẹt vĩnh viễn ở tài khoản phía bên kia")
                .isEqualTo(100_000);
        assertThat(postingRepository.totalOfTenant(TENANT)).isZero();
    }

    @Test
    @DisplayName("REVERSAL không đối xứng với bút toán gốc bị từ chối — không phải cửa hậu rút tiền ví đóng băng")
    void reversal_khong_doi_xung_bi_tu_choi() {
        String frozen = newWalletRef();
        String accomplice = newWalletRef();
        open(frozen);
        open(accomplice);
        JournalEntry topUp = postEntry(topUpCommand(frozen, 100_000));
        setStatus(frozen, EAccountStatus.FROZEN);

        // Trỏ về một bút toán có thật, nhưng chuyển tiền sang ví khác thay vì hoàn về nguồn gốc
        assertThatThrownBy(() -> post(new PostJournalCommand(
                UUID.randomUUID(), EJournalType.REVERSAL, topUp.getId(), ECurrency.VND,
                List.of(new Leg(frozen, -100_000), new Leg(accomplice, 100_000)))))
                .isInstanceOf(AppException.class)
                .extracting(e -> ((AppException) e).getErrorCode())
                .isEqualTo("REVERSAL_MISMATCH");

        // Đúng tài khoản nhưng sai số tiền cũng không được
        assertThatThrownBy(() -> post(new PostJournalCommand(
                UUID.randomUUID(), EJournalType.REVERSAL, topUp.getId(), ECurrency.VND,
                List.of(new Leg(frozen, -60_000), new Leg(BANK, 60_000)))))
                .isInstanceOf(AppException.class)
                .extracting(e -> ((AppException) e).getErrorCode())
                .isEqualTo("REVERSAL_MISMATCH");

        assertThat(balanceOf(frozen)).isEqualTo(100_000);
        assertThat(balanceOf(accomplice)).isZero();
    }

    @Test
    @DisplayName("status_version do DB tự tăng mỗi lần trạng thái đổi, không lặp lại, không sửa tay được")
    void status_version_tu_tang_khong_lap_lai() {
        String wallet = newWalletRef();
        open(wallet);

        setStatus(wallet, EAccountStatus.FROZEN);
        setStatus(wallet, EAccountStatus.ACTIVE);
        assertThat(open(wallet).getStatusVersion())
                .as("ACTIVE -> FROZEN -> ACTIVE: trạng thái quay lại nhưng version thì không")
                .isEqualTo(2);

        jdbcTemplate.update("UPDATE accounts SET status_version = 0 WHERE tenant_id = ? AND external_ref = ?",
                TENANT, wallet);
        assertThat(open(wallet).getStatusVersion())
                .as("version chỉ đi lên; kéo lùi nó là làm consumer áp lại trạng thái cũ")
                .isEqualTo(2);
    }

    // ------------------------------------------------------------------
    // 3 — Tài khoản hệ thống theo tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Cấp tài khoản hệ thống cho tenant mới: đủ bộ cho mọi currency, chạy lại không tạo thêm")
    void cap_tai_khoan_he_thong_idempotent_theo_tenant() {
        UUID newTenant = UUID.randomUUID();
        int expected = 3 * ECurrency.values().length;

        assertThat(provisioner.provision(newTenant)).isEqualTo(expected);
        assertThat(provisioner.provision(newTenant)).as("chạy lại không tạo thêm").isZero();

        List<Account> accounts = accountRepository.findByTenantId(newTenant);
        assertThat(accounts).hasSize(expected)
                .allMatch(account -> account.getType() == ETypeAccount.SYSTEM);
        assertThat(accountRepository
                .findByTenantIdAndExternalRefAndCurrency(newTenant, BANK, ECurrency.VND))
                .as("EXTERNAL_BANK của tenant mới là tài khoản RIÊNG, không dùng chung")
                .isPresent()
                .get()
                .extracting(Account::getId)
                .isNotEqualTo(accountRepository
                        .findByTenantIdAndExternalRefAndCurrency(TENANT, BANK, ECurrency.VND)
                        .orElseThrow().getId());

        // Tenant mới dùng được ngay: mở ví + nạp tiền chạy trên tài khoản hệ thống của chính nó
        String wallet = newWalletRef();
        TenantContext.runWith(newTenant, () -> {
            accountService.openUserAccount(new OpenAccountCommand(wallet, ECurrency.VND));
            ledgerService.postJournal(topUpCommand(wallet, 70_000));
        });
        assertThat(postingRepository.totalOfTenant(newTenant)).isZero();
        assertThat(postingRepository.totalOfTenant(TENANT)).as("không đụng sổ tenant khác").isZero();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private static String newWalletRef() {
        return UUID.randomUUID().toString();
    }

    private Account open(String ref) {
        return TenantContext.callWith(TENANT,
                () -> accountService.openUserAccount(new OpenAccountCommand(ref, ECurrency.VND)));
    }

    private void post(PostJournalCommand command) {
        postEntry(command);
    }

    private JournalEntry postEntry(PostJournalCommand command) {
        return TenantContext.callWith(TENANT, () -> ledgerService.postJournal(command));
    }

    private void topUp(String wallet, long amountMinor) {
        post(topUpCommand(wallet, amountMinor));
    }

    private static PostJournalCommand topUpCommand(String wallet, long amountMinor) {
        return new PostJournalCommand(UUID.randomUUID(), EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(BANK, -amountMinor), new Leg(wallet, amountMinor)));
    }

    private static PostJournalCommand transfer(String from, String to, long amountMinor) {
        return new PostJournalCommand(UUID.randomUUID(), EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(from, -amountMinor), new Leg(to, amountMinor)));
    }

    private void setStatus(String ref, EAccountStatus status) {
        jdbcTemplate.update("UPDATE accounts SET status = ? WHERE tenant_id = ? AND external_ref = ?",
                status.name(), TENANT, ref);
    }

    private long balanceOf(String ref) {
        UUID accountId = accountRepository
                .findByTenantIdAndExternalRefAndCurrency(TENANT, ref, ECurrency.VND)
                .orElseThrow().getId();
        return postingRepository.balanceOf(TENANT, accountId);
    }

    private int countAccounts(String ref) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM accounts WHERE tenant_id = ? AND external_ref = ?",
                Integer.class, TENANT, ref);
        return count == null ? 0 : count;
    }
}
