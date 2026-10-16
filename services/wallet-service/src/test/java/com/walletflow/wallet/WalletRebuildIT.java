package com.walletflow.wallet;

import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.ledger.StatementLine;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.entity.Wallet;
import com.walletflow.wallet.entity.WalletEntry;
import com.walletflow.wallet.enums.EWalletStatus;
import com.walletflow.wallet.repositories.WalletEntryRepository;
import com.walletflow.wallet.repositories.WalletRepository;
import com.walletflow.wallet.services.WalletProjector;
import com.walletflow.wallet.services.WalletRebuildService;
import com.walletflow.wallet.services.WalletService;
import com.walletflow.wallet.support.Events;
import com.walletflow.wallet.support.FakeLedger;
import com.walletflow.wallet.support.FakeLedgerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Dựng lại read model từ sổ cái (roadmap 1.1). Ledger luôn thắng.
 *
 * <p>Hai tính chất khó, cả hai đều nói về event ĐANG BAY lúc rebuild — thứ một bài test "làm sai
 * số dư rồi rebuild" đơn thuần không bao giờ chạm tới:
 * <ol>
 *   <li>Bút toán đã nằm trong sao kê mà projector chưa kịp áp: sau rebuild, projector áp nó lần nữa
 *       thì KHÔNG được cộng hai lần.</li>
 *   <li>Rebuild ví A không được làm ví B mất tiền, dù A và B chung một event. Khử trùng theo event
 *       sẽ vỡ đúng ở đây.</li>
 * </ol>
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=3600000",
        "spring.kafka.listener.auto-startup=false",
        "walletflow.kafka.declare-topics=false",
        "walletflow.wallet.pending-resend.enabled=false",
        "management.tracing.enabled=false"
})
@Import(FakeLedgerConfig.class)
@Testcontainers
class WalletRebuildIT {

    private static final UUID TENANT = TenantContext.DEFAULT_TENANT;

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("walletdb");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    WalletService walletService;
    @Autowired
    WalletProjector projector;
    @Autowired
    WalletRebuildService rebuildService;
    @Autowired
    WalletRepository walletRepository;
    @Autowired
    WalletEntryRepository entryRepository;
    @Autowired
    FakeLedger ledger;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE wallet_entries, wallets, outbox_event");
        ledger.reset();
    }

    @Test
    @DisplayName("Read model hỏng đủ kiểu (số dư sai, thiếu dòng, thừa dòng, dòng sai số tiền): rebuild khớp ledger")
    void rebuild_sua_moi_kieu_hong() {
        UUID wallet = activeWallet();
        LedgerPostedEvent first = Events.topUp(wallet, 100_000);
        LedgerPostedEvent second = Events.withdraw(wallet, 30_000);
        LedgerPostedEvent missing = Events.topUp(wallet, 5_000);
        apply(first);
        apply(second);

        // Phá hỏng read model theo cả bốn cách
        jdbcTemplate.update("UPDATE wallets SET balance_minor = 999999 WHERE id = ?", wallet);
        jdbcTemplate.update("UPDATE wallet_entries SET amount_minor = 1 WHERE entry_id = ?", second.entryId());
        jdbcTemplate.update("""
                INSERT INTO wallet_entries (tenant_id, wallet_id, entry_id, transaction_id, type, amount_minor)
                VALUES (?, ?, ?, ?, 'TOPUP', 777)
                """, TENANT, wallet, UUID.randomUUID(), UUID.randomUUID());

        ledger.declare(TENANT, wallet, EAccountStatus.ACTIVE, 0, List.of(
                Events.lineFor(wallet, first), Events.lineFor(wallet, second), Events.lineFor(wallet, missing)));

        WalletRebuildService.RebuildResult result = rebuild(wallet);

        assertThat(result.balanceBefore()).isEqualTo(999_999);
        assertThat(result.balanceAfter()).isEqualTo(75_000);
        assertThat(balanceOf(wallet)).isEqualTo(75_000);
        assertThat(entryRepository.findByTenantIdAndWalletId(TENANT, wallet))
                .extracting(WalletEntry::getEntryId)
                .as("dòng thừa bị gỡ, dòng thiếu được thêm")
                .containsExactlyInAnyOrder(first.entryId(), second.entryId(), missing.entryId());
        assertThat(entryRepository.sumOf(wallet)).as("tổng lịch sử = số dư").isEqualTo(75_000);
    }

    @Test
    @DisplayName("⭐ Bút toán đang bay: có trong sao kê nhưng chưa áp -> rebuild rồi áp lại KHÔNG cộng hai lần")
    void event_dang_bay_khong_bi_cong_hai_lan() {
        UUID wallet = activeWallet();
        LedgerPostedEvent applied = Events.topUp(wallet, 100_000);
        LedgerPostedEvent inFlight = Events.topUp(wallet, 20_000);   // ledger đã ghi, Kafka chưa giao
        apply(applied);

        ledger.declare(TENANT, wallet, EAccountStatus.ACTIVE, 0,
                List.of(Events.lineFor(wallet, applied), Events.lineFor(wallet, inFlight)));
        rebuild(wallet);
        assertThat(balanceOf(wallet)).isEqualTo(120_000);

        apply(inFlight);   // Kafka giao muộn
        assertThat(balanceOf(wallet))
                .as("rebuild chỉ đặt balance = SUM mà không ghi dòng lịch sử thì ở đây ra 140 000")
                .isEqualTo(120_000);

        LedgerPostedEvent after = Events.withdraw(wallet, 7_000);   // ghi sổ SAU lần đọc sao kê
        apply(after);
        assertThat(balanceOf(wallet)).as("bút toán mới hơn sao kê vẫn được áp").isEqualTo(113_000);
    }

    @Test
    @DisplayName("⭐ KHOÁ rồi mới ĐỌC: bút toán ghi sổ sau lần đọc, áp trong lúc rebuild, không bị ghi đè mất")
    void khoa_truoc_doc_sau_khong_mat_but_toan() throws Exception {
        UUID wallet = activeWallet();
        LedgerPostedEvent applied = Events.topUp(wallet, 100_000);
        apply(applied);
        LedgerPostedEvent newer = Events.topUp(wallet, 9_000);   // ghi sổ SAU khi rebuild đọc sao kê

        ledger.declare(TENANT, wallet, EAccountStatus.ACTIVE, 0, List.of(Events.lineFor(wallet, applied)));

        // Đúng lúc rebuild đang đọc sổ cái, projector nhận bút toán mới hơn sao kê
        Thread projectorThread = new Thread(() -> apply(newer));
        ledger.duringRead(() -> {
            projectorThread.start();
            try {
                projectorThread.join(1_500);   // cho nó MỌI cơ hội chạy xong nếu không bị chặn
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            assertThat(projectorThread.isAlive())
                    .as("ví đã bị khoá TRƯỚC khi đọc, nên projector phải đang chờ khoá")
                    .isTrue();
        });

        rebuild(wallet);
        projectorThread.join(10_000);

        assertThat(balanceOf(wallet))
                .as("đọc rồi mới khoá: projector cộng 9 000 trước, rebuild ghi đè về 100 000, và vì "
                        + "dòng lịch sử đã có nên bút toán đó không bao giờ được áp lại")
                .isEqualTo(109_000);
        assertThat(entryRepository.sumOf(wallet)).isEqualTo(109_000);
    }

    @Test
    @DisplayName("⭐ Rebuild ví A không làm ví B mất tiền, dù A và B chung một event")
    void rebuild_mot_vi_khong_lam_vi_kia_mat_tien() {
        UUID a = activeWallet();
        UUID b = activeWallet();
        LedgerPostedEvent topUp = Events.topUp(a, 100_000);
        apply(topUp);
        LedgerPostedEvent transfer = Events.transfer(a, b, 40_000);   // chưa giao tới wallet-service

        ledger.declare(TENANT, a, EAccountStatus.ACTIVE, 0,
                List.of(Events.lineFor(a, topUp), Events.lineFor(a, transfer)));
        rebuild(a);

        apply(transfer);

        assertThat(balanceOf(a)).as("A đã gồm transfer từ rebuild, không trừ lần hai").isEqualTo(60_000);
        assertThat(balanceOf(b))
                .as("khử trùng THEO EVENT thì rebuild A đánh dấu event đã xử lý, và B không bao giờ nhận được 40 000")
                .isEqualTo(40_000);
    }

    @Test
    @DisplayName("Ví PENDING: ledger chưa có tài khoản thì giữ nguyên; đã có thì nhận trạng thái từ sao kê")
    void rebuild_vi_pending() {
        UUID wallet = TenantContext.callWith(TENANT,
                () -> walletService.open(UUID.randomUUID(), ECurrency.VND).wallet().id());

        WalletRebuildService.RebuildResult nothing = rebuild(wallet);
        assertThat(nothing.status()).isEqualTo(EWalletStatus.PENDING);
        assertThat(nothing.entriesAdded()).isZero();

        // AccountOpened bị mất, nhưng ledger đã mở tài khoản: rebuild tự lấp lỗ đó
        ledger.declare(TENANT, wallet, EAccountStatus.ACTIVE, 0, List.of(
                new StatementLine(UUID.randomUUID(), UUID.randomUUID(), EJournalType.TOPUP, 1_000, Instant.now())));
        WalletRebuildService.RebuildResult result = rebuild(wallet);

        assertThat(result.status()).isEqualTo(EWalletStatus.ACTIVE);
        assertThat(balanceOf(wallet)).isEqualTo(1_000);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private UUID activeWallet() {
        return TenantContext.callWith(TENANT, () -> {
            UUID wallet = walletService.open(UUID.randomUUID(), ECurrency.VND).wallet().id();
            projector.applyAccountOpened(Events.opened(wallet, EAccountStatus.ACTIVE, 0));
            return wallet;
        });
    }

    private void apply(LedgerPostedEvent event) {
        TenantContext.runWith(TENANT, () -> projector.applyPosted(event, Instant.now()));
    }

    private WalletRebuildService.RebuildResult rebuild(UUID wallet) {
        return TenantContext.callWith(TENANT, () -> rebuildService.rebuild(wallet));
    }

    private long balanceOf(UUID wallet) {
        return walletRepository.findByTenantIdAndId(TENANT, wallet).map(Wallet::getBalanceMinor).orElseThrow();
    }
}
