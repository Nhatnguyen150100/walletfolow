package com.walletflow.wallet;

import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.Leg;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.entity.Wallet;
import com.walletflow.wallet.enums.EWalletStatus;
import com.walletflow.wallet.repositories.WalletEntryRepository;
import com.walletflow.wallet.repositories.WalletRepository;
import com.walletflow.wallet.services.WalletProjector;
import com.walletflow.wallet.services.WalletService;
import com.walletflow.wallet.support.Events;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Projection số dư từ event của sổ cái (roadmap 1.1).
 *
 * <p>Tính chất trung tâm: <b>số dư cuối KHÔNG phụ thuộc vào việc event đến mấy lần, theo thứ tự nào,
 * hay song song ra sao</b> — nó luôn bằng tổng các bút toán duy nhất. Kafka giao at-least-once và
 * không giữ thứ tự giữa các partition, nên mọi khả năng đó đều sẽ xảy ra ở production.
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
@Testcontainers
class WalletProjectionIT {

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
    WalletRepository walletRepository;
    @Autowired
    WalletEntryRepository entryRepository;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE wallet_entries, wallets, outbox_event");
    }

    // ------------------------------------------------------------------
    // ⭐ 100 event, có lặp, sai thứ tự -> số dư = tổng bút toán duy nhất
    // ------------------------------------------------------------------
    @Test
    @DisplayName("100 event có lặp và đến sai thứ tự: số dư khớp chính xác tổng bút toán duy nhất")
    void event_lap_va_sai_thu_tu_van_ra_dung_so_du() {
        UUID wallet = activeWallet(TENANT, UUID.randomUUID());
        Random random = new Random(42);   // cố định seed: test đỏ thì tái hiện được

        List<LedgerPostedEvent> unique = new ArrayList<>();
        long expected = 0;
        for (int i = 0; i < 100; i++) {
            // Nạp nhiều hơn rút để số dư sổ cái không bao giờ âm — giống thật
            LedgerPostedEvent event = (i % 3 == 2)
                    ? Events.withdraw(wallet, 1 + random.nextInt(1_000))
                    : Events.topUp(wallet, 1 + random.nextInt(5_000));
            unique.add(event);
            expected += event.legs().stream()
                    .filter(leg -> leg.accountRef().equals(wallet.toString()))
                    .mapToLong(Leg::amountMinor).sum();
        }

        List<LedgerPostedEvent> deliveries = new ArrayList<>();
        for (LedgerPostedEvent event : unique) {
            int times = 1 + random.nextInt(3);   // Kafka giao lại 1-3 lần
            for (int t = 0; t < times; t++) {
                deliveries.add(event);
            }
        }
        Collections.shuffle(deliveries, random);
        deliveries.forEach(event -> apply(TENANT, event));

        assertThat(balanceOf(wallet))
                .as("cộng delta + khử trùng theo (ví, bút toán): thứ tự và số lần giao không quan trọng")
                .isEqualTo(expected);
        assertThat(entryRepository.countByWalletId(wallet)).isEqualTo(100);
        assertThat(entryRepository.sumOf(wallet))
                .as("đối soát nội bộ: tổng lịch sử = số dư")
                .isEqualTo(expected);
    }

    // ------------------------------------------------------------------
    // Một event chạm hai ví
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Chuyển A→B là MỘT event chạm hai ví: cả hai được cập nhật, giao lại không đổi gì")
    void chuyen_giua_hai_vi_cap_nhat_ca_hai() {
        UUID a = activeWallet(TENANT, UUID.randomUUID());
        UUID b = activeWallet(TENANT, UUID.randomUUID());
        apply(TENANT, Events.topUp(a, 100_000));

        LedgerPostedEvent transfer = Events.transfer(a, b, 30_000);
        apply(TENANT, transfer);
        apply(TENANT, transfer);

        assertThat(balanceOf(a)).isEqualTo(70_000);
        assertThat(balanceOf(b)).isEqualTo(30_000);
    }

    @Test
    @DisplayName("Vế tài khoản hệ thống, ref lạ và sai loại tiền đều bị bỏ qua")
    void bo_qua_ve_khong_phai_vi_cua_minh() {
        UUID wallet = activeWallet(TENANT, UUID.randomUUID());
        UUID stranger = UUID.randomUUID();

        apply(TENANT, Events.posted(EJournalType.TOPUP,
                new Leg(Events.BANK, -50_000), new Leg(stranger.toString(), 50_000)));
        apply(TENANT, new LedgerPostedEvent(UUID.randomUUID(), UUID.randomUUID(), EJournalType.TOPUP,
                ECurrency.USD, List.of(new Leg(Events.BANK, -10), new Leg(wallet.toString(), 10))));

        assertThat(balanceOf(wallet))
                .as("ví VND không được cộng tiền USD")
                .isZero();
        assertThat(entryRepository.countByWalletId(wallet)).isZero();
    }

    @Test
    @DisplayName("Event của tenant B nhắm vào id ví của tenant A: không áp")
    void khong_ap_cheo_tenant() {
        UUID tenantB = UUID.randomUUID();
        UUID walletOfA = activeWallet(TENANT, UUID.randomUUID());

        apply(tenantB, Events.topUp(walletOfA, 99_000));

        assertThat(balanceOf(walletOfA))
                .as("ref trùng id nhưng khác tenant là tài khoản của khách khác")
                .isZero();
    }

    // ------------------------------------------------------------------
    // Trạng thái theo statusVersion
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Trạng thái chỉ đi tới: bản cũ đến muộn bị bỏ qua, ACTIVE→FROZEN→ACTIVE vẫn đúng")
    void trang_thai_theo_status_version() {
        UUID wallet = openWallet(TENANT, UUID.randomUUID());
        assertThat(statusOf(wallet)).isEqualTo(EWalletStatus.PENDING);

        TenantContext.runWith(TENANT, () -> {
            projector.applyAccountOpened(Events.opened(wallet, EAccountStatus.ACTIVE, 0));
            projector.applyAccountOpened(Events.opened(wallet, EAccountStatus.FROZEN, 1));
            // Bản phát lại của v0 đến muộn sau v1: không được kéo ví về ACTIVE
            projector.applyAccountOpened(Events.opened(wallet, EAccountStatus.ACTIVE, 0));
        });
        assertThat(statusOf(wallet)).isEqualTo(EWalletStatus.FROZEN);

        TenantContext.runWith(TENANT,
                () -> projector.applyAccountOpened(Events.opened(wallet, EAccountStatus.ACTIVE, 2)));
        assertThat(statusOf(wallet))
                .as("cùng trạng thái ACTIVE như v0 nhưng version mới hơn -> phải được áp")
                .isEqualTo(EWalletStatus.ACTIVE);
    }

    // ------------------------------------------------------------------
    // ⭐ Đồng thời
    // ------------------------------------------------------------------
    @Test
    @DisplayName("100 event khác nhau áp song song từ 16 luồng: không mất update nào")
    void ap_song_song_khong_mat_update() throws Exception {
        UUID wallet = activeWallet(TENANT, UUID.randomUUID());
        List<LedgerPostedEvent> events = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            events.add(Events.topUp(wallet, 1_000));
        }

        runConcurrently(16, events.stream().<Callable<Void>>map(event -> () -> {
            apply(TENANT, event);
            return null;
        }).toList());

        assertThat(balanceOf(wallet)).isEqualTo(100_000);
        assertThat(entryRepository.countByWalletId(wallet)).isEqualTo(100);
    }

    @Test
    @DisplayName("Chuyển A→B và B→A xen kẽ song song: không deadlock, số dư cuối đúng")
    void chuyen_nguoc_chieu_song_song_khong_deadlock() throws Exception {
        UUID a = activeWallet(TENANT, UUID.randomUUID());
        UUID b = activeWallet(TENANT, UUID.randomUUID());
        apply(TENANT, Events.topUp(a, 1_000_000));
        apply(TENANT, Events.topUp(b, 1_000_000));

        // Mỗi event chạm CẢ HAI ví. Khoá không theo thứ tự cố định thì luồng A→B giữ A chờ B, luồng
        // B→A giữ B chờ A — Postgres huỷ một bên với "deadlock detected", và applyPosted ném lỗi.
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            LedgerPostedEvent forward = Events.transfer(a, b, 100);
            LedgerPostedEvent backward = Events.transfer(b, a, 300);
            tasks.add(() -> {
                apply(TENANT, forward);
                return null;
            });
            tasks.add(() -> {
                apply(TENANT, backward);
                return null;
            });
        }
        runConcurrently(16, tasks);

        assertThat(balanceOf(a)).isEqualTo(1_000_000 - 50 * 100 + 50 * 300);
        assertThat(balanceOf(b)).isEqualTo(1_000_000 + 50 * 100 - 50 * 300);
    }

    @Test
    @DisplayName("CÙNG một event giao 10 lần song song: chỉ cộng một lần, không luồng nào lỗi")
    void cung_event_song_song_chi_ap_mot_lan() throws Exception {
        UUID wallet = activeWallet(TENANT, UUID.randomUUID());
        LedgerPostedEvent event = Events.topUp(wallet, 5_000);

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.add(() -> {
                apply(TENANT, event);
                return null;
            });
        }
        runConcurrently(10, tasks);

        assertThat(balanceOf(wallet))
                .as("'tìm rồi mới chèn' sẽ để hai luồng cùng thấy 'chưa có' và cộng hai lần")
                .isEqualTo(5_000);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private UUID openWallet(UUID tenant, UUID user) {
        return TenantContext.callWith(tenant,
                () -> walletService.open(user, ECurrency.VND).wallet().id());
    }

    private UUID activeWallet(UUID tenant, UUID user) {
        UUID wallet = openWallet(tenant, user);
        TenantContext.runWith(tenant,
                () -> projector.applyAccountOpened(Events.opened(wallet, EAccountStatus.ACTIVE, 0)));
        return wallet;
    }

    private void apply(UUID tenant, LedgerPostedEvent event) {
        TenantContext.runWith(tenant, () -> projector.applyPosted(event, Instant.now()));
    }

    private long balanceOf(UUID wallet) {
        return walletRepository.findByTenantIdAndId(TENANT, wallet).map(Wallet::getBalanceMinor).orElseThrow();
    }

    private EWalletStatus statusOf(UUID wallet) {
        return walletRepository.findByTenantIdAndId(TENANT, wallet).map(Wallet::getStatus).orElseThrow();
    }

    private static void runConcurrently(int threads, List<Callable<Void>> tasks) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (Future<Void> result : pool.invokeAll(tasks)) {
                result.get();   // ném lại lỗi của bất kỳ luồng nào
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
