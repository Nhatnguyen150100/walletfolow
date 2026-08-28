package com.walletflow.ledger;

import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.InsufficientFundsException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.ledger.dto.JournalView;
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
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test tích hợp cho sổ cái — chứng minh các tính chất sống còn của tiền:
 *
 * <ol>
 *   <li>Bảo toàn tiền: tổng mọi posting toàn hệ thống luôn = 0 (P7)</li>
 *   <li>Không ghi bút toán lệch: SUM mỗi journal = 0, sai thì từ chối trọn vẹn</li>
 *   <li>Idempotent theo {@code (transactionId, type)}: gửi lại lệnh TRẢ VỀ bút toán cũ (P2)</li>
 *   <li>Không bao giờ âm dưới tải song song: 100 luồng rút cùng một ví (P3 + P6)</li>
 *   <li>Append-only: DB từ chối UPDATE/DELETE trên bút toán</li>
 * </ol>
 *
 * <p>Chạy trên Postgres thật (Testcontainers) chứ không phải H2, vì thứ đang được kiểm
 * chứng — {@code SELECT ... FOR UPDATE}, ràng buộc UNIQUE, trigger, hành vi isolation — là hành vi
 * riêng của Postgres. Việc context khởi động được cũng đồng thời chứng minh migration
 * Flyway khớp mapping entity (do {@code ddl-auto=validate}).
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",              // test không cần service discovery
        "spring.cloud.config.enabled=false",        // test không cần config-server
        "outbox.relay.delay-ms=3600000",            // chặn OutboxRelay đi tìm Kafka
        "spring.kafka.listener.auto-startup=false", // test này không cần Kafka
        "management.tracing.enabled=false"          // không xuất trace khi test
})
@Testcontainers
class LedgerServiceIT {

    private static final String EXTERNAL_BANK = "EXTERNAL_BANK";
    private static final String WALLET_A = "WALLET_A";
    private static final String WALLET_B = "WALLET_B";

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("ledgerdb");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    LedgerService ledgerService;
    @Autowired
    AccountRepository accountRepository;
    @Autowired
    PostingRepository postingRepository;
    @Autowired
    JournalEntryRepository journalEntryRepository;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void resetLedger() {
        // KHÔNG dùng @Transactional trên test: test concurrency cần dữ liệu được COMMIT
        // thật thì các luồng khác mới nhìn thấy nhau.
        //
        // Dùng TRUNCATE chứ không DELETE: trigger append-only (V2) chặn DELETE từng hàng — đó chính
        // là bất biến đang được bảo vệ. TRUNCATE là thao tác quản trị tường minh, không kích hoạt
        // trigger FOR EACH ROW, nên chỉ dọn dẹp trong test mới dùng được.
        jdbcTemplate.execute("TRUNCATE postings, journal_entries");
        accountRepository.findAll().stream()
                .filter(account -> account.getType() == ETypeAccount.USER)
                .forEach(accountRepository::delete);   // giữ lại account SYSTEM do Flyway seed
    }

    // ------------------------------------------------------------------
    // Test 1 — Bất biến vàng: tiền không tự sinh, không bốc hơi
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Nạp rồi chuyển tiền: số dư đúng và tổng toàn hệ thống vẫn bằng 0")
    void tong_moi_posting_toan_he_thong_luon_bang_0() {
        seedWallet(WALLET_A);
        seedWallet(WALLET_B);

        ledgerService.postJournal(topUp(WALLET_A, 100_000));
        ledgerService.postJournal(transfer(WALLET_A, WALLET_B, 30_000));

        assertThat(balanceOf(WALLET_A)).isEqualTo(70_000);
        assertThat(balanceOf(WALLET_B)).isEqualTo(30_000);
        // Tài khoản kỹ thuật giữ phần đối ứng của tiền đi vào hệ thống
        assertThat(balanceOf(EXTERNAL_BANK)).isEqualTo(-100_000);
        assertThat(postingRepository.totalOfSystem()).isZero();
    }

    // ------------------------------------------------------------------
    // Test 2 — Bút toán lệch bị từ chối trọn vẹn (không ghi nửa vời)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Bút toán có SUM != 0 bị từ chối và không để lại posting nào")
    void tu_choi_but_toan_khong_can_bang() {
        seedWallet(WALLET_A);

        PostJournalCommand lechVe = new PostJournalCommand(
                UUID.randomUUID(), EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(WALLET_A, -100_000), new Leg(EXTERNAL_BANK, 50_000)));

        assertThatThrownBy(() -> ledgerService.postJournal(lechVe))
                .isInstanceOf(BadRequestException.class);

        assertThat(postingRepository.count()).isZero();
        assertThat(journalEntryRepository.count()).isZero();
    }

    // ------------------------------------------------------------------
    // Test 2b — Tràn số không được phép "giả vờ cân bằng" (P8, plan §6 quy tắc 4)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Các vế cộng lại tràn số long không được coi là cân bằng")
    void tu_choi_but_toan_can_bang_nho_tran_so() {
        seedWallet(WALLET_A);

        // MAX + MAX + 2 wrap về đúng 0 nếu cộng bằng toán tử '+' thông thường,
        // tức bút toán "tiền tự sinh" sẽ lọt qua kiểm tra SUM = 0.
        PostJournalCommand tranSo = new PostJournalCommand(
                UUID.randomUUID(), EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(WALLET_A, Long.MAX_VALUE),
                        new Leg(WALLET_B, Long.MAX_VALUE),
                        new Leg(EXTERNAL_BANK, 2)));

        assertThatThrownBy(() -> ledgerService.postJournal(tranSo))
                .isInstanceOf(BadRequestException.class);

        assertThat(postingRepository.count()).isZero();
        assertThat(journalEntryRepository.count()).isZero();
    }

    // ------------------------------------------------------------------
    // Test 2c — Bút toán phải có >= 2 vế và không có vế 0 (plan §5.1)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Bút toán rỗng / một vế / có vế bằng 0 đều bị từ chối, không sinh journal entry")
    void tu_choi_but_toan_khong_du_ve() {
        seedWallet(WALLET_A);

        // legs rỗng có SUM = 0 nên "cân bằng" một cách vô nghĩa
        assertThatThrownBy(() -> ledgerService.postJournal(new PostJournalCommand(
                UUID.randomUUID(), EJournalType.TRANSFER, null, ECurrency.VND, List.of())))
                .isInstanceOf(BadRequestException.class);

        assertThatThrownBy(() -> ledgerService.postJournal(new PostJournalCommand(
                UUID.randomUUID(), EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(WALLET_A, 0)))))
                .isInstanceOf(BadRequestException.class);

        // Vế 0 không mang thông tin: phải bị chặn ở tầng app (400) chứ không để CHECK ở DB nổ (500)
        assertThatThrownBy(() -> ledgerService.postJournal(new PostJournalCommand(
                UUID.randomUUID(), EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(WALLET_A, 0), new Leg(EXTERNAL_BANK, 0)))))
                .isInstanceOf(BadRequestException.class);

        assertThat(journalEntryRepository.count())
                .as("không được ghi journal entry nào, kể cả entry không có posting")
                .isZero();
        assertThat(postingRepository.count()).isZero();
    }

    // ------------------------------------------------------------------
    // Test 3 — ⭐ Idempotency: lệnh trùng TRẢ VỀ kết quả cũ, không phải lỗi (P2, plan §7.2)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Gửi lại cùng transactionId: trả về đúng bút toán cũ, số dư không nhân đôi")
    void cung_transaction_id_tra_ve_but_toan_cu() {
        seedWallet(WALLET_A);
        PostJournalCommand command = topUp(WALLET_A, 100_000);

        JournalEntry first = ledgerService.postJournal(command);
        JournalEntry replay = ledgerService.postJournal(command);

        assertThat(replay.getId())
                .as("lệnh trùng KHÔNG được ném lỗi — Saga sẽ hiểu nhầm là ghi sổ thất bại và hoàn tiền")
                .isEqualTo(first.getId());

        assertThat(balanceOf(WALLET_A)).isEqualTo(100_000);   // không thành 200_000
        assertThat(journalEntryRepository.count()).isEqualTo(1);
        assertThat(postingRepository.count()).isEqualTo(2);
    }

    // ------------------------------------------------------------------
    // Test 4 — Không đủ tiền thì từ chối (P6, trường hợp tuần tự)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Rút quá số dư bị từ chối, ví không bị chạm tới")
    void tu_choi_khi_khong_du_so_du() {
        seedWallet(WALLET_A);
        ledgerService.postJournal(topUp(WALLET_A, 50_000));

        assertThatThrownBy(() -> ledgerService.postJournal(withdraw(WALLET_A, 80_000)))
                .isInstanceOf(InsufficientFundsException.class);

        assertThat(balanceOf(WALLET_A)).isEqualTo(50_000);
        assertThat(postingRepository.totalOfSystem()).isZero();
    }

    // ------------------------------------------------------------------
    // Test 5 — ⭐ 100 luồng rút song song cùng một ví (P3 + P6)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("100 luồng cùng rút một ví: đúng 10 lần thành công, số dư về 0 và không bao giờ âm")
    void ban_100_luong_rut_cung_mot_vi_khong_bao_gio_am() throws Exception {
        seedWallet(WALLET_A);
        ledgerService.postJournal(topUp(WALLET_A, 100_000));   // đủ cho ĐÚNG 10 lần rút 10_000

        int soLuong = 100;
        int soLuongRut = 10_000;
        ExecutorService pool = Executors.newFixedThreadPool(8);   // < Hikari max-pool (10)
        CountDownLatch banDongLoat = new CountDownLatch(1);
        CountDownLatch hoanTat = new CountDownLatch(soLuong);
        AtomicInteger thanhCong = new AtomicInteger();
        Queue<Throwable> loiNgoaiDuKien = new ConcurrentLinkedQueue<>();

        try {
            for (int i = 0; i < soLuong; i++) {
                pool.submit(() -> {
                    try {
                        banDongLoat.await();                       // dồn mọi luồng vào cùng một thời điểm
                        ledgerService.postJournal(withdraw(WALLET_A, soLuongRut));
                        thanhCong.incrementAndGet();
                    } catch (InsufficientFundsException expected) {
                        // Luồng "thua cuộc" — đây chính là hành vi ĐÚNG
                    } catch (Throwable unexpected) {
                        loiNgoaiDuKien.add(unexpected);
                    } finally {
                        hoanTat.countDown();
                    }
                });
            }
            banDongLoat.countDown();
            assertThat(hoanTat.await(120, TimeUnit.SECONDS))
                    .as("100 luồng phải kết thúc trong 120s (nếu treo: nghi deadlock do khoá sai thứ tự)")
                    .isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(loiNgoaiDuKien)
                .as("chỉ được phép thất bại vì InsufficientFunds, không phải lỗi kỹ thuật")
                .isEmpty();
        assertThat(thanhCong.get())
                .as("100_000 / 10_000 = đúng 10 lần rút được phép")
                .isEqualTo(10);
        assertThat(balanceOf(WALLET_A))
                .as("số dư cuối phải bằng 0 và TUYỆT ĐỐI không âm")
                .isZero();
        assertThat(postingRepository.totalOfSystem())
                .as("bất biến bảo toàn tiền vẫn giữ sau khi chạy đua")
                .isZero();
    }

    // ------------------------------------------------------------------
    // Test 6 — ⭐ REVERSAL phải truy được về bút toán gốc (plan §5.2, §7.4)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("REVERSAL bù trừ bút toán gốc, trỏ về nó bằng referenceId và đưa số dư về 0")
    void reversal_bu_tru_va_tro_ve_but_toan_goc() {
        seedWallet(WALLET_A);
        UUID transactionId = UUID.randomUUID();

        JournalEntry original = ledgerService.postJournal(new PostJournalCommand(
                transactionId, EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(EXTERNAL_BANK, -100_000), new Leg(WALLET_A, 100_000))));

        // Bù trừ dùng CÙNG transactionId, khác type -> uk_journal_txn_type cho phép, và lịch sử
        // của giao dịch gồm cả hai bút toán.
        JournalEntry reversal = ledgerService.postJournal(new PostJournalCommand(
                transactionId, EJournalType.REVERSAL, original.getId(), ECurrency.VND,
                List.of(new Leg(WALLET_A, -100_000), new Leg(EXTERNAL_BANK, 100_000))));

        assertThat(reversal.getReferenceId()).isEqualTo(original.getId());
        assertThat(balanceOf(WALLET_A)).isZero();
        assertThat(postingRepository.totalOfSystem()).isZero();
    }

    @Test
    @DisplayName("REVERSAL không có referenceId, hoặc trỏ vào bút toán không tồn tại, đều bị từ chối")
    void reversal_bat_buoc_co_reference_hop_le() {
        seedWallet(WALLET_A);
        ledgerService.postJournal(topUp(WALLET_A, 100_000));

        assertThatThrownBy(() -> ledgerService.postJournal(new PostJournalCommand(
                UUID.randomUUID(), EJournalType.REVERSAL, null, ECurrency.VND,
                List.of(new Leg(WALLET_A, -50_000), new Leg(EXTERNAL_BANK, 50_000)))))
                .isInstanceOf(BadRequestException.class);

        assertThatThrownBy(() -> ledgerService.postJournal(new PostJournalCommand(
                UUID.randomUUID(), EJournalType.REVERSAL, UUID.randomUUID(), ECurrency.VND,
                List.of(new Leg(WALLET_A, -50_000), new Leg(EXTERNAL_BANK, 50_000)))))
                .isInstanceOf(ResourceNotFoundException.class);

        // Ngược lại: bút toán thường mang referenceId là dấu hiệu hiểu sai ngữ nghĩa
        assertThatThrownBy(() -> ledgerService.postJournal(new PostJournalCommand(
                UUID.randomUUID(), EJournalType.TRANSFER, UUID.randomUUID(), ECurrency.VND,
                List.of(new Leg(WALLET_A, -50_000), new Leg(EXTERNAL_BANK, 50_000)))))
                .isInstanceOf(BadRequestException.class);

        assertThat(journalEntryRepository.count()).as("chỉ còn bút toán nạp tiền ban đầu").isEqualTo(1);
    }

    // ------------------------------------------------------------------
    // Test 7 — ⭐ Append-only được cưỡng chế ở tầng DB (plan §3)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("DB từ chối UPDATE và DELETE trên bút toán, kể cả bằng SQL trực tiếp")
    void so_cai_la_append_only_o_tang_db() {
        seedWallet(WALLET_A);
        ledgerService.postJournal(topUp(WALLET_A, 100_000));

        // SQL trực tiếp = con đường đi tắt duy nhất còn lại sau khi entity đã bỏ setter và
        // repository đã bỏ delete(). Trigger phải chặn được cả đường này.
        assertThatThrownBy(() -> jdbcTemplate.execute("UPDATE postings SET amount_minor = 999999"))
                .hasMessageContaining("APPEND-ONLY");

        assertThatThrownBy(() -> jdbcTemplate.execute("DELETE FROM postings"))
                .hasMessageContaining("APPEND-ONLY");

        assertThatThrownBy(() -> jdbcTemplate.execute("DELETE FROM journal_entries"))
                .hasMessageContaining("APPEND-ONLY");

        assertThat(balanceOf(WALLET_A))
                .as("không thao tác nào xuyên qua được, số dư nguyên vẹn")
                .isEqualTo(100_000);
    }

    // ------------------------------------------------------------------
    // Test 8 — describe(): event phải nói về sổ cái, không phải về lệnh
    // ------------------------------------------------------------------
    @Test
    @DisplayName("describe() đọc lại các vế từ sổ và chọn ví bị ghi nợ làm partition key")
    void describe_doc_lai_but_toan_tu_so() {
        seedWallet(WALLET_A);
        seedWallet(WALLET_B);
        ledgerService.postJournal(topUp(WALLET_A, 100_000));

        JournalEntry entry = ledgerService.postJournal(transfer(WALLET_A, WALLET_B, 30_000));
        JournalView view = ledgerService.describe(entry);

        assertThat(view.legs()).hasSize(2);
        assertThat(view.legs()).extracting(Leg::accountRef)
                .containsExactlyInAnyOrder(WALLET_A, WALLET_B);
        assertThat(view.primaryWalletRef())
                .as("ví bị ghi nợ là ví chủ động của giao dịch")
                .isEqualTo(WALLET_A);

        // Nạp tiền không có ví nào bị ghi nợ (vế nợ là tài khoản hệ thống) -> lấy ví được ghi có
        JournalView topUpView = ledgerService.describe(
                ledgerService.postJournal(topUp(WALLET_B, 10_000)));
        assertThat(topUpView.primaryWalletRef()).isEqualTo(WALLET_B);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private void seedWallet(String externalRef) {
        accountRepository.save(Account.builder()
                .externalRef(externalRef)
                .type(ETypeAccount.USER)
                .currency(ECurrency.VND)
                .build());
    }

    /** Tiền đi vào hệ thống: ghi nợ tài khoản kỹ thuật, ghi có ví người dùng. */
    private PostJournalCommand topUp(String wallet, long amountMinor) {
        return new PostJournalCommand(UUID.randomUUID(), EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(EXTERNAL_BANK, -amountMinor), new Leg(wallet, amountMinor)));
    }

    /** Tiền rời hệ thống: ghi nợ ví người dùng, ghi có tài khoản kỹ thuật. */
    private PostJournalCommand withdraw(String wallet, long amountMinor) {
        return new PostJournalCommand(UUID.randomUUID(), EJournalType.WITHDRAW, null, ECurrency.VND,
                List.of(new Leg(wallet, -amountMinor), new Leg(EXTERNAL_BANK, amountMinor)));
    }

    private PostJournalCommand transfer(String from, String to, long amountMinor) {
        return new PostJournalCommand(UUID.randomUUID(), EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(from, -amountMinor), new Leg(to, amountMinor)));
    }

    private long balanceOf(String externalRef) {
        UUID accountId = accountRepository
                .findByExternalRefAndCurrency(externalRef, ECurrency.VND)
                .orElseThrow()
                .getId();
        return postingRepository.balanceOf(accountId);
    }
}
