package com.walletflow.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.LedgerRejectedEvent;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.exception.AppException;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ConflictException;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.listener.LedgerCommandListener;
import com.walletflow.ledger.repositories.AccountRepository;
import com.walletflow.ledger.repositories.PostingRepository;
import com.walletflow.ledger.services.AccountService;
import com.walletflow.ledger.services.LedgerCommandHandler;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Hàng rào lệnh nạp bị từ chối (roadmap 2.4b, ADR-020): ledger đã nói "không" với một lệnh NẠP thì nói
 * "không" mãi mãi.
 *
 * <p>Vì sao đây là tính chất về tiền: lời từ chối lệnh nạp kích hoạt HOÀN TIỀN về thẻ qua cổng thanh toán.
 * Nếu sau đó một bản gửi lại (job quét của saga gửi lại với eventId mới) ghi sổ được — vì tài khoản đã được
 * mở lại — người dùng nhận tiền hai lần: ví được cộng VÀ tiền về thẻ. {@code SUM(postings)=0} vẫn đúng nên
 * không phép kiểm bảo toàn nào bắt được; chỉ test "từ chối rồi thì không ghi sổ nữa" mới bắt được.
 *
 * <p>Đi qua {@link LedgerCommandListener} (lớp thật, gọi trực tiếp với envelope JSON) để phủ cả đường
 * "AppException → reject" mà Kafka sẽ chạy; không cần broker vì không có gì ở đây phụ thuộc vào việc
 * message đi qua mạng — outbox được đọc thẳng từ bảng.
 */
@SpringBootTest(
        properties = {
                "eureka.client.enabled=false",
                "spring.cloud.config.enabled=false",
                "outbox.relay.delay-ms=3600000",
                "spring.kafka.listener.auto-startup=false",
                "walletflow.kafka.declare-topics=false",
                "management.tracing.enabled=false"
        })
@Testcontainers
class TopupRejectionFenceIT {

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
    LedgerCommandListener listener;
    @Autowired
    LedgerCommandHandler handler;
    @Autowired
    AccountService accountService;
    @Autowired
    AccountRepository accountRepository;
    @Autowired
    PostingRepository postingRepository;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    EnvelopeReader envelopeReader;

    @BeforeEach
    void reset() {
        // TRUNCATE không kích hoạt trigger append-only theo hàng (xem V2), nên dọn được cả rejected_topups.
        jdbcTemplate.execute("TRUNCATE postings, journal_entries, rejected_topups");
        jdbcTemplate.execute("DELETE FROM outbox_event");
        jdbcTemplate.execute("DELETE FROM processed_event");
        jdbcTemplate.execute("DELETE FROM accounts WHERE type = 'USER'");
    }

    // ------------------------------------------------------------------
    // 1 — Từ chối rồi mở lại: gửi lại VẪN bị từ chối, sổ không có gì
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Lệnh nạp bị từ chối, ví được mở lại, gửi lại cùng giao dịch với eventId mới: VẪN bị từ chối, sổ không đổi")
    void lenh_nap_bi_tu_choi_roi_mo_lai_vi_gui_lai_van_tu_choi() {
        String wallet = newWalletRef();
        open(wallet);
        setStatus(wallet, EAccountStatus.CLOSED);
        UUID transactionId = UUID.randomUUID();

        receive(UUID.randomUUID(), topUp(transactionId, wallet, 100_000));

        assertThat(fenceReasonCode(transactionId))
                .as("lần từ chối đầu dựng hàng rào, mang đúng mã nghiệp vụ")
                .isEqualTo("ACCOUNT_CLOSED");
        assertThat(rejectedEvents(transactionId)).hasSize(1);

        // Ví được mở lại (vận hành gỡ khoá) rồi job quét của saga gửi lại đúng lệnh cũ với eventId mới.
        setStatus(wallet, EAccountStatus.ACTIVE);
        receive(UUID.randomUUID(), topUp(transactionId, wallet, 100_000));

        List<EventEnvelope<LedgerRejectedEvent>> rejected = rejectedEvents(transactionId);
        assertThat(rejected)
                .as("bản gửi lại phải nhận lại lời từ chối, không phải bút toán — saga mới được phép hoàn tiền")
                .hasSize(2);
        assertThat(rejected).extracting(e -> e.payload().reasonCode()).containsOnly("ACCOUNT_CLOSED");
        assertThat(countJournals(transactionId))
                .as("ghi sổ ở đây = người dùng nhận tiền hai lần (ví cộng + tiền về thẻ)")
                .isZero();
        assertThat(balanceOf(wallet)).isZero();
        assertThat(postingRepository.totalOfTenant(TENANT)).isZero();

        // Đối chứng: ví thực sự nạp được sau khi mở lại — nếu không, các khẳng định trên đúng một cách vô nghĩa.
        UUID freshTransaction = UUID.randomUUID();
        receive(UUID.randomUUID(), topUp(freshTransaction, wallet, 40_000));
        assertThat(countJournals(freshTransaction)).isEqualTo(1);
        assertThat(balanceOf(wallet)).isEqualTo(40_000);
        assertThat(postingRepository.totalOfTenant(TENANT)).isZero();
    }

    // ------------------------------------------------------------------
    // 2 — Từ chối lại phát đúng mã của lần đầu, cùng eventId
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Từ chối lại phát event với CÙNG eventId tất định và CÙNG mã lỗi của lần từ chối đầu")
    void tu_choi_lai_phat_dung_ma_lan_dau() {
        String wallet = newWalletRef();
        open(wallet);
        setStatus(wallet, EAccountStatus.CLOSED);
        UUID transactionId = UUID.randomUUID();

        receive(UUID.randomUUID(), topUp(transactionId, wallet, 70_000));
        // Lần sau, ví không còn đóng và cũng không còn là lý do từ chối nữa — chỉ hàng rào còn nói "không".
        // Mã lỗi của lần đầu phải được giữ, nếu không event (cùng eventId) đổi nội dung giữa các lần phát.
        setStatus(wallet, EAccountStatus.ACTIVE);
        receive(UUID.randomUUID(), topUp(transactionId, wallet, 70_000));
        receive(UUID.randomUUID(), topUp(transactionId, wallet, 70_000));

        List<EventEnvelope<LedgerRejectedEvent>> rejected = rejectedEvents(transactionId);
        assertThat(rejected).hasSize(3);
        assertThat(rejected).extracting(EventEnvelope::eventId)
                .as("eventId tất định theo giao dịch -> consumer dedup được bản phát lại")
                .containsOnly(rejected.get(0).eventId());
        assertThat(rejected).extracting(e -> e.payload().reasonCode()).containsOnly("ACCOUNT_CLOSED");
        assertThat(rejected).extracting(e -> e.payload().reason())
                .as("cùng eventId thì cùng NỘI DUNG, kể cả lời giải thích")
                .containsOnly(rejected.get(0).payload().reason());
        assertThat(rejected).extracting(e -> e.payload().type()).containsOnly(EJournalType.TOPUP);
        assertThat(countFences(transactionId)).as("hàng rào chỉ có một hàng").isEqualTo(1);
    }

    @Test
    @DisplayName("Lần từ chối sau mang mã KHÁC: event vẫn phát đúng mã và lời giải thích của lần đầu, cùng eventId; hàng rào không đổi")
    void tu_choi_sau_voi_ma_khac_van_phat_ma_lan_dau() {
        String wallet = newWalletRef();
        open(wallet);
        setStatus(wallet, EAccountStatus.CLOSED);
        UUID transactionId = UUID.randomUUID();
        PostJournalCommand command = topUp(transactionId, wallet, 50_000);
        receive(UUID.randomUUID(), command);
        String firstReason = jdbcTemplate.queryForObject(
                "SELECT reason FROM rejected_topups WHERE tenant_id = ? AND transaction_id = ?",
                String.class, TENANT, transactionId);
        assertThat(fenceReasonCode(transactionId)).isEqualTo("ACCOUNT_CLOSED");

        // Lần từ chối sau do một lý do KHÁC (ví dụ phiên bản code khác): vẫn phải nói đúng điều đã nói lần đầu
        TenantContext.runWith(TENANT, () -> handler.reject(UUID.randomUUID(), command,
                new BadRequestException("X_OTHER", "khác")));

        List<EventEnvelope<LedgerRejectedEvent>> rejected = rejectedEvents(transactionId);
        assertThat(rejected).hasSize(2);
        assertThat(rejected).extracting(EventEnvelope::eventId).containsOnly(rejected.get(0).eventId());
        assertThat(rejected).extracting(e -> e.payload().reasonCode())
                .as("cùng eventId thì cùng nội dung: mã của LẦN ĐẦU, không phải mã của lần gọi này")
                .containsOnly("ACCOUNT_CLOSED");
        assertThat(rejected).extracting(e -> e.payload().reason()).containsOnly(firstReason);
        assertThat(fenceReasonCode(transactionId)).isEqualTo("ACCOUNT_CLOSED");
        assertThat(countFences(transactionId)).isEqualTo(1);
        assertThat(countJournals(transactionId)).isZero();
    }

    // ------------------------------------------------------------------
    // 3 — Chuyển tiền KHÔNG dùng hàng rào (ADR-014 "ledger thắng")
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Chuyển tiền bị từ chối không dựng hàng rào: nạp đủ tiền rồi gửi lại cùng giao dịch thì ghi sổ được")
    void chuyen_tien_bi_tu_choi_khong_dung_hang_rao() {
        String sender = newWalletRef();
        String receiver = newWalletRef();
        open(sender);
        open(receiver);
        UUID transactionId = UUID.randomUUID();

        receive(UUID.randomUUID(), transfer(transactionId, sender, receiver, 50_000));

        List<EventEnvelope<LedgerRejectedEvent>> rejected = rejectedEvents(transactionId);
        assertThat(rejected).hasSize(1);
        assertThat(rejected.get(0).payload().reasonCode()).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(countFences(transactionId))
                .as("luật của chuyển tiền giữ nguyên: hàng rào chỉ dành cho TOPUP")
                .isZero();

        receive(UUID.randomUUID(), topUp(UUID.randomUUID(), sender, 80_000));
        receive(UUID.randomUUID(), transfer(transactionId, sender, receiver, 50_000));

        assertThat(countJournals(transactionId))
                .as("ledger thắng: gửi lại sau khi đủ tiền thì ghi sổ thành công")
                .isEqualTo(1);
        assertThat(balanceOf(sender)).isEqualTo(30_000);
        assertThat(balanceOf(receiver)).isEqualTo(50_000);
        assertThat(postingRepository.totalOfTenant(TENANT)).isZero();
    }

    // ------------------------------------------------------------------
    // 4 — Lời từ chối CŨ sau khi đã ghi sổ thì không được phát
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Lời từ chối cũ đến sau khi giao dịch đã ghi sổ: không phát event, không dựng hàng rào")
    void tu_choi_cu_sau_khi_da_ghi_so_thi_khong_phat() {
        String wallet = newWalletRef();
        open(wallet);
        UUID transactionId = UUID.randomUUID();
        PostJournalCommand command = topUp(transactionId, wallet, 90_000);

        receive(UUID.randomUUID(), command);
        assertThat(countJournals(transactionId)).isEqualTo(1);

        // Một consumer khác (lúc rebalance) mang tới lời từ chối sinh ra TRƯỚC khi lệnh được ghi sổ.
        TenantContext.runWith(TENANT, () -> handler.reject(
                UUID.randomUUID(), command, new ConflictException("ACCOUNT_CLOSED", "lời từ chối cũ")));

        assertThat(rejectedEvents(transactionId))
                .as("phát nó là bảo saga hoàn tiền cho một giao dịch mà tiền ĐÃ vào ví")
                .isEmpty();
        assertThat(countFences(transactionId))
                .as("có hàng rào cho giao dịch đã ghi sổ là vừa có bút toán vừa có lời từ chối")
                .isZero();
        assertThat(countJournals(transactionId)).isEqualTo(1);
        assertThat(balanceOf(wallet)).isEqualTo(90_000);
    }

    // ------------------------------------------------------------------
    // 5 — Hàng rào không gỡ được
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Hàng rào là append-only: UPDATE và DELETE đều bị trigger ở DB chặn")
    void hang_rao_khong_go_duoc() {
        String wallet = newWalletRef();
        open(wallet);
        setStatus(wallet, EAccountStatus.CLOSED);
        UUID transactionId = UUID.randomUUID();
        receive(UUID.randomUUID(), topUp(transactionId, wallet, 10_000));
        assertThat(countFences(transactionId)).isEqualTo(1);

        assertThatThrownBy(() -> jdbcTemplate.update(
                "DELETE FROM rejected_topups WHERE transaction_id = ?", transactionId))
                .hasMessageContaining("APPEND-ONLY");
        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE rejected_topups SET reason_code = 'DA_SUA' WHERE transaction_id = ?", transactionId))
                .hasMessageContaining("APPEND-ONLY");

        assertThat(fenceReasonCode(transactionId))
                .as("gỡ hoặc sửa hàng rào là mở đường cho ghi sổ giao dịch đã bị từ chối")
                .isEqualTo("ACCOUNT_CLOSED");
    }

    // ------------------------------------------------------------------
    // 6 — Đua nhau: ghi sổ thành công và từ chối cùng một giao dịch
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Ghi sổ và từ chối cùng giao dịch nạp chạy ĐỒNG THỜI: có bút toán HOẶC hàng rào, không bao giờ cả hai")
    void ghi_so_va_tu_choi_dong_thoi_chi_mot_ket_qua_thang() throws Exception {
        String wallet = newWalletRef();
        open(wallet);
        int rounds = 40;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            int journalWins = 0;
            int fenceWins = 0;
            for (int i = 0; i < rounds; i++) {
                UUID transactionId = UUID.randomUUID();
                PostJournalCommand command = topUp(transactionId, wallet, 1_000);
                CountDownLatch go = new CountDownLatch(1);

                Future<Object> poster = pool.submit(() -> {
                    go.await();
                    try {
                        TenantContext.runWith(TENANT, () -> handler.handle(UUID.randomUUID(), command));
                    } catch (AppException fencedOut) {
                        // Hàng rào thắng: lệnh ghi bị từ chối đúng luật, không phải lỗi.
                    }
                    return null;
                });
                Future<Object> rejecter = pool.submit(() -> {
                    go.await();
                    TenantContext.runWith(TENANT, () -> handler.reject(
                            UUID.randomUUID(), command, new ConflictException("ACCOUNT_CLOSED", "đóng")));
                    return null;
                });
                go.countDown();
                poster.get(30, TimeUnit.SECONDS);
                rejecter.get(30, TimeUnit.SECONDS);

                int journals = countJournals(transactionId);
                int fences = countFences(transactionId);
                assertThat(journals + fences)
                        .as("vòng %d: đúng MỘT trong {bút toán, hàng rào} (journals=%d, fences=%d)",
                                i, journals, fences)
                        .isEqualTo(1);
                if (journals == 1) {
                    journalWins++;
                    assertThat(rejectedEvents(transactionId))
                            .as("đã ghi sổ thì không có lời từ chối nào được phát")
                            .isEmpty();
                } else {
                    fenceWins++;
                    assertThat(rejectedEvents(transactionId)).isNotEmpty();
                }
            }
            assertThat(journalWins + fenceWins).isEqualTo(rounds);
            assertThat(postingRepository.totalOfTenant(TENANT)).isZero();
            assertThat(balanceOf(wallet))
                    .as("số dư = tổng các lệnh nạp THẮNG, không hơn")
                    .isEqualTo(journalWins * 1_000L);
        } finally {
            pool.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private static String newWalletRef() {
        return UUID.randomUUID().toString();
    }

    private void open(String ref) {
        TenantContext.runWith(TENANT,
                () -> accountService.openUserAccount(new OpenAccountCommand(ref, ECurrency.VND)));
    }

    private void setStatus(String ref, EAccountStatus status) {
        jdbcTemplate.update("UPDATE accounts SET status = ? WHERE tenant_id = ? AND external_ref = ?",
                status.name(), TENANT, ref);
    }

    private static PostJournalCommand topUp(UUID transactionId, String wallet, long amountMinor) {
        return new PostJournalCommand(transactionId, EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(BANK, -amountMinor), new Leg(wallet, amountMinor)));
    }

    private static PostJournalCommand transfer(UUID transactionId, String from, String to, long amountMinor) {
        return new PostJournalCommand(transactionId, EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(from, -amountMinor), new Leg(to, amountMinor)));
    }

    /** Đóng vai Kafka: giao một PostJournalCommand đã bọc envelope cho listener thật. */
    private void receive(UUID eventId, PostJournalCommand command) {
        EventEnvelope<Object> envelope =
                EventEnvelope.of(eventId, "PostJournal", TENANT, null, command, Instant.now());
        try {
            listener.onPostJournal(objectMapper.writeValueAsString(envelope));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Các LedgerRejected đã xếp trong outbox cho giao dịch này, đã kiểm đúng schema hợp đồng. */
    private List<EventEnvelope<LedgerRejectedEvent>> rejectedEvents(UUID transactionId) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT payload FROM outbox_event WHERE topic = ? AND payload LIKE ? ORDER BY created_at, id",
                WalletTopics.EVT_LEDGER_REJECTED, "%" + transactionId + "%");
        List<EventEnvelope<LedgerRejectedEvent>> events = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            String json = (String) row.get("payload");
            EventSchemas.assertValid(WalletTopics.EVT_LEDGER_REJECTED, json);
            events.add(envelopeReader.read(json, LedgerRejectedEvent.class));
        }
        return events;
    }

    private int countJournals(UUID transactionId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM journal_entries WHERE tenant_id = ? AND transaction_id = ?",
                Integer.class, TENANT, transactionId);
        return count == null ? 0 : count;
    }

    private int countFences(UUID transactionId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM rejected_topups WHERE tenant_id = ? AND transaction_id = ?",
                Integer.class, TENANT, transactionId);
        return count == null ? 0 : count;
    }

    private String fenceReasonCode(UUID transactionId) {
        return jdbcTemplate.queryForObject(
                "SELECT reason_code FROM rejected_topups WHERE tenant_id = ? AND transaction_id = ?",
                String.class, TENANT, transactionId);
    }

    private long balanceOf(String ref) {
        UUID accountId = accountRepository
                .findByTenantIdAndExternalRefAndCurrency(TENANT, ref, ECurrency.VND)
                .orElseThrow().getId();
        return postingRepository.balanceOf(TENANT, accountId);
    }
}
