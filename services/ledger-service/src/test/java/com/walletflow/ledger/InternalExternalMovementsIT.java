package com.walletflow.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.ledger.ExternalMovement;
import com.walletflow.common.ledger.ExternalMovementsRequest;
import com.walletflow.common.ledger.TopupEntryRef;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.security.AuthHeaders;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.entity.JournalEntry;
import com.walletflow.ledger.services.AccountService;
import com.walletflow.ledger.services.LedgerService;
import com.walletflow.ledger.services.SystemAccountProvisioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Hai đường đọc phục vụ đối soát bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024):
 * {@code POST /internal/entries/external-movements} (chiều xuôi) và
 * {@code GET /internal/entries/topups} (chiều ngược, xuyên tenant).
 *
 * <p>Bên đối soát đọc KẾT LUẬN từ các phân biệt sau, nên mỗi cái có một test:
 * <ol>
 *   <li><b>Ròng, không phải tổng</b> — TOPUP rồi REVERSAL ra 0 nhưng vẫn HIỆN; nếu REVERSAL không
 *       bị trừ đi, mọi lần nạp đã hoàn sẽ thành break giả.</li>
 *   <li><b>Vắng mặt = chưa có đồng nào qua cửa ngoài</b> — TRANSFER không chạm {@code EXTERNAL_BANK}.</li>
 *   <li><b>Cô lập tenant</b> ở chiều xuôi; chiều ngược thì CỐ Ý xuyên tenant nhưng mỗi phần tử mang
 *       đúng tenant của nó.</li>
 *   <li><b>Con trỏ (created_at, id) không bỏ sót/lặp</b> kể cả khi qua chuỗi HTTP với Instant — Postgres
 *       giữ micro-giây, Java giữ nano-giây.</li>
 * </ol>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eureka.client.enabled=false",
                "spring.cloud.config.enabled=false",
                "outbox.relay.delay-ms=3600000",
                "spring.kafka.listener.auto-startup=false",
                "walletflow.kafka.declare-topics=false",
                "management.tracing.enabled=false",
                "walletflow.ledger.reconciliation.initial-delay=PT24H"
        })
@Testcontainers
class InternalExternalMovementsIT {

    @LocalServerPort
    int port;

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
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE postings, journal_entries");
        jdbcTemplate.execute("DELETE FROM outbox_event");
        jdbcTemplate.execute("DELETE FROM processed_event");
        jdbcTemplate.execute("DELETE FROM accounts WHERE type = 'USER'");
    }

    // ------------------------------------------------------------------
    // 1 — /external-movements: ròng theo lệnh
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ TOPUP x: ròng +x cho đúng lệnh đó, đúng loại tiền")
    void nap_tien_tra_ve_rong_duong_dung_so_tien() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        UUID txn = UUID.randomUUID();
        post(tenant, topUp(txn, wallet, 100_000));

        Raw response = movements(tenant, List.of(txn));

        assertThat(response.status()).isEqualTo(200);
        assertThat(asMap(response.body()))
                .as("TOPUP ghi NỢ EXTERNAL_BANK 100k: tiền rời thế giới ngoài vào ví => ròng +100k")
                .containsExactlyEntriesOf(Map.of(txn, Map.of(ECurrency.VND, 100_000L)));
    }

    @Test
    @DisplayName("⭐ TOPUP rồi REVERSAL: HIỆN với ròng 0 (không vắng mặt) — nạp đã hoàn không phải break")
    void nap_roi_dao_thi_rong_bang_0_va_van_hien() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        UUID txn = UUID.randomUUID();
        JournalEntry topup = post(tenant, topUp(txn, wallet, 100_000));
        post(tenant, new PostJournalCommand(txn, EJournalType.REVERSAL, topup.getId(), ECurrency.VND,
                List.of(new Leg(wallet, -100_000), new Leg(BANK, 100_000))));

        Raw response = movements(tenant, List.of(txn));

        assertThat(response.status()).isEqualTo(200);
        assertThat(asMap(response.body()))
                .as("REVERSAL không bị lọc ra: nếu bị lọc, ròng thành +100k và mọi lần nạp đã hoàn "
                        + "bị kêu là 'tiền vào sổ mà provider không có'")
                .containsExactlyEntriesOf(Map.of(txn, Map.of(ECurrency.VND, 0L)));
    }

    @Test
    @DisplayName("Lệnh chỉ có TRANSFER (không chạm EXTERNAL_BANK) và lệnh chưa từng ghi sổ: VẮNG MẶT")
    void transfer_va_lenh_chua_ghi_so_thi_vang_mat() {
        UUID tenant = newTenant();
        String from = openWallet(tenant);
        String to = openWallet(tenant);
        post(tenant, topUp(UUID.randomUUID(), from, 100_000));
        UUID transferTxn = UUID.randomUUID();
        post(tenant, new PostJournalCommand(transferTxn, EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(from, -30_000), new Leg(to, 30_000))));
        UUID neverPosted = UUID.randomUUID();

        Raw response = movements(tenant, List.of(transferTxn, neverPosted));

        assertThat(response.status()).isEqualTo(200);
        assertThat(asMap(response.body()))
                .as("không có vế EXTERNAL_BANK => chưa đồng nào qua cửa ngoài; không 404, không 0 giả")
                .isEmpty();
    }

    @Test
    @DisplayName("Lô nhiều lệnh (nạp, nạp+đảo, rút, chuyển, chưa ghi): mỗi lệnh trả đúng số của MÌNH")
    void lo_nhieu_lenh_moi_lenh_dung_so_cua_minh() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        String other = openWallet(tenant);
        UUID topup1 = UUID.randomUUID();
        UUID topup2 = UUID.randomUUID();
        UUID withdraw = UUID.randomUUID();
        UUID transfer = UUID.randomUUID();
        UUID neverPosted = UUID.randomUUID();
        post(tenant, topUp(topup1, wallet, 70_000));
        JournalEntry second = post(tenant, topUp(topup2, wallet, 25_000));
        post(tenant, new PostJournalCommand(topup2, EJournalType.REVERSAL, second.getId(), ECurrency.VND,
                List.of(new Leg(wallet, -25_000), new Leg(BANK, 25_000))));
        post(tenant, new PostJournalCommand(withdraw, EJournalType.WITHDRAW, null, ECurrency.VND,
                List.of(new Leg(wallet, -20_000), new Leg(BANK, 20_000))));
        post(tenant, new PostJournalCommand(transfer, EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(wallet, -5_000), new Leg(other, 5_000))));

        Raw response = movements(tenant, List.of(topup1, topup2, withdraw, transfer, neverPosted));

        assertThat(response.status()).isEqualTo(200);
        assertThat(asMap(response.body())).containsExactlyInAnyOrderEntriesOf(Map.of(
                topup1, Map.of(ECurrency.VND, 70_000L),
                topup2, Map.of(ECurrency.VND, 0L),
                withdraw, Map.of(ECurrency.VND, -20_000L)));
    }

    // ------------------------------------------------------------------
    // 2 — Cô lập tenant
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Cô lập tenant: lệnh ghi ở tenant B hỏi bằng header tenant A thì VẮNG MẶT; B vẫn thấy đúng")
    void lenh_cua_tenant_khac_thi_vang_mat() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        String walletB = openWallet(tenantB);
        UUID txn = UUID.randomUUID();
        post(tenantB, topUp(txn, walletB, 64_000));

        Raw asA = movements(tenantA, List.of(txn));
        Raw asB = movements(tenantB, List.of(txn));

        assertThat(asA.status()).isEqualTo(200);
        assertThat(asMap(asA.body()))
                .as("transactionId của tenant khác không được lộ số tiền, và trả y như id lạ")
                .isEmpty();
        assertThat(asMap(asB.body())).containsExactlyEntriesOf(Map.of(txn, Map.of(ECurrency.VND, 64_000L)));
    }

    @Test
    @DisplayName("Cùng transactionId ở hai tenant: mỗi tenant chỉ thấy ròng của RIÊNG mình")
    void cung_transaction_id_o_hai_tenant_khong_tron_so() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        String walletA = openWallet(tenantA);
        String walletB = openWallet(tenantB);
        UUID txn = UUID.randomUUID();
        post(tenantA, topUp(txn, walletA, 10_000));
        post(tenantB, topUp(txn, walletB, 90_000));

        assertThat(asMap(movements(tenantA, List.of(txn)).body()))
                .containsExactlyEntriesOf(Map.of(txn, Map.of(ECurrency.VND, 10_000L)));
        assertThat(asMap(movements(tenantB, List.of(txn)).body()))
                .as("JOIN thiếu lọc tenant sẽ cộng cả EXTERNAL_BANK của tenant kia (100k thay vì 90k)")
                .containsExactlyEntriesOf(Map.of(txn, Map.of(ECurrency.VND, 90_000L)));
    }

    // ------------------------------------------------------------------
    // 3 — Validation
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Danh sách rỗng: 400, không trả 200 + mảng rỗng khiến người gọi tưởng 'không có gì lệch'")
    void danh_sach_rong_bi_tu_choi() {
        UUID tenant = newTenant();

        Raw response = movements(tenant, List.of());

        assertThat(response.status()).isEqualTo(400);
        assertThat(response.body().path("errorCode").asText()).isNotBlank();
    }

    @Test
    @DisplayName("501 transactionId bị từ chối: 400 + errorCode TOO_MANY_TRANSACTION_IDS; đúng 500 thì được")
    void qua_500_id_thi_bi_tu_choi() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        UUID txn = UUID.randomUUID();
        post(tenant, topUp(txn, wallet, 1_000));

        Raw rejected = movements(tenant, idsIncluding(txn, ExternalMovementsRequest.MAX_IDS + 1));
        Raw accepted = movements(tenant, idsIncluding(txn, ExternalMovementsRequest.MAX_IDS));

        assertThat(rejected.status()).isEqualTo(400);
        assertThat(rejected.body().path("errorCode").asText()).isEqualTo("TOO_MANY_TRANSACTION_IDS");
        assertThat(accepted.status()).as("đúng trần thì không bị chặn").isEqualTo(200);
        assertThat(asMap(accepted.body())).containsExactlyEntriesOf(Map.of(txn, Map.of(ECurrency.VND, 1_000L)));
    }

    @Test
    @DisplayName("Danh sách chứa phần tử null: 400, không 500")
    void phan_tu_null_bi_tu_choi() {
        UUID tenant = newTenant();
        List<UUID> withNull = new ArrayList<>();
        withNull.add(UUID.randomUUID());
        withNull.add(null);

        Raw response = movements(tenant, withNull);

        assertThat(response.status()).isEqualTo(400);
    }

    // ------------------------------------------------------------------
    // 4 — /topups: xuyên tenant, chỉ TOPUP
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ /topups quét XUYÊN tenant: thấy lần nạp của cả hai tenant, mỗi phần tử mang đúng tenant/entry/lệnh")
    void topups_xuyen_tenant_mang_dung_tenant_cua_tung_phan_tu() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        String walletA = openWallet(tenantA);
        String walletB = openWallet(tenantB);
        UUID txnA = UUID.randomUUID();
        UUID txnB = UUID.randomUUID();
        JournalEntry entryA = post(tenantA, topUp(txnA, walletA, 11_000));
        JournalEntry entryB = post(tenantB, topUp(txnB, walletB, 22_000));

        List<TopupEntryRef> page = topups(null, null, future(), 500);

        assertThat(page).hasSize(2);
        Map<UUID, TopupEntryRef> byEntry = page.stream()
                .collect(Collectors.toMap(TopupEntryRef::entryId, r -> r));
        assertThat(byEntry.get(entryA.getId()).tenantId()).isEqualTo(tenantA);
        assertThat(byEntry.get(entryA.getId()).transactionId()).isEqualTo(txnA);
        assertThat(byEntry.get(entryB.getId()).tenantId()).isEqualTo(tenantB);
        assertThat(byEntry.get(entryB.getId()).transactionId()).isEqualTo(txnB);
        assertThat(page).extracting(TopupEntryRef::postedAt).doesNotContainNull();
    }

    @Test
    @DisplayName("/topups chỉ trả TOPUP: TRANSFER, WITHDRAW và REVERSAL không lọt vào")
    void topups_loai_bo_transfer_withdraw_va_reversal() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        String other = openWallet(tenant);
        UUID topupTxn = UUID.randomUUID();
        JournalEntry topup = post(tenant, topUp(topupTxn, wallet, 100_000));
        // Đảo NGAY khi ví còn đủ 100k: ledger kiểm đủ tiền với vế ghi nợ ví của REVERSAL
        post(tenant, new PostJournalCommand(topupTxn, EJournalType.REVERSAL, topup.getId(), ECurrency.VND,
                List.of(new Leg(wallet, -100_000), new Leg(BANK, 100_000))));
        // Lần nạp thứ hai cấp tiền cho TRANSFER / WITHDRAW phía sau
        JournalEntry funding = post(tenant, topUp(UUID.randomUUID(), wallet, 50_000));
        post(tenant, new PostJournalCommand(UUID.randomUUID(), EJournalType.TRANSFER, null, ECurrency.VND,
                List.of(new Leg(wallet, -10_000), new Leg(other, 10_000))));
        post(tenant, new PostJournalCommand(UUID.randomUUID(), EJournalType.WITHDRAW, null, ECurrency.VND,
                List.of(new Leg(wallet, -5_000), new Leg(BANK, 5_000))));

        List<TopupEntryRef> page = topups(null, null, future(), 500);

        assertThat(page).extracting(TopupEntryRef::entryId)
                .as("đúng hai TOPUP; REVERSAL cùng transactionId, TRANSFER và WITHDRAW không lọt vào")
                .containsExactlyInAnyOrder(topup.getId(), funding.getId());
    }

    @Test
    @DisplayName("`before` loại bút toán ghi tại/sau mốc: mốc = postedAt của bút toán thứ 2 thì chỉ còn bút toán thứ 1")
    void before_loai_but_toan_tai_va_sau_moc() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        for (int i = 0; i < 3; i++) {
            post(tenant, topUp(UUID.randomUUID(), wallet, 1_000 + i));
        }
        List<DbEntry> inDb = topupsInDb();
        assertThat(inDb).hasSize(3);
        assertThat(inDb.get(1).createdAt()).as("dựng ba mốc phân biệt").isAfter(inDb.get(0).createdAt());

        List<TopupEntryRef> beforeSecond = topups(null, null, inDb.get(1).createdAt(), 500);
        List<TopupEntryRef> beforeFirst = topups(null, null, inDb.get(0).createdAt(), 500);

        assertThat(beforeSecond).extracting(TopupEntryRef::entryId)
                .as("created_at < before là NGHIÊM NGẶT: bút toán đúng tại mốc không lọt vào")
                .containsExactly(inDb.get(0).id());
        assertThat(beforeFirst).isEmpty();
    }

    // ------------------------------------------------------------------
    // 5 — /topups: phân trang bằng con trỏ
    // ------------------------------------------------------------------
    @Test
    @DisplayName("⭐ Phân trang limit=1 và limit=2 bằng con trỏ (postedAt, entryId) trả về qua HTTP: đủ, đúng thứ tự, không lặp, không hụt")
    void phan_trang_bang_con_tro_di_het_khong_lap_khong_hut() {
        UUID tenantA = newTenant();
        UUID tenantB = newTenant();
        String walletA = openWallet(tenantA);
        String walletB = openWallet(tenantB);
        for (int i = 0; i < 5; i++) {
            post(i % 2 == 0 ? tenantA : tenantB, topUp(UUID.randomUUID(), i % 2 == 0 ? walletA : walletB, 1_000 + i));
        }
        List<DbEntry> expected = topupsInDb();
        assertThat(expected).hasSize(5);

        for (int limit : new int[]{1, 2}) {
            List<TopupEntryRef> walked = walk(limit, future());

            assertThat(walked).extracting(TopupEntryRef::entryId)
                    .as("limit=%d: đúng thứ tự (created_at, id), không lặp, không hụt", limit)
                    .containsExactlyElementsOf(expected.stream().map(DbEntry::id).toList());
            assertThat(walked).extracting(TopupEntryRef::postedAt)
                    .as("postedAt trả về giữ nguyên micro-giây của DB — con trỏ đi vòng qua chuỗi ISO vẫn đúng")
                    .containsExactlyElementsOf(expected.stream().map(DbEntry::createdAt).toList());
        }
    }

    @Test
    @DisplayName("⭐ Nhiều bút toán CÙNG created_at: con trỏ (created_at, id) phân xử bằng id — không hụt, không lặp")
    void cung_created_at_thi_phan_xu_bang_id() {
        UUID tenant = newTenant();
        Instant sameInstant = Instant.parse("2020-01-01T00:00:00.123456Z");
        // id được chọn tăng dần để thứ tự mong đợi hiển nhiên; chèn đảo ngược để khỏi trùng thứ tự ghi.
        List<UUID> ids = IntStream.rangeClosed(1, 5)
                .mapToObj(i -> UUID.fromString("00000000-0000-0000-0000-00000000000" + i)).toList();
        for (int i = ids.size() - 1; i >= 0; i--) {
            jdbcTemplate.update("""
                    INSERT INTO journal_entries (id, transaction_id, type, created_at, tenant_id)
                    VALUES (?, ?, 'TOPUP', ?, ?)
                    """, ids.get(i), UUID.randomUUID(), java.sql.Timestamp.from(sameInstant), tenant);
        }

        List<TopupEntryRef> walked = walk(2, sameInstant.plusSeconds(1));

        assertThat(walked).extracting(TopupEntryRef::entryId)
                .as("cùng mốc thời gian: nếu con trỏ chỉ so created_at thì trang sau bỏ qua toàn bộ hàng "
                        + "cùng mốc (hụt) hoặc quét lại chúng (lặp)")
                .containsExactlyElementsOf(ids);
        assertThat(walked).extracting(TopupEntryRef::postedAt).containsOnly(sameInstant);
    }

    @Test
    @DisplayName("Con trỏ ở sau bút toán cuối: trang rỗng (kết thúc quét), không lỗi")
    void con_tro_o_cuoi_thi_trang_rong() {
        UUID tenant = newTenant();
        String wallet = openWallet(tenant);
        post(tenant, topUp(UUID.randomUUID(), wallet, 5_000));
        DbEntry last = topupsInDb().getLast();

        List<TopupEntryRef> page = topups(last.createdAt(), last.id(), future(), 10);

        assertThat(page).isEmpty();
    }

    // ------------------------------------------------------------------
    // 6 — /topups: validation
    // ------------------------------------------------------------------
    @Test
    @DisplayName("limit=0 và limit=501 bị từ chối: 400 + errorCode INVALID_PAGE_LIMIT; limit=500 được")
    void limit_ngoai_khoang_bi_tu_choi() {
        Raw zero = rawTopups("before", future().toString(), "limit", "0");
        Raw over = rawTopups("before", future().toString(), "limit", "501");
        Raw atLimit = rawTopups("before", future().toString(), "limit", "500");

        assertThat(zero.status()).isEqualTo(400);
        assertThat(zero.body().path("errorCode").asText()).isEqualTo("INVALID_PAGE_LIMIT");
        assertThat(over.status()).isEqualTo(400);
        assertThat(over.body().path("errorCode").asText()).isEqualTo("INVALID_PAGE_LIMIT");
        assertThat(atLimit.status()).as("đúng trần thì không bị chặn").isEqualTo(200);
    }

    @Test
    @DisplayName("Thiếu `before`: 400 — không âm thầm quét luôn cả bút toán chưa yên")
    void thieu_before_bi_tu_choi() {
        Raw response = rawTopups("limit", "10");

        assertThat(response.status()).isEqualTo(400);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private record Raw(int status, JsonNode body) {
    }

    private record DbEntry(UUID id, Instant createdAt) {
    }

    private Raw movements(UUID tenant, List<UUID> ids) {
        return RestClient.create("http://localhost:" + port).post()
                .uri("/internal/entries/external-movements")
                .header(AuthHeaders.TENANT_ID, tenant.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new ExternalMovementsRequest(ids))
                .exchange((request, response) ->
                        new Raw(response.getStatusCode().value(), response.bodyTo(JsonNode.class)));
    }

    /** Đọc mảng JSON thành {@code transactionId → currency → net} qua record hợp đồng thật. */
    private static Map<UUID, Map<ECurrency, Long>> asMap(JsonNode body) {
        assertThat(body.isArray()).as("200 phải là một mảng, nhận: %s", body).isTrue();
        Map<UUID, Map<ECurrency, Long>> result = new HashMap<>();
        body.forEach(node -> {
            ExternalMovement m = new ExternalMovement(UUID.fromString(node.path("transactionId").asText()),
                    ECurrency.valueOf(node.path("currency").asText()), node.path("netInMinor").asLong());
            assertThat(result.computeIfAbsent(m.transactionId(), id -> new HashMap<>())
                    .put(m.currency(), m.netInMinor()))
                    .as("mỗi (lệnh, loại tiền) đúng một dòng").isNull();
        });
        return result;
    }

    private static List<UUID> idsIncluding(UUID real, int total) {
        List<UUID> ids = new ArrayList<>();
        ids.add(real);
        while (ids.size() < total) {
            ids.add(UUID.randomUUID());
        }
        return ids;
    }

    /**
     * Dựng URL Y HỆT {@code HttpLedgerClient} của bank-adapter (UriBuilder + Instant/UUID là object),
     * để chứng minh định dạng chuỗi Instant đi vòng được qua HTTP.
     */
    private List<TopupEntryRef> topups(Instant afterPostedAt, UUID afterId, Instant before, int limit) {
        return RestClient.create("http://localhost:" + port).get()
                .uri(builder -> {
                    builder.path("/internal/entries/topups")
                            .queryParam("before", before)
                            .queryParam("limit", limit);
                    if (afterPostedAt != null && afterId != null) {
                        builder.queryParam("afterPostedAt", afterPostedAt).queryParam("afterId", afterId);
                    }
                    return builder.build();
                })
                .retrieve()
                .body(new ParameterizedTypeReference<List<TopupEntryRef>>() {
                });
    }

    /** Đi hết mọi trang, mỗi trang lấy con trỏ từ phần tử CUỐI của trang trước. */
    private List<TopupEntryRef> walk(int limit, Instant before) {
        List<TopupEntryRef> all = new ArrayList<>();
        Instant cursorAt = null;
        UUID cursorId = null;
        for (int guard = 0; guard < 100; guard++) {
            List<TopupEntryRef> page = topups(cursorAt, cursorId, before, limit);
            assertThat(page.size()).isLessThanOrEqualTo(limit);
            if (page.isEmpty()) {
                return all;
            }
            all.addAll(page);
            TopupEntryRef last = page.getLast();
            cursorAt = last.postedAt();
            cursorId = last.entryId();
        }
        throw new AssertionError("phân trang không kết thúc sau 100 trang — con trỏ không tiến");
    }

    /** Gọi thô để thử tham số sai; cặp {@code key, value, key, value...}. */
    private Raw rawTopups(String... keyValues) {
        return RestClient.create("http://localhost:" + port).get()
                .uri(builder -> {
                    builder.path("/internal/entries/topups");
                    for (int i = 0; i < keyValues.length; i += 2) {
                        builder.queryParam(keyValues[i], keyValues[i + 1]);
                    }
                    return builder.build();
                })
                .exchange((request, response) ->
                        new Raw(response.getStatusCode().value(), response.bodyTo(JsonNode.class)));
    }

    /** Sự thật từ DB (thứ tự và micro-giây) để so với những gì API trả. */
    private List<DbEntry> topupsInDb() {
        return jdbcTemplate.query("""
                SELECT id, created_at FROM journal_entries WHERE type = 'TOPUP' ORDER BY created_at, id
                """, (rs, i) -> new DbEntry(rs.getObject("id", UUID.class),
                rs.getObject("created_at", OffsetDateTime.class).toInstant()));
    }

    private static Instant future() {
        return Instant.now().plusSeconds(3600);
    }

    /** Tenant mới có bộ tài khoản hệ thống RIÊNG (ADR-010), nên mỗi test tự dựng tenant của mình. */
    private UUID newTenant() {
        UUID tenant = UUID.randomUUID();
        provisioner.provision(tenant);
        return tenant;
    }

    private String openWallet(UUID tenant) {
        String ref = UUID.randomUUID().toString();
        TenantContext.callWith(tenant,
                () -> accountService.openUserAccount(new OpenAccountCommand(ref, ECurrency.VND)));
        return ref;
    }

    private JournalEntry post(UUID tenant, PostJournalCommand command) {
        return TenantContext.callWith(tenant, () -> ledgerService.postJournal(command));
    }

    private static PostJournalCommand topUp(UUID transactionId, String wallet, long amountMinor) {
        return new PostJournalCommand(transactionId, EJournalType.TOPUP, null, ECurrency.VND,
                List.of(new Leg(BANK, -amountMinor), new Leg(wallet, amountMinor)));
    }
}
