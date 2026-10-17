package com.walletflow.common.contract;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.walletflow.common.event.AccountOpenedEvent;
import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.EJournalType;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.LedgerRejectedEvent;
import com.walletflow.common.event.Leg;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.PostJournalCommand;
import com.walletflow.common.event.TransactionCompletedEvent;
import com.walletflow.common.event.TransactionFailedEvent;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Hợp đồng message giữa các service (roadmap 1.3).
 *
 * <p>Ba chiều, mỗi chiều chặn một cách vỡ khác nhau:
 * <ol>
 *   <li><b>Producer:</b> record Java hiện tại serialize ra JSON đúng schema. Ai đổi tên/xoá field
 *       của record mà không đổi schema thì đỏ ở đây — thay vì đỏ ở consumer, trên production.</li>
 *   <li><b>Consumer:</b> message v1 ĐÃ TỪNG đi trên dây (fixtures) vẫn đọc được bằng record hiện
 *       tại — kể cả message mang field lạ do producer mới hơn thêm vào.</li>
 *   <li><b>Schema có răng:</b> message thiếu field bắt buộc hoặc sai kiểu bị schema từ chối. Một
 *       schema chấp nhận mọi thứ thì hai chiều trên là kiểm tra suông.</li>
 * </ol>
 *
 * <p>ObjectMapper ở đây cấu hình như của Spring Boot (ngày giờ dạng chuỗi ISO, bỏ qua field lạ). Các
 * service còn kiểm message THẬT chúng phát ra ở integration test, nên khác biệt cấu hình nếu có cũng
 * lộ ra.
 */
class EventContractTest {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .findAndAddModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();
    private static final EnvelopeReader READER = new EnvelopeReader(MAPPER);
    private static final UUID TENANT = UUID.randomUUID();

    // ------------------------------------------------------------------
    // 1 — Producer: record hiện tại -> JSON đúng schema
    // ------------------------------------------------------------------
    static Stream<Arguments> producedMessages() {
        UUID wallet = UUID.randomUUID();
        List<Leg> legs = List.of(new Leg("EXTERNAL_BANK", -1_000), new Leg(wallet.toString(), 1_000));
        return Stream.of(
                Arguments.of(WalletTopics.EVT_LEDGER_POSTED, "LedgerPosted",
                        new LedgerPostedEvent(UUID.randomUUID(), UUID.randomUUID(), EJournalType.TOPUP,
                                ECurrency.VND, legs)),
                Arguments.of(WalletTopics.EVT_LEDGER_REJECTED, "LedgerRejected",
                        new LedgerRejectedEvent(UUID.randomUUID(), EJournalType.WITHDRAW,
                                "INSUFFICIENT_FUNDS", "Không đủ số dư")),
                Arguments.of(WalletTopics.EVT_ACCOUNT_OPENED, "AccountOpened",
                        new AccountOpenedEvent(UUID.randomUUID(), wallet.toString(), ECurrency.VND,
                                EAccountStatus.ACTIVE, 0)),
                Arguments.of(WalletTopics.CMD_OPEN_ACCOUNT, "OpenAccount",
                        new OpenAccountCommand(wallet.toString(), ECurrency.VND)),
                Arguments.of(WalletTopics.CMD_POST_JOURNAL, "PostJournal",
                        new PostJournalCommand(UUID.randomUUID(), EJournalType.TRANSFER, null,
                                ECurrency.VND, legs)),
                Arguments.of(WalletTopics.EVT_TXN_COMPLETED, "TransactionCompleted",
                        new TransactionCompletedEvent(UUID.randomUUID(), EJournalType.TRANSFER,
                                UUID.randomUUID(), wallet, UUID.randomUUID(), 1_000, ECurrency.VND,
                                UUID.randomUUID())),
                Arguments.of(WalletTopics.EVT_TXN_FAILED, "TransactionFailed",
                        new TransactionFailedEvent(UUID.randomUUID(), EJournalType.TRANSFER,
                                UUID.randomUUID(), wallet, UUID.randomUUID(), 1_000, ECurrency.VND,
                                "INSUFFICIENT_FUNDS")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("producedMessages")
    @DisplayName("Producer: record hiện tại serialize ra đúng schema của topic")
    void producer_dung_hop_dong(String topic, String eventType, Object payload) throws Exception {
        String json = MAPPER.writeValueAsString(
                EventEnvelope.of(UUID.randomUUID(), eventType, TENANT, "trace-1", payload, Instant.now()));

        EventSchemas.assertValid(topic, json);
    }

    // ------------------------------------------------------------------
    // 2 — Consumer: message v1 đã từng đi trên dây vẫn đọc được
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Consumer: fixture v1 hợp lệ với schema và đọc được bằng record hiện tại")
    void consumer_doc_duoc_fixture_v1() throws IOException {
        String posted = fixture("ledger.event.posted.v1.json");
        EventSchemas.assertValid(WalletTopics.EVT_LEDGER_POSTED, posted);
        LedgerPostedEvent event = READER.read(posted, LedgerPostedEvent.class).payload();
        assertThat(event.legs()).hasSize(2);
        assertThat(event.type()).isEqualTo(EJournalType.TRANSFER);

        String opened = fixture("ledger.event.account-opened.v1.json");
        EventSchemas.assertValid(WalletTopics.EVT_ACCOUNT_OPENED, opened);
        assertThat(READER.read(opened, AccountOpenedEvent.class).payload().statusVersion()).isEqualTo(2);
    }

    @Test
    @DisplayName("Consumer: message có field LẠ (producer mới hơn thêm vào) vẫn hợp lệ và đọc được")
    void consumer_chiu_duoc_field_moi() throws IOException {
        String json = fixture("ledger.event.posted.v1-with-unknown-field.json");

        EventSchemas.assertValid(WalletTopics.EVT_LEDGER_POSTED, json);
        assertThat(READER.read(json, LedgerPostedEvent.class).payload().legs())
                .as("luật tiến hoá 'chỉ thêm field optional' chỉ đứng vững nếu consumer bỏ qua field lạ")
                .hasSize(2);
    }

    // ------------------------------------------------------------------
    // 3 — Schema có răng
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Schema từ chối: thiếu tenant, vế bằng 0, ref ví không phải UUID, trạng thái lạ")
    void schema_tu_choi_message_sai() throws IOException {
        String posted = fixture("ledger.event.posted.v1.json");

        assertThat(EventSchemas.validate(WalletTopics.EVT_LEDGER_POSTED,
                posted.replace("\"tenantId\"", "\"tenantIdX\"")))
                .as("thiếu tenantId").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.EVT_LEDGER_POSTED,
                posted.replace("\"amountMinor\": 250000", "\"amountMinor\": 0")))
                .as("vế bằng 0 là vô nghĩa").isNotEmpty();

        String open = MAPPER.writeValueAsString(EventEnvelope.of(UUID.randomUUID(), "OpenAccount", TENANT,
                null, new OpenAccountCommand("WALLET-001", ECurrency.VND), Instant.now()));
        assertThat(EventSchemas.validate(WalletTopics.CMD_OPEN_ACCOUNT, open))
                .as("ref không phải UUID — ledger sẽ đẩy vào DLT, nên producer phải bị chặn từ test")
                .isNotEmpty();

        String opened = fixture("ledger.event.account-opened.v1.json");
        assertThat(EventSchemas.validate(WalletTopics.EVT_ACCOUNT_OPENED,
                opened.replace("\"FROZEN\"", "\"SUSPENDED\"")))
                .as("trạng thái ngoài enum").isNotEmpty();
    }

    private static String fixture(String name) throws IOException {
        try (InputStream stream = EventContractTest.class.getResourceAsStream("/contract/fixtures/" + name)) {
            assertThat(stream).as("fixture " + name).isNotNull();
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
