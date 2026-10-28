package com.walletflow.common.contract;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.walletflow.common.event.AccountOpenedEvent;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.BankChargePendingEvent;
import com.walletflow.common.event.BankCompletedEvent;
import com.walletflow.common.event.BankFailedEvent;
import com.walletflow.common.event.BankRefundCommand;
import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.EBankOperation;
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
                                "INSUFFICIENT_FUNDS")),
                Arguments.of(WalletTopics.EVT_TXN_COMPLETED, "TransactionCompleted",
                        // 2.4a: lệnh NẠP không có ví nguồn — tiền đến từ ngân hàng (ADR-019)
                        new TransactionCompletedEvent(UUID.randomUUID(), EJournalType.TOPUP,
                                UUID.randomUUID(), null, wallet, 500_000, ECurrency.VND,
                                UUID.randomUUID())),
                Arguments.of(WalletTopics.EVT_TXN_FAILED, "TransactionFailed",
                        new TransactionFailedEvent(UUID.randomUUID(), EJournalType.TOPUP,
                                UUID.randomUUID(), null, wallet, 500_000, ECurrency.VND,
                                "PAYMENT_EXPIRED")),
                Arguments.of(WalletTopics.CMD_POST_JOURNAL, "PostJournal",
                        // Lệnh ghi sổ của một lần nạp: tiền rời EXTERNAL_BANK và vào ví
                        new PostJournalCommand(UUID.randomUUID(), EJournalType.TOPUP, null,
                                ECurrency.VND, legs)),
                Arguments.of(WalletTopics.CMD_POST_JOURNAL, "PostJournal",
                        // 2.5 PR4: lệnh ĐẢO bút toán (ADR-022). Loại DUY NHẤT mà `referenceId` khác
                        // null — nó trỏ về bút toán bị đảo, và thiếu nó thì ledger từ chối dứt khoát.
                        // Schema .v1 cho phép `referenceId` là uuid|null nhưng KHÔNG cưỡng chế được
                        // "bắt buộc với REVERSAL" (không có if/then); fixture này là nơi ghim hình
                        // dạng đúng để một thay đổi phá vỡ bị bắt ở contract test.
                        new PostJournalCommand(UUID.randomUUID(), EJournalType.REVERSAL,
                                UUID.randomUUID(), ECurrency.VND, legs)),
                Arguments.of(WalletTopics.CMD_BANK_CHARGE, "BankCharge",
                        new BankChargeCommand(UUID.randomUUID(), wallet, 500_000, ECurrency.VND)),
                Arguments.of(WalletTopics.CMD_BANK_CHARGE, "BankCharge",
                        // 2.2b: IP người dùng cho cổng thanh toán (field optional)
                        new BankChargeCommand(UUID.randomUUID(), wallet, 500_000, ECurrency.VND,
                                "2001:db8:85a3::8a2e:370:7334")),
                Arguments.of(WalletTopics.EVT_BANK_CHARGE_PENDING, "BankChargePending",
                        new BankChargePendingEvent(UUID.randomUUID(), wallet, "mock", "MOCK-1",
                                "https://sandbox.example/pay/MOCK-1")),
                Arguments.of(WalletTopics.EVT_BANK_COMPLETED, "BankCompleted",
                        new BankCompletedEvent(UUID.randomUUID(), EBankOperation.CHARGE, wallet, "mock",
                                "MOCK-1", 500_000, ECurrency.VND)),
                Arguments.of(WalletTopics.EVT_BANK_FAILED, "BankFailed",
                        new BankFailedEvent(UUID.randomUUID(), EBankOperation.CHARGE, wallet, "mock",
                                "MOCK-1", "DECLINED")),
                Arguments.of(WalletTopics.EVT_BANK_FAILED, "BankFailed",
                        // Provider từ chối ngay lúc tạo phiên: chưa có mã phía provider
                        new BankFailedEvent(UUID.randomUUID(), EBankOperation.CHARGE, wallet, "mock",
                                null, "DECLINED")),
                Arguments.of(WalletTopics.CMD_BANK_REFUND, "BankRefund",
                        // 2.4b: bù trừ saga nạp tiền — hoàn trọn lần thu khi ledger từ chối ghi sổ
                        new BankRefundCommand(UUID.randomUUID(), wallet, 500_000, ECurrency.VND,
                                "ACCOUNT_CLOSED")),
                Arguments.of(WalletTopics.EVT_BANK_COMPLETED, "BankCompleted",
                        // Kết quả của lần HOÀN đi chung topic với lần thu, phân biệt bằng operation
                        new BankCompletedEvent(UUID.randomUUID(), EBankOperation.REFUND, wallet, "mock",
                                "MOCK-RF-1", 500_000, ECurrency.VND)),
                Arguments.of(WalletTopics.EVT_BANK_FAILED, "BankFailed",
                        new BankFailedEvent(UUID.randomUUID(), EBankOperation.REFUND, wallet, "vnpay",
                                null, "REFUND_UNSUPPORTED")));
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

        String charge = fixture("txn.command.bank-charge.v1.json");
        EventSchemas.assertValid(WalletTopics.CMD_BANK_CHARGE, charge);
        BankChargeCommand command = READER.read(charge, BankChargeCommand.class).payload();
        assertThat(command.amountMinor()).isEqualTo(500_000);
        assertThat(command.currency()).isEqualTo(ECurrency.VND);
        assertThat(command.clientIp())
                .as("lệnh v1 trước 2.2b không có IP: vẫn đọc được, IP là null (adapter dùng IP máy chủ)")
                .isNull();

        // Kết quả của một lệnh NẠP: ví nguồn là null trên dây (2.4a, ADR-019 §7). Fixture này khoá
        // đúng hình dạng JSON mà notification-service sẽ phải đọc được — nó PHẢI coi fromWalletId là
        // nullable ngay từ bản đầu, không được map vào một trường không-null.
        String topup = fixture("txn.event.completed.v1-topup.json");
        EventSchemas.assertValid(WalletTopics.EVT_TXN_COMPLETED, topup);
        TransactionCompletedEvent completed = READER.read(topup, TransactionCompletedEvent.class).payload();
        assertThat(completed.fromWalletId()).isNull();
        assertThat(completed.type()).isEqualTo(EJournalType.TOPUP);
        assertThat(completed.amountMinor()).isEqualTo(500_000);

        // Lệnh v1 sau 2.2b mang IP người dùng (field optional, không đổi version)
        String withIp = fixture("txn.command.bank-charge.v1-with-client-ip.json");
        EventSchemas.assertValid(WalletTopics.CMD_BANK_CHARGE, withIp);
        BankChargeCommand commandWithIp = READER.read(withIp, BankChargeCommand.class).payload();
        assertThat(commandWithIp.clientIp()).isEqualTo("203.0.113.7");
        assertThat(commandWithIp.amountMinor()).isEqualTo(500_000);
        assertThat(commandWithIp.currency()).isEqualTo(ECurrency.VND);

        // 2.4b: lệnh hoàn tiền v1
        String refund = fixture("txn.command.bank-refund.v1.json");
        EventSchemas.assertValid(WalletTopics.CMD_BANK_REFUND, refund);
        BankRefundCommand refundCommand = READER.read(refund, BankRefundCommand.class).payload();
        assertThat(refundCommand.amountMinor()).isEqualTo(500_000);
        assertThat(refundCommand.currency()).isEqualTo(ECurrency.VND);
        assertThat(refundCommand.reasonCode()).isEqualTo("ACCOUNT_CLOSED");
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

    @Test
    @DisplayName("Schema từ chối lệnh thu tiền: số tiền 0/âm, thiếu ví, loại tiền lạ")
    void schema_tu_choi_lenh_thu_tien_sai() throws IOException {
        String charge = fixture("txn.command.bank-charge.v1.json");

        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_CHARGE,
                charge.replace("\"amountMinor\": 500000", "\"amountMinor\": 0")))
                .as("thu 0 đồng là vô nghĩa").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_CHARGE,
                charge.replace("\"amountMinor\": 500000", "\"amountMinor\": -500000")))
                .as("thu số âm là chi tiền ra — sai loại thao tác").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_CHARGE,
                charge.replace("\"walletId\"", "\"walletIdX\"")))
                .as("không có ví thì thu xong không biết cộng vào đâu").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_CHARGE,
                charge.replace("\"VND\"", "\"BTC\"")))
                .as("loại tiền ngoài enum").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_CHARGE,
                charge.replace("\"currency\": \"VND\"", "\"currency\": \"VND\", \"clientIp\": \"" + "1".repeat(46) + "\"")))
                .as("clientIp dài hơn IPv6 dạng chữ (45) là rác, không phải IP").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_CHARGE,
                charge.replace("\"currency\": \"VND\"", "\"currency\": \"VND\", \"clientIp\": \"203.0.113.7\"")))
                .as("đối chứng: clientIp hợp lệ được chấp nhận").isEmpty();
    }

    @Test
    @DisplayName("Schema từ chối lệnh hoàn tiền: số tiền 0/âm, thiếu ví, thiếu lý do hoàn")
    void schema_tu_choi_lenh_hoan_tien_sai() throws IOException {
        String refund = fixture("txn.command.bank-refund.v1.json");

        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_REFUND,
                refund.replace("\"amountMinor\": 500000", "\"amountMinor\": 0")))
                .as("hoàn 0 đồng là vô nghĩa").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_REFUND,
                refund.replace("\"amountMinor\": 500000", "\"amountMinor\": -500000")))
                .as("hoàn số âm là thu thêm — sai loại thao tác").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_REFUND,
                refund.replace("\"walletId\"", "\"walletIdX\"")))
                .as("không có ví thì không khớp được với lần thu").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_REFUND,
                refund.replace("\"reasonCode\"", "\"reasonCodeX\"")))
                .as("hoàn tiền không lý do thì đối soát không truy được vì sao tiền rời đi").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.CMD_BANK_REFUND,
                refund.replace("\"BankRefund\"", "\"BankCharge\"")))
                .as("nhầm loại lệnh trên topic hoàn tiền").isNotEmpty();
    }

    @Test
    @DisplayName("Schema kết quả giao dịch: ví nguồn được phép null (nạp tiền) nhưng KHÔNG được vắng mặt")
    void schema_vi_nguon_null_duoc_vang_mat_thi_khong() throws Exception {
        UUID wallet = UUID.randomUUID();
        String topup = MAPPER.writeValueAsString(EventEnvelope.of(UUID.randomUUID(), "TransactionCompleted",
                TENANT, null, new TransactionCompletedEvent(UUID.randomUUID(), EJournalType.TOPUP,
                        UUID.randomUUID(), null, wallet, 500_000, ECurrency.VND, UUID.randomUUID()),
                Instant.now()));

        assertThat(EventSchemas.validate(WalletTopics.EVT_TXN_COMPLETED, topup))
                .as("nạp tiền không có ví nguồn — null là câu trả lời đúng, không phải thiếu dữ liệu")
                .isEmpty();
        // Nới ở 2.4a chỉ nới GIÁ TRỊ, không nới sự CÓ MẶT: một producer quên hẳn field vẫn phải bị bắt,
        // nếu không thì "null" (biết là nạp tiền) và "không gửi" (bug) trở thành một thứ.
        assertThat(EventSchemas.validate(WalletTopics.EVT_TXN_COMPLETED,
                topup.replace("\"fromWalletId\"", "\"fromWalletIdX\"")))
                .as("vắng mặt fromWalletId vẫn là message hỏng").isNotEmpty();
        assertThat(EventSchemas.validate(WalletTopics.EVT_TXN_COMPLETED,
                topup.replace("\"fromWalletId\":null", "\"fromWalletId\":\"khong-phai-uuid\"")))
                .as("có giá trị thì phải là UUID").isNotEmpty();
    }

    private static String fixture(String name) throws IOException {
        try (InputStream stream = EventContractTest.class.getResourceAsStream("/contract/fixtures/" + name)) {
            assertThat(stream).as("fixture " + name).isNotNull();
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
