package com.walletflow.bank.provider.vnpay;

import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ChargeRequest;
import com.walletflow.bank.provider.ChargeResult;
import com.walletflow.bank.provider.ProviderStatus;
import com.walletflow.bank.provider.ProviderUnavailableException;
import com.walletflow.bank.provider.RefundRequest;
import com.walletflow.bank.provider.RefundResult;
import com.walletflow.bank.provider.VerifiedWebhook;
import com.walletflow.bank.support.FakeVnPayServer;
import com.walletflow.common.money.ECurrency;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.web.client.RestClient;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phần của {@link VnPayProvider} không đi qua mạng: dựng URL thanh toán và các ranh giới của nó. Nhánh hỏi trạng
 * thái ({@code querydr}) cần một máy chủ VNPay giả thật và nằm ở {@code VnPayChargeIT}.
 *
 * <p>{@code RestClient} ở đây không bao giờ được dùng tới (tạo phiên không gọi mạng — ADR-017); nếu code đổi sang
 * gọi mạng thì test sẽ lỗi kết nối thay vì lặng lẽ qua.
 */
class VnPayProviderTest {

    private static final String SECRET = "wf-test-secret-key";
    private static final String PAY_URL = "https://pay.test/vpcpay.html";
    private static final String KEY = "123e4567-e89b-12d3-a456-426614174000";
    private static final String TXN_REF = "123e4567e89b12d3a456426614174000";
    /** 14:30:00 GMT+7 ngày 2026-10-18; phần lẻ giây phải bị cắt. */
    private static final Instant NOW = Instant.parse("2026-10-18T07:30:00.789Z");

    private final VnPaySettings settings = new VnPaySettings("WFTEST01", SECRET, PAY_URL,
            "https://api.test/merchant_webapi/api/transaction", "https://example.test/return?x=1", "10.0.0.1",
            "vn", "other");
    private final VnPayProvider provider = new VnPayProvider(settings, RestClient.create(),
            Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    @DisplayName("⭐ URL thanh toán khớp đáp án tính độc lập bằng openssl: tham số sắp theo tên, mã hoá US-ASCII, ký HMAC-SHA512 trên chuỗi query")
    void url_thanh_toan_khop_dap_an_openssl() {
        ChargeResult.Created created = created(request(KEY, 150_000, "203.0.113.7"));

        // Chuỗi dưới đây viết tay theo quy tắc của VNPay; hash: printf '%s' '<chuỗi>' | openssl dgst -sha512 -hmac '<secret>'
        String expected = PAY_URL + "?vnp_Amount=15000000&vnp_Command=pay&vnp_CreateDate=20261018143000"
                + "&vnp_CurrCode=VND&vnp_ExpireDate=20261018144500&vnp_IpAddr=203.0.113.7&vnp_Locale=vn"
                + "&vnp_OrderInfo=Nap+tien+vi+" + TXN_REF + "&vnp_OrderType=other"
                + "&vnp_ReturnUrl=https%3A%2F%2Fexample.test%2Freturn%3Fx%3D1&vnp_TmnCode=WFTEST01"
                + "&vnp_TxnRef=" + TXN_REF + "&vnp_Version=2.1.0"
                + "&vnp_SecureHash=b535c8c696b2fdd1df6ba6ec49ac4f4d6e5e668a97f58f3397ca58878b65f8c9"
                + "80d1ee4aa4c27b9801fdb075f2f1f9b0cd868673ee4edcef6bad13e646b3299c";
        assertThat(created.paymentUrl()).isEqualTo(expected);
    }

    @Test
    @DisplayName("Created mang mã tham chiếu = khoá idempotency bỏ dấu gạch, mốc tạo cắt về giây, hạn = mốc tạo + ttl; "
            + "bằng chứng đối soát không chứa IP người dùng lẫn chữ ký")
    void created_mang_ma_tham_chieu_moc_tao_va_han() {
        ChargeResult.Created created = created(request(KEY, 150_000, "203.0.113.7"));

        assertThat(created.providerRef()).isEqualTo(TXN_REF);
        assertThat(created.sessionCreatedAt()).isEqualTo(Instant.parse("2026-10-18T07:30:00Z"));
        assertThat(created.expiresAt()).isEqualTo(Instant.parse("2026-10-18T07:45:00Z"));
        assertThat(created.rawResponse())
                .contains(TXN_REF, "15000000", "20261018143000", "20261018144500")
                .doesNotContain("203.0.113.7")
                .doesNotContainIgnoringCase("SecureHash");
    }

    @Test
    @DisplayName("Gửi lại cùng khoá idempotency: cùng mã tham chiếu và cùng mốc (VNPay không bao giờ thấy hai giao dịch cho một lần thu)")
    void gui_lai_cung_khoa_ra_cung_ma_tham_chieu() {
        ChargeResult.Created first = created(request(KEY, 150_000, "203.0.113.7"));
        ChargeResult.Created again = created(request(KEY, 150_000, "198.51.100.9"));

        assertThat(again.providerRef()).isEqualTo(first.providerRef());
        assertThat(again.sessionCreatedAt()).isEqualTo(first.sessionCreatedAt());
        assertThat(query(again.paymentUrl()).get("vnp_IpAddr"))
                .as("IP lấy từ lệnh đang xử lý, không thuộc danh tính của lần thu")
                .isEqualTo("198.51.100.9");
    }

    @ParameterizedTest(name = "khoá \"{0}\" -> mã tham chiếu \"{1}\"")
    @CsvSource({
            "123e4567-e89b-12d3-a456-426614174000, 123e4567e89b12d3a456426614174000",
            "a-b_c.d e!f, abcdef",
            "ABC123, ABC123"
    })
    @DisplayName("vnp_TxnRef chỉ giữ chữ và số (VNPay: Alphanumeric)")
    void ma_tham_chieu_chi_giu_chu_va_so(String idempotencyKey, String expected) {
        assertThat(VnPayProvider.txnRef(idempotencyKey)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "clientIp có {0} ký tự -> {1}")
    @CsvSource({
            "0, 10.0.0.1",
            "6, 10.0.0.1",
            "7, CLIENT",
            "15, CLIENT",
            "45, CLIENT",
            "46, 10.0.0.1"
    })
    @DisplayName("vnp_IpAddr: IP người dùng khi dài 7..45 ký tự (khuôn của VNPay), ngoài khoảng đó thì IP máy chủ")
    void ip_nguoi_dung_chi_dung_khi_vua_khuon(int length, String expected) {
        String clientIp = "9".repeat(length);

        String used = query(created(request(KEY, 1_000, clientIp)).paymentUrl()).get("vnp_IpAddr");

        assertThat(used).isEqualTo(expected.equals("CLIENT") ? clientIp : expected);
    }

    @Test
    @DisplayName("Không có clientIp (null): dùng IP máy chủ")
    void khong_co_client_ip_dung_ip_may_chu() {
        assertThat(query(created(request(KEY, 1_000, null)).paymentUrl()).get("vnp_IpAddr")).isEqualTo("10.0.0.1");
    }

    @Test
    @DisplayName("USD: từ chối nghiệp vụ CURRENCY_NOT_SUPPORTED, không dựng URL")
    void usd_bi_tu_choi() {
        ChargeResult result = provider.charge(new ChargeRequest(KEY, 1_000, ECurrency.USD, null, Duration.ofMinutes(15)));

        assertThat(result).isInstanceOfSatisfying(ChargeResult.Declined.class, declined -> {
            assertThat(declined.failureCode()).isEqualTo("CURRENCY_NOT_SUPPORTED");
            assertThat(declined.rawResponse()).contains("USD");
        });
    }

    @Test
    @DisplayName("Ranh giới số tiền: đúng trần 9.999.999.999 thì tạo phiên (vnp_Amount 999999999900), vượt 1 đơn vị thì AMOUNT_LIMIT_EXCEEDED")
    void ranh_gioi_tran_so_tien() {
        ChargeResult.Created atLimit = created(request(KEY, 9_999_999_999L, null));
        assertThat(query(atLimit.paymentUrl()).get("vnp_Amount")).isEqualTo("999999999900");

        ChargeResult overLimit = provider.charge(request(KEY, 10_000_000_000L, null));
        assertThat(overLimit).isInstanceOfSatisfying(ChargeResult.Declined.class,
                declined -> assertThat(declined.failureCode()).isEqualTo("AMOUNT_LIMIT_EXCEEDED"));
    }

    @Test
    @DisplayName("vnp_Amount = số tiền × 100 (VNPay quy định), vnp_Amount của 1 đồng là 100")
    void so_tien_nhan_100() {
        assertThat(query(created(request(KEY, 1, null)).paymentUrl()).get("vnp_Amount")).isEqualTo("100");
        assertThat(query(created(request(KEY, 123_456, null)).paymentUrl()).get("vnp_Amount")).isEqualTo("12345600");
    }

    @Test
    @DisplayName("Hạn phiên theo sessionTtl của yêu cầu, không phải hằng số: ttl 30 phút -> vnp_ExpireDate = tạo + 30 phút")
    void han_phien_theo_ttl_cua_yeu_cau() {
        ChargeResult.Created created = created(new ChargeRequest(KEY, 1_000, ECurrency.VND, null, Duration.ofMinutes(30)));

        assertThat(query(created.paymentUrl()))
                .containsEntry("vnp_CreateDate", "20261018143000")
                .containsEntry("vnp_ExpireDate", "20261018150000");
        assertThat(created.expiresAt()).isEqualTo(Instant.parse("2026-10-18T08:00:00Z"));
    }

    @Test
    @DisplayName("Hỏi trạng thái khi không có ngày tạo phiên: ProviderUnavailableException, không gọi mạng (không được hiểu 'không tìm thấy' thành 'chưa thu')")
    void hoi_trang_thai_thieu_ngay_tao_la_khong_biet() {
        assertThatThrownBy(() -> provider.getStatus(TXN_REF, null))
                .isInstanceOf(ProviderUnavailableException.class)
                .hasMessageContaining(TXN_REF);
    }

    @Test
    @DisplayName("Hoàn tiền chưa được hỗ trợ: từ chối dứt khoát REFUND_UNSUPPORTED, không bao giờ 'đã hoàn', không gọi mạng")
    void hoan_tien_chua_ho_tro() {
        // provider dựng với RestClient không trỏ tới máy nào: gọi mạng thì nổ, nên trả về được tức là không gọi
        RefundResult result = provider.refund(new RefundRequest(KEY, TXN_REF, 1_000, ECurrency.VND));

        assertThat(result)
                .as("từ chối (2.4b) để saga chuyển người hoàn tay — ném lỗi là thử lại vô ích rồi DLT và mở breaker")
                .isInstanceOfSatisfying(RefundResult.Declined.class,
                        declined -> assertThat(declined.failureCode()).isEqualTo(VnPayProvider.REFUND_UNSUPPORTED));
    }

    @Test
    @DisplayName("Tên provider là 'vnpay'; toString của cấu hình không bao giờ in hash secret")
    void ten_provider_va_cau_hinh_khong_lo_secret() {
        assertThat(provider.name()).isEqualTo("vnpay");
        assertThat(settings.toString()).doesNotContain(SECRET).contains("hashSecret=***").contains("WFTEST01");
    }

    @Test
    @DisplayName("account() là vnp_TmnCode (phiên chỉ tra được dưới tài khoản đó); tạo URL không qua mạng nên chargeUsesNetwork() = false")
    void tai_khoan_la_tmn_code_va_tao_url_khong_qua_mang() {
        assertThat(provider.account()).isEqualTo("WFTEST01");
        assertThat(provider.chargeUsesNetwork()).isFalse();
    }

    @ParameterizedTest(name = "vnp_TransactionType = {0}")
    @NullSource
    @ValueSource(strings = {"02", "03", "", "04"})
    @DisplayName("⭐ querydr 00/00 mà loại giao dịch KHÔNG phải 01 (02/03 hoàn tiền, thiếu, lạ): UNRESOLVED, không bao giờ SUCCEEDED")
    void giao_dich_00_khong_phai_loai_thanh_toan_la_unresolved(String transactionType) {
        ProviderStatus status = ask("00", transactionType);

        assertThat(status.state()).isEqualTo(EProviderState.UNRESOLVED);
        assertThat(status.capturedAmountMinor()).as("không có số tiền thu nào được công nhận").isZero();
    }

    @Test
    @DisplayName("querydr 00/00 loại 01: SUCCEEDED với số tiền = vnp_Amount / 100")
    void giao_dich_00_loai_thanh_toan_la_succeeded() {
        ProviderStatus status = ask("00", "01");

        assertThat(status.state()).isEqualTo(EProviderState.SUCCEEDED);
        assertThat(status.capturedAmountMinor()).isEqualTo(150_000);
        assertThat(status.capturedCurrency()).isEqualTo(ECurrency.VND);
    }

    @ParameterizedTest(name = "vnp_TransactionStatus = \"{0}\"")
    @CsvSource({"01, PENDING", "02, PENDING", "04, UNRESOLVED", "07, UNRESOLVED", "99, UNRESOLVED"})
    @DisplayName("Ánh xạ trạng thái giao dịch (loại 01): 01/02 chưa thu; còn lại lơ lửng")
    void anh_xa_trang_thai_giao_dich(String transactionStatus, EProviderState expected) {
        assertThat(ask(transactionStatus, "01").state()).isEqualTo(expected);
    }

    private ProviderStatus ask(String transactionStatus, String transactionType) {
        try (FakeVnPayServer fake = new FakeVnPayServer("WFTEST01", SECRET)) {
            VnPaySettings fakeSettings = new VnPaySettings("WFTEST01", SECRET, fake.payUrl(), fake.apiUrl(),
                    "https://example.test/return", "10.0.0.1", "vn", "other");
            VnPayProvider against = new VnPayProvider(fakeSettings, RestClient.create(), Clock.fixed(NOW, ZoneOffset.UTC));
            ChargeResult.Created session = (ChargeResult.Created) against.charge(request(KEY, 150_000, "203.0.113.7"));
            String txnRef = fake.open(session.paymentUrl()).get("vnp_TxnRef");
            fake.setStatus(txnRef, transactionStatus);
            fake.setTransactionType(txnRef, transactionType);
            return against.getStatus(session.providerRef(), session.sessionCreatedAt());
        }
    }

    // ------------------------------------------------------------------
    // verifyWebhook — IPN (2.3). Chữ ký tính ĐỘC LẬP, không gọi VnPaySigner.
    // ------------------------------------------------------------------

    @Test
    @DisplayName("⭐ IPN ký đúng: chấp nhận, trả vnp_TxnRef và đúng chuỗi đã ký (không gồm vnp_SecureHash)")
    void ipn_ky_dung_duoc_chap_nhan() {
        Map<String, String> ipn = ipnParams();
        String data = ipnSignedData();
        ipn.put("vnp_SecureHash", independentHmac(SECRET, data));

        Optional<VerifiedWebhook> verified = provider.verifyWebhook(ipn);

        assertThat(verified).isPresent();
        assertThat(verified.get().providerRef()).isEqualTo(TXN_REF);
        assertThat(verified.get().signedData()).isEqualTo(data);
    }

    @Test
    @DisplayName("⭐ IPN bị sửa nội dung sau khi ký: từ chối — nếu không, ai cũng tự tuyên bố đã trả tiền")
    void ipn_bi_sua_sau_khi_ky_bi_tu_choi() {
        Map<String, String> ipn = ipnParams();
        ipn.put("vnp_SecureHash", independentHmac(SECRET, ipnSignedData()));
        ipn.put("vnp_Amount", "99900000");

        assertThat(provider.verifyWebhook(ipn)).isEmpty();
    }

    @Test
    @DisplayName("vnp_SecureHashType KHÔNG nằm trong dữ liệu ký: gửi kèm vẫn verify được")
    void ipn_co_secure_hash_type_van_verify_duoc() {
        Map<String, String> ipn = ipnParams();
        ipn.put("vnp_SecureHash", independentHmac(SECRET, ipnSignedData()));
        ipn.put("vnp_SecureHashType", "HmacSHA512");

        assertThat(provider.verifyWebhook(ipn)).isPresent();
    }

    @Test
    @DisplayName("Tham số có giá trị RỖNG bị bỏ khỏi dữ liệu ký (quy tắc của VNPay)")
    void ipn_tham_so_rong_khong_nam_trong_du_lieu_ky() {
        Map<String, String> ipn = ipnParams();
        // Chữ ký tính trên chuỗi KHÔNG có vnp_PromotionCode; gửi kèm nó rỗng vẫn phải khớp
        ipn.put("vnp_PromotionCode", "");
        ipn.put("vnp_SecureHash", independentHmac(SECRET, ipnSignedData()));

        assertThat(provider.verifyWebhook(ipn)).isPresent();
    }

    @ParameterizedTest(name = "chữ ký [{0}]")
    @NullSource
    @ValueSource(strings = {"", "deadbeef", "KHONGPHAIHEX"})
    @DisplayName("Chữ ký thiếu, rỗng hoặc sai: từ chối, không có ngoại lệ nào")
    void ipn_chu_ky_hong_bi_tu_choi(String signature) {
        Map<String, String> ipn = ipnParams();
        if (signature != null) {
            ipn.put("vnp_SecureHash", signature);
        }

        assertThat(provider.verifyWebhook(ipn)).isEmpty();
    }

    @Test
    @DisplayName("⭐ IPN ký đúng nhưng mang mã merchant KHÁC: từ chối — cùng chuẩn với querydr")
    void ipn_ma_merchant_khac_bi_tu_choi() {
        Map<String, String> ipn = ipnParams();
        ipn.put("vnp_TmnCode", "OTHER001");
        String data = ipnSignedData().replace("vnp_TmnCode=WFTEST01", "vnp_TmnCode=OTHER001");
        ipn.put("vnp_SecureHash", independentHmac(SECRET, data));

        assertThat(provider.verifyWebhook(ipn)).isEmpty();
    }

    @ParameterizedTest(name = "vnp_TxnRef = [{0}]")
    @ValueSource(strings = {"co ky tu la!", "qua-dai-vi-co-gach", "ref@voi#ky$tu%la"})
    @DisplayName("vnp_TxnRef không đúng khuôn Alphanumeric: vẫn hợp lệ chữ ký nhưng providerRef là null "
            + "(mã dài hơn cột provider_ref sẽ thành lỗi ghi DB → 500)")
    void ipn_txn_ref_sai_khuon_thi_khong_lay_lam_ref(String txnRef) {
        Optional<VerifiedWebhook> verified = provider.verifyWebhook(signedIpnWithTxnRef(txnRef));

        assertThat(verified).isPresent();
        assertThat(verified.get().providerRef()).isNull();
    }

    /** IPN tối giản mang {@code txnRef} đã cho, ký đúng — để soi riêng khuôn của mã tham chiếu. */
    private static Map<String, String> signedIpnWithTxnRef(String txnRef) {
        SortedMap<String, String> sorted = new TreeMap<>();
        sorted.put("vnp_TmnCode", "WFTEST01");
        sorted.put("vnp_TxnRef", txnRef);
        String data = sorted.entrySet().stream()
                .map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.US_ASCII)
                        + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.US_ASCII))
                .collect(Collectors.joining("&"));
        Map<String, String> ipn = new LinkedHashMap<>(sorted);
        ipn.put("vnp_SecureHash", independentHmac(SECRET, data));
        return ipn;
    }

    @Test
    @DisplayName("vnp_TxnRef dài 101 ký tự (quá khuôn VNPay và quá cột provider_ref): providerRef là null")
    void ipn_txn_ref_qua_dai_thi_khong_lay_lam_ref() {
        Optional<VerifiedWebhook> verified = provider.verifyWebhook(signedIpnWithTxnRef("x".repeat(101)));

        assertThat(verified).isPresent();
        assertThat(verified.get().providerRef()).isNull();
    }

    @Test
    @DisplayName("IPN chỉ có mỗi chữ ký (không tham số nào khác): từ chối, không bao giờ coi là hợp lệ")
    void ipn_rong_bi_tu_choi() {
        assertThat(provider.verifyWebhook(Map.of("vnp_SecureHash", independentHmac(SECRET, "")))).isEmpty();
        assertThat(provider.verifyWebhook(Map.of())).isEmpty();
    }

    @Test
    @DisplayName("IPN ký đúng nhưng KHÔNG mang vnp_TxnRef: vẫn hợp lệ nhưng providerRef là null (tầng trên tự xử)")
    void ipn_thieu_txn_ref_van_hop_le_nhung_khong_co_ref() {
        Map<String, String> ipn = new LinkedHashMap<>();
        ipn.put("vnp_ResponseCode", "00");
        ipn.put("vnp_TmnCode", "WFTEST01");
        ipn.put("vnp_SecureHash", independentHmac(SECRET, "vnp_ResponseCode=00&vnp_TmnCode=WFTEST01"));

        Optional<VerifiedWebhook> verified = provider.verifyWebhook(ipn);

        assertThat(verified).isPresent();
        assertThat(verified.get().providerRef()).isNull();
    }

    /**
     * IPN giống thật: có {@code vnp_OrderInfo} mang KHOẢNG TRẮNG (phải thành {@code +} trong chuỗi ký) và
     * {@code vnp_PayDate}. Fixture toàn chữ-số sẽ không chạm tới quy tắc encode — đúng chỗ rủi ro nhất.
     */
    private static Map<String, String> ipnParams() {
        Map<String, String> ipn = new LinkedHashMap<>();
        ipn.put("vnp_TxnRef", TXN_REF);
        ipn.put("vnp_Amount", "15000000");
        ipn.put("vnp_ResponseCode", "00");
        ipn.put("vnp_TmnCode", "WFTEST01");
        ipn.put("vnp_OrderInfo", "Nap tien vi " + TXN_REF);
        ipn.put("vnp_BankCode", "NCB");
        ipn.put("vnp_PayDate", "20261018143500");
        return ipn;
    }

    /** Chuỗi ký của {@link #ipnParams()}: sắp theo tên, encode US-ASCII (dấu cách thành {@code +}). */
    private static String ipnSignedData() {
        return "vnp_Amount=15000000&vnp_BankCode=NCB&vnp_OrderInfo=Nap+tien+vi+" + TXN_REF
                + "&vnp_PayDate=20261018143500&vnp_ResponseCode=00&vnp_TmnCode=WFTEST01&vnp_TxnRef=" + TXN_REF;
    }

    /** HMAC-SHA512 viết lại trong test — một lỗi ở VnPaySigner không được tự xác nhận chính nó. */
    private static String independentHmac(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA512"));
            return HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private ChargeResult.Created created(ChargeRequest request) {
        ChargeResult result = provider.charge(request);
        assertThat(result).isInstanceOf(ChargeResult.Created.class);
        return (ChargeResult.Created) result;
    }

    private static ChargeRequest request(String key, long amountMinor, String clientIp) {
        return new ChargeRequest(key, amountMinor, ECurrency.VND, clientIp, Duration.ofMinutes(15));
    }

    /** Query string của URL thanh toán đã giải mã (không gồm chữ ký). */
    private static Map<String, String> query(String url) {
        Map<String, String> params = new LinkedHashMap<>();
        for (String pair : url.substring(url.indexOf('?') + 1).split("&")) {
            int eq = pair.indexOf('=');
            params.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.US_ASCII),
                    URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.US_ASCII));
        }
        return params;
    }
}
