package com.walletflow.bank.provider.vnpay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ChargeRequest;
import com.walletflow.bank.provider.ChargeResult;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.ProviderStatus;
import com.walletflow.bank.provider.ProviderUnavailableException;
import com.walletflow.bank.provider.RefundRequest;
import com.walletflow.bank.provider.RefundResult;
import com.walletflow.bank.provider.VerifiedWebhook;
import com.walletflow.common.money.ECurrency;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Cổng VNPay, API 2.1.0, luồng chuyển hướng (ADR-016 §7, ADR-017).
 *
 * <p><b>Tạo phiên không gọi mạng.</b> URL thanh toán được dựng và KÝ tại chỗ; VNPay chỉ biết tới giao dịch khi
 * người dùng mở URL. Hệ quả:
 * <ul>
 *   <li>{@code vnp_TxnRef} lấy tất định từ idempotency key — gửi lại lệnh thu ra cùng mã tham chiếu, nên ở
 *       phía VNPay không bao giờ có hai giao dịch cho một lần thu.</li>
 *   <li>"Không tìm thấy giao dịch" (mã 91) là câu trả lời BÌNH THƯỜNG cho một phiên chưa ai mở — nghĩa là
 *       chưa thu đồng nào, không phải lỗi.</li>
 * </ul>
 *
 * <p><b>Hỏi trạng thái ({@code querydr}) — ánh xạ thận trọng:</b>
 * <table>
 *   <tr><th>VNPay</th><th>Ở đây</th><th>Vì sao</th></tr>
 *   <tr><td>00 / giao dịch 00, loại 01 (thanh toán)</td><td>SUCCEEDED</td><td>đã thu; số tiền đối chiếu ở
 *       ChargeStatusService</td></tr>
 *   <tr><td>00 / giao dịch 00, loại khác (02/03 hoàn tiền)</td><td>UNRESOLVED</td><td>"thành công" của một
 *       lần HOÀN — tiền đã quay về người dùng, cộng ví là tạo tiền</td></tr>
 *   <tr><td>00 / giao dịch 01, 02</td><td>PENDING (chưa thu)</td><td>chưa trả xong / lần trả hỏng — phiên còn
 *       hạn thì người dùng vẫn có thể trả lại, nên KHÔNG kết luận thất bại sớm; hết hạn + grace mới kết
 *       thúc EXPIRED</td></tr>
 *   <tr><td>00 / giao dịch khác (04 đảo, 07 nghi gian lận, 05/06/09 hoàn...)</td><td>UNRESOLVED</td>
 *       <td>tiền có thể đã rời tài khoản người dùng — không bao giờ tự hết hạn, cần người</td></tr>
 *   <tr><td>91</td><td>PENDING (chưa thu)</td><td>chưa ai mở URL</td></tr>
 *   <tr><td>mã khác, sai chữ ký, sai mã merchant/tham chiếu, không đọc được</td>
 *       <td>{@link ProviderUnavailableException}</td><td>"không biết" — không bao giờ thành thất bại</td></tr>
 * </table>
 *
 * <p><b>Mọi câu trả lời được kiểm chữ ký TRƯỚC khi đọc nghĩa.</b> Câu trả lời giả mạo (DNS bị đầu độc, proxy
 * độc hại) nói "đã thu" là cách tạo tiền từ không khí.
 */
public class VnPayProvider implements PaymentProvider {

    public static final String NAME = "vnpay";

    static final String VERSION = "2.1.0";
    /** Trần của {@code vnp_Amount} (Numeric[1,12]) chia 100: số tiền lớn hơn VNPay không nhận. */
    static final long MAX_AMOUNT_MINOR = 9_999_999_999L;
    /** {@code vnp_TransactionType} của giao dịch thanh toán (02/03 là hoàn tiền toàn phần/một phần). */
    static final String PAYMENT_TRANSACTION_TYPE = "01";
    /** Khuôn {@code vnp_TxnRef} theo tài liệu VNPay: Alphanumeric, tối đa 100 ký tự. */
    private static final Pattern VALID_TXN_REF = Pattern.compile("[A-Za-z0-9]{1,100}");

    /** Thứ tự ký của request querydr — theo tài liệu VNPay, không được đổi. */
    private static final String[] QUERY_SIGNED_FIELDS = {"vnp_RequestId", "vnp_Version", "vnp_Command",
            "vnp_TmnCode", "vnp_TxnRef", "vnp_TransactionDate", "vnp_CreateDate", "vnp_IpAddr", "vnp_OrderInfo"};
    /** Thứ tự ký của câu trả lời querydr — theo tài liệu VNPay, không được đổi. */
    private static final String[] RESPONSE_SIGNED_FIELDS = {"vnp_ResponseId", "vnp_Command", "vnp_ResponseCode",
            "vnp_Message", "vnp_TmnCode", "vnp_TxnRef", "vnp_Amount", "vnp_BankCode", "vnp_PayDate",
            "vnp_TransactionNo", "vnp_TransactionType", "vnp_TransactionStatus", "vnp_OrderInfo",
            "vnp_PromotionCode", "vnp_PromotionAmount"};

    private final VnPaySettings settings;
    private final RestClient http;
    private final Clock clock;
    private final ObjectMapper json = JsonMapper.builder().build();

    /**
     * @param http client đã cấu hình connect/read timeout NGẮN hơn {@code resilience.timeout} (xem
     *             {@link PaymentProvider})
     */
    public VnPayProvider(VnPaySettings settings, RestClient http, Clock clock) {
        this.settings = settings;
        this.http = http;
        this.clock = clock;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String account() {
        return settings.tmnCode();
    }

    /** URL ký tại chỗ — không có gì để circuit breaker bảo vệ. */
    @Override
    public boolean chargeUsesNetwork() {
        return false;
    }

    @Override
    public ChargeResult charge(ChargeRequest request) {
        if (request.currency() != ECurrency.VND) {
            return new ChargeResult.Declined("CURRENCY_NOT_SUPPORTED",
                    "{\"reason\":\"VNPay chỉ nhận VND\",\"currency\":\"" + request.currency() + "\"}");
        }
        if (request.amountMinor() > MAX_AMOUNT_MINOR) {
            return new ChargeResult.Declined("AMOUNT_LIMIT_EXCEEDED",
                    "{\"reason\":\"vượt trần vnp_Amount\",\"maxAmountMinor\":" + MAX_AMOUNT_MINOR + "}");
        }

        String txnRef = txnRef(request.idempotencyKey());
        // VNPay chỉ có độ phân giải giây: lưu đúng mốc đã ký, để querydr gửi lại khớp từng giây
        Instant createdAt = clock.instant().truncatedTo(ChronoUnit.SECONDS);
        Instant expiresAt = createdAt.plus(request.sessionTtl()).truncatedTo(ChronoUnit.SECONDS);
        String amount = Long.toString(Math.multiplyExact(request.amountMinor(), 100L));

        TreeMap<String, String> params = new TreeMap<>();
        params.put("vnp_Version", VERSION);
        params.put("vnp_Command", "pay");
        params.put("vnp_TmnCode", settings.tmnCode());
        params.put("vnp_Amount", amount);
        params.put("vnp_CurrCode", "VND");
        params.put("vnp_TxnRef", txnRef);
        // Không dấu, không ký tự đặc biệt (yêu cầu của VNPay); mã tham chiếu đủ để đối chiếu
        params.put("vnp_OrderInfo", "Nap tien vi " + txnRef);
        params.put("vnp_OrderType", settings.orderType());
        params.put("vnp_Locale", settings.locale());
        params.put("vnp_ReturnUrl", settings.returnUrl());
        params.put("vnp_IpAddr", ipOrServer(request.clientIp()));
        params.put("vnp_CreateDate", VnPaySigner.format(createdAt));
        params.put("vnp_ExpireDate", VnPaySigner.format(expiresAt));

        String paymentUrl = settings.payUrl() + "?" + VnPaySigner.signedQuery(params, settings.hashSecret());
        // Bằng chứng đối soát: thứ đã gửi đi, KHÔNG có IP người dùng (dữ liệu cá nhân) và không có chữ ký
        String raw = "{\"vnp_TxnRef\":\"" + txnRef + "\",\"vnp_Amount\":\"" + amount + "\",\"vnp_CreateDate\":\""
                + params.get("vnp_CreateDate") + "\",\"vnp_ExpireDate\":\"" + params.get("vnp_ExpireDate") + "\"}";
        return new ChargeResult.Created(txnRef, paymentUrl, raw, createdAt, expiresAt);
    }

    @Override
    public ProviderStatus getStatus(String providerRef, Instant sessionCreatedAt) {
        if (sessionCreatedAt == null) {
            // Không có ngày tạo thì VNPay trả "không tìm thấy" — hiểu nhầm thành "chưa thu" là nguy hiểm
            throw new ProviderUnavailableException("VNPay: thiếu ngày tạo phiên của " + providerRef
                    + " — không hỏi được trạng thái");
        }
        Map<String, String> query = new LinkedHashMap<>();
        query.put("vnp_RequestId", UUID.randomUUID().toString().replace("-", ""));
        query.put("vnp_Version", VERSION);
        query.put("vnp_Command", "querydr");
        query.put("vnp_TmnCode", settings.tmnCode());
        query.put("vnp_TxnRef", providerRef);
        query.put("vnp_OrderInfo", "Truy van giao dich " + providerRef);
        query.put("vnp_TransactionDate", VnPaySigner.format(sessionCreatedAt));
        query.put("vnp_CreateDate", VnPaySigner.format(clock.instant()));
        query.put("vnp_IpAddr", settings.serverIp());
        query.put("vnp_SecureHash", VnPaySigner.hmacSha512(settings.hashSecret(),
                VnPaySigner.pipeJoined(query, QUERY_SIGNED_FIELDS)));

        String raw = post(query);
        Map<String, String> answer = parse(raw);
        verify(answer, providerRef);
        return interpret(answer, raw);
    }

    /** Hoàn tiền thuộc saga top-up (2.4) — chưa có luồng nào gọi tới. */
    @Override
    public RefundResult refund(RefundRequest request) {
        throw new UnsupportedOperationException("VNPay refund chưa được hỗ trợ (roadmap 2.4)");
    }

    /**
     * IPN của VNPay được ký trên CHÍNH chuỗi query nó gửi, nên kiểm bằng cách dựng lại chuỗi đó rồi so
     * HMAC — cùng một hàm dựng với chiều gửi ({@link VnPaySigner#ipnSignedData}).
     *
     * <p>Fail-closed: thiếu {@code vnp_SecureHash}, sai chữ ký, hoặc payload rỗng đều là không hợp lệ.
     * {@code vnp_SecureHash} và {@code vnp_SecureHashType} bị loại khỏi dữ liệu ký — chúng không được ký.
     */
    @Override
    public Optional<VerifiedWebhook> verifyWebhook(Map<String, String> params) {
        String received = params.get("vnp_SecureHash");
        if (received == null || received.isEmpty() || params.isEmpty()) {
            return Optional.empty();
        }
        SortedMap<String, String> signed = new TreeMap<>(params);
        signed.remove("vnp_SecureHash");
        signed.remove("vnp_SecureHashType");
        if (signed.isEmpty()) {
            return Optional.empty();
        }
        String data = VnPaySigner.ipnSignedData(signed);
        if (!VnPaySigner.signatureMatches(VnPaySigner.hmacSha512(settings.hashSecret(), data), received)) {
            return Optional.empty();
        }
        // Cùng chuẩn với getStatus: câu trả lời mang mã merchant khác không nói gì về phiên của ta
        if (!settings.tmnCode().equals(signed.get("vnp_TmnCode"))) {
            return Optional.empty();
        }
        // Mã tham chiếu chỉ ĐỌC ra ở đây; việc nó có khớp lần thu nào không là của tầng trên. Chỉ nhận mã
        // đúng khuôn VNPay (Alphanumeric): mã dài hơn cột provider_ref sẽ thành lỗi ghi DB → 500, và một
        // mã chứa ký tự lạ thì chắc chắn không tra được lần thu nào.
        String txnRef = signed.get("vnp_TxnRef");
        return Optional.of(new VerifiedWebhook(VALID_TXN_REF.matcher(txnRef == null ? "" : txnRef).matches()
                ? txnRef : null, data));
    }

    private String post(Map<String, String> body) {
        try {
            String raw = http.post()
                    .uri(settings.apiUrl())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            if (raw == null || raw.isBlank()) {
                throw new ProviderUnavailableException("VNPay querydr: câu trả lời rỗng");
            }
            return raw;
        } catch (RestClientException failure) {
            // Không đưa message gốc vào: nó có thể chứa nguyên body. Lớp lỗi là đủ để chẩn đoán.
            throw new ProviderUnavailableException("VNPay querydr: lỗi kết nối/HTTP ("
                    + failure.getClass().getSimpleName() + ")", failure);
        }
    }

    private Map<String, String> parse(String raw) {
        try {
            JsonNode root = json.readTree(raw);
            if (root == null || !root.isObject()) {
                throw new ProviderUnavailableException("VNPay querydr: câu trả lời không phải JSON object");
            }
            Map<String, String> fields = new HashMap<>();
            for (Iterator<Map.Entry<String, JsonNode>> it = root.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> field = it.next();
                fields.put(field.getKey(), field.getValue().isNull() ? null : field.getValue().asText());
            }
            return fields;
        } catch (JsonProcessingException unreadable) {
            throw new ProviderUnavailableException("VNPay querydr: câu trả lời không đọc được", unreadable);
        }
    }

    /**
     * Chữ ký, lệnh, mã merchant, mã tham chiếu — sai bất kỳ cái nào thì câu trả lời không phải của VNPay về
     * phiên này, và "không phải về phiên này" luôn có nghĩa là KHÔNG BIẾT, không bao giờ là một kết luận.
     *
     * <p>Mọi điều kiện ở đây fail-closed: thiếu field cũng là sai. Một câu trả lời không chứng minh được nó
     * nói về {@code providerRef} thì không được dùng cho việc gì — kể cả cho nhánh tưởng như vô hại
     * {@code vnp_ResponseCode=91} ("không tìm thấy giao dịch"), vì 91 là căn cứ DUY NHẤT để
     * {@code markExpired} kết thúc một lần thu. Nhận một câu 91 không gắn với giao dịch nào tức là cho phép
     * phát lại nó để đánh EXPIRED một lần thu người dùng ĐÃ TRẢ TIỀN — tiền nằm ở tài khoản merchant mà ví
     * không được cộng.
     */
    private void verify(Map<String, String> answer, String providerRef) {
        String expected = VnPaySigner.hmacSha512(settings.hashSecret(),
                VnPaySigner.pipeJoined(answer, RESPONSE_SIGNED_FIELDS));
        if (!VnPaySigner.signatureMatches(expected, answer.get("vnp_SecureHash"))) {
            throw new ProviderUnavailableException("VNPay querydr: chữ ký câu trả lời KHÔNG hợp lệ cho " + providerRef
                    + " — bỏ qua câu trả lời");
        }
        // Câu trả lời của lệnh khác (refund chẳng hạn) mang nghĩa hoàn toàn khác dù ký đúng bằng cùng khoá
        if (!"querydr".equals(answer.get("vnp_Command"))) {
            throw new ProviderUnavailableException("VNPay querydr: câu trả lời của lệnh "
                    + answer.get("vnp_Command") + ", không phải querydr");
        }
        if (!settings.tmnCode().equals(answer.get("vnp_TmnCode"))) {
            throw new ProviderUnavailableException("VNPay querydr: câu trả lời mang mã merchant khác");
        }
        if (!providerRef.equals(answer.get("vnp_TxnRef"))) {
            throw new ProviderUnavailableException("VNPay querydr: câu trả lời nói về giao dịch "
                    + answer.get("vnp_TxnRef") + ", không phải " + providerRef);
        }
    }

    private ProviderStatus interpret(Map<String, String> answer, String raw) {
        String responseCode = answer.get("vnp_ResponseCode");
        if ("91".equals(responseCode)) {
            return new ProviderStatus(EProviderState.PENDING, null, 0, ECurrency.VND, raw);
        }
        if (!"00".equals(responseCode)) {
            throw new ProviderUnavailableException("VNPay querydr trả mã " + responseCode
                    + " — không kết luận được trạng thái");
        }
        // vnp_TxnRef không cần kiểm lại ở đây: verify() đã bắt buộc nó khớp providerRef cho MỌI câu trả lời
        String status = answer.get("vnp_TransactionStatus");
        return switch (status == null ? "" : status) {
            // "Thành công" chỉ khi đó là giao dịch THANH TOÁN (loại 01). Loại 02/03 là hoàn toàn phần/một phần:
            // VNPay báo "thành công" cho chính lần hoàn, với số tiền gốc — cộng ví theo nó là cộng tiền đã trả lại
            case "00" -> PAYMENT_TRANSACTION_TYPE.equals(answer.get("vnp_TransactionType"))
                    ? new ProviderStatus(EProviderState.SUCCEEDED, null, capturedMinor(answer), ECurrency.VND, raw)
                    : new ProviderStatus(EProviderState.UNRESOLVED, null, 0, ECurrency.VND, raw);
            case "01", "02" -> new ProviderStatus(EProviderState.PENDING, null, 0, ECurrency.VND, raw);
            default -> new ProviderStatus(EProviderState.UNRESOLVED, null, 0, ECurrency.VND, raw);
        };
    }

    /** {@code vnp_Amount} là số tiền × 100; lẻ trăm thì câu trả lời vô nghĩa với VND — không đoán. */
    private static long capturedMinor(Map<String, String> answer) {
        try {
            long scaled = Long.parseLong(answer.get("vnp_Amount"));
            if (scaled % 100 != 0) {
                throw new ProviderUnavailableException("VNPay querydr: vnp_Amount " + scaled + " không chia hết cho 100");
            }
            return scaled / 100;
        } catch (NumberFormatException unreadable) {
            throw new ProviderUnavailableException("VNPay querydr: vnp_Amount không đọc được", unreadable);
        }
    }

    /** Mã tham chiếu gửi VNPay: idempotency key bỏ ký tự không phải chữ/số ({@code vnp_TxnRef} là Alphanumeric). */
    static String txnRef(String idempotencyKey) {
        return idempotencyKey.replaceAll("[^A-Za-z0-9]", "");
    }

    /** IP người dùng nếu có và vừa khuôn của VNPay ([7,45] ký tự), không thì IP máy chủ. */
    private String ipOrServer(String clientIp) {
        if (clientIp != null && clientIp.length() >= 7 && clientIp.length() <= 45) {
            return clientIp;
        }
        return settings.serverIp();
    }
}
