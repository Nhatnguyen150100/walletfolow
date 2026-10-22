package com.walletflow.bank.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * VNPay giả: HTTP server thật trên cổng ngẫu nhiên (JDK {@code com.sun.net.httpserver}, không thêm thư viện), đóng
 * hai vai — trang thanh toán (test đóng vai người dùng mở URL và trả tiền, {@link #pay}) và API {@code querydr}.
 *
 * <p><b>Chữ ký được cài đặt ĐỘC LẬP với code production</b> (không gọi {@code VnPaySigner}): tự sắp tham số, tự mã
 * hoá, tự HMAC-SHA512 bằng {@code javax.crypto}. Nếu cả hai phía cùng sai theo một cách thì test không phát hiện
 * được; nên phía này viết lại từ tài liệu VNPay, và còn bắt thêm việc URL phải ở dạng CHUẨN (đã sắp, đã mã hoá) chứ
 * không chỉ "ký khớp".
 *
 * <p><b>Cư xử như VNPay ở điểm nguy hiểm nhất:</b> {@code querydr} tìm giao dịch theo cặp
 * {@code (vnp_TxnRef, vnp_TransactionDate)}; ngày sai thì VNPay nói "không tìm thấy" (mã 91) — đúng câu trả lời
 * của một phiên chưa ai mở. Một adapter hỏi sai ngày sẽ thấy "chưa thu" cho người đã trả tiền.
 */
public final class FakeVnPayServer implements AutoCloseable {

    public static final String PAY_PATH = "/paymentv2/vpcpay.html";
    public static final String QUERY_PATH = "/merchant_webapi/api/transaction";

    /** Thứ tự ký của request querydr theo tài liệu VNPay. */
    private static final List<String> REQUEST_SIGNED_FIELDS = List.of("vnp_RequestId", "vnp_Version", "vnp_Command",
            "vnp_TmnCode", "vnp_TxnRef", "vnp_TransactionDate", "vnp_CreateDate", "vnp_IpAddr", "vnp_OrderInfo");
    /** Thứ tự ký của câu trả lời querydr theo tài liệu VNPay. */
    private static final List<String> RESPONSE_SIGNED_FIELDS = List.of("vnp_ResponseId", "vnp_Command",
            "vnp_ResponseCode", "vnp_Message", "vnp_TmnCode", "vnp_TxnRef", "vnp_Amount", "vnp_BankCode",
            "vnp_PayDate", "vnp_TransactionNo", "vnp_TransactionType", "vnp_TransactionStatus", "vnp_OrderInfo",
            "vnp_PromotionCode", "vnp_PromotionAmount");
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
            .withZone(ZoneOffset.ofHours(7));
    private static final String HASH_PARAM = "&vnp_SecureHash=";

    /** Một yêu cầu querydr nhận được, kèm kết quả kiểm checksum của chính fake. */
    public record QueryRequest(String rawBody, Map<String, String> fields, boolean checksumValid,
                               String contentType) {
        public String get(String name) {
            return fields.get(name);
        }
    }

    /** Giao dịch VNPay đã biết (sau khi người dùng mở URL). */
    private static final class Txn {
        private final String txnRef;
        private final String createDate;
        private volatile long scaledAmount;
        private volatile String status;
        /** "01" = giao dịch thanh toán, "02"/"03" = hoàn toàn phần/một phần (VNPay trả cùng một câu trả lời querydr). */
        private volatile String transactionType = "01";
        private final String orderInfo;

        private Txn(String txnRef, String createDate, long scaledAmount, String status, String orderInfo) {
            this.txnRef = txnRef;
            this.createDate = createDate;
            this.scaledAmount = scaledAmount;
            this.status = status;
            this.orderInfo = orderInfo;
        }
    }

    private final String tmnCode;
    private final String hashSecret;
    private final HttpServer server;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ObjectMapper json = new ObjectMapper();

    private final ConcurrentMap<String, Txn> txns = new ConcurrentHashMap<>();
    private final List<QueryRequest> requests = new CopyOnWriteArrayList<>();

    private volatile String forcedResponseCode;
    private volatile boolean tamperSignature;
    private volatile boolean forgeSuccessAfterSigning;
    private volatile String forcedTmnCode;
    private volatile String forcedTxnRef;
    private volatile String forcedCommand;
    private volatile Integer forcedHttpStatus;
    private volatile String forcedRawBody;
    private volatile Duration delay = Duration.ZERO;
    /** Đếm về 0 khi reset/close để một lần "trễ" đang chờ được thả ngay, không giữ thread tới hết giờ. */
    private volatile CountDownLatch release = new CountDownLatch(1);

    public FakeVnPayServer(String tmnCode, String hashSecret) {
        this.tmnCode = tmnCode;
        this.hashSecret = hashSecret;
        try {
            // IPv4 tường minh: getLoopbackAddress() có thể ra ::1, mà URL "http://::1:port" không hợp lệ
            this.server = HttpServer.create(
                    new InetSocketAddress(InetAddress.getByAddress(new byte[]{127, 0, 0, 1}), 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException("Không mở được cổng cho VNPay giả", e);
        }
        server.createContext(QUERY_PATH, this::handleQuery);
        server.setExecutor(executor);
        server.start();
    }

    public String payUrl() {
        return "http://" + hostPort() + PAY_PATH;
    }

    public String apiUrl() {
        return "http://" + hostPort() + QUERY_PATH;
    }

    // ------------------------------------------------------------------
    // Người dùng mở URL thanh toán
    // ------------------------------------------------------------------

    /**
     * Test đóng vai người dùng mở URL rồi TRẢ TIỀN thành công (trạng thái giao dịch "00"). Kiểm URL như VNPay
     * kiểm: đúng trang, query ở dạng chuẩn, chữ ký đúng, đúng mã merchant, đủ trường bắt buộc.
     *
     * @return các tham số của URL đã giải mã (có cả {@code vnp_SecureHash}) để test đối chiếu
     * @throws AssertionError URL sai chuẩn hoặc chữ ký sai — VNPay thật sẽ từ chối trang thanh toán
     */
    public Map<String, String> pay(String paymentUrl) {
        return record(paymentUrl, "00");
    }

    /** Người dùng mở URL nhưng chưa trả xong (trạng thái giao dịch "01") — VNPay đã biết giao dịch này. */
    public Map<String, String> open(String paymentUrl) {
        return record(paymentUrl, "01");
    }

    private Map<String, String> record(String paymentUrl, String status) {
        URI uri = URI.create(paymentUrl);
        String page = uri.getScheme() + "://" + uri.getRawAuthority() + uri.getRawPath();
        require(page.equals(payUrl()), "URL không trỏ về trang thanh toán của VNPay giả: " + page);

        String rawQuery = uri.getRawQuery();
        require(rawQuery != null, "URL không có query");
        int hashAt = rawQuery.lastIndexOf(HASH_PARAM);
        require(hashAt > 0, "Query không kết thúc bằng vnp_SecureHash");
        String signedPart = rawQuery.substring(0, hashAt);
        String receivedHash = rawQuery.substring(hashAt + HASH_PARAM.length());

        TreeMap<String, String> params = new TreeMap<>();
        for (String pair : signedPart.split("&")) {
            int eq = pair.indexOf('=');
            require(eq > 0, "Tham số không có dạng khoá=giá trị: " + pair);
            String name = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.US_ASCII);
            String previous = params.put(name, URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.US_ASCII));
            require(previous == null, "Tham số lặp: " + name);
        }

        // Dạng chuẩn của VNPay: sắp theo tên, giá trị URL-encode US-ASCII, bỏ tham số rỗng
        String canonical = params.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> encode(entry.getKey()) + "=" + encode(entry.getValue()))
                .collect(Collectors.joining("&"));
        require(canonical.equals(signedPart),
                "Query không ở dạng chuẩn (sắp theo tên, mã hoá US-ASCII).\n  nhận được: " + signedPart
                        + "\n  chuẩn là:  " + canonical);
        require(hmac(hashSecret, canonical).equalsIgnoreCase(receivedHash), "vnp_SecureHash của URL thanh toán SAI");

        require(tmnCode.equals(params.get("vnp_TmnCode")), "vnp_TmnCode sai: " + params.get("vnp_TmnCode"));
        require("2.1.0".equals(params.get("vnp_Version")), "vnp_Version sai: " + params.get("vnp_Version"));
        require("pay".equals(params.get("vnp_Command")), "vnp_Command sai: " + params.get("vnp_Command"));
        require("VND".equals(params.get("vnp_CurrCode")), "vnp_CurrCode sai: " + params.get("vnp_CurrCode"));
        String txnRef = params.get("vnp_TxnRef");
        require(txnRef != null && txnRef.matches("[A-Za-z0-9]{1,100}"), "vnp_TxnRef không phải Alphanumeric: " + txnRef);
        String amount = params.get("vnp_Amount");
        require(amount != null && amount.matches("[0-9]{1,12}"), "vnp_Amount không phải Numeric[1,12]: " + amount);
        String createDate = params.get("vnp_CreateDate");
        String expireDate = params.get("vnp_ExpireDate");
        require(createDate != null && createDate.matches("[0-9]{14}"), "vnp_CreateDate sai dạng: " + createDate);
        require(expireDate != null && expireDate.matches("[0-9]{14}") && expireDate.compareTo(createDate) > 0,
                "vnp_ExpireDate phải sau vnp_CreateDate: " + expireDate);
        String ip = params.get("vnp_IpAddr");
        require(ip != null && ip.length() >= 7 && ip.length() <= 45, "vnp_IpAddr ngoài khuôn [7,45]: " + ip);

        txns.put(txnRef, new Txn(txnRef, createDate, Long.parseLong(amount), status,
                params.getOrDefault("vnp_OrderInfo", "")));

        Map<String, String> result = new LinkedHashMap<>(params);
        result.put("vnp_SecureHash", receivedHash);
        return result;
    }

    // ------------------------------------------------------------------
    // Điều khiển cho test
    // ------------------------------------------------------------------

    /** Đổi {@code vnp_TransactionStatus} của một giao dịch đã biết ("00" đã trả, "01" chưa xong, "02" lỗi, "04" đảo...). */
    public void setStatus(String txnRef, String transactionStatus) {
        txn(txnRef).status = transactionStatus;
    }

    /** Đổi {@code vnp_TransactionType} ("01" thanh toán, mặc định; "02" hoàn toàn phần; "03" hoàn một phần). */
    public void setTransactionType(String txnRef, String transactionType) {
        txn(txnRef).transactionType = transactionType;
    }

    /** Đổi {@code vnp_Amount} (đã nhân 100) mà VNPay báo cho giao dịch — giả lập số thu khác số yêu cầu. */
    public void setAmount(String txnRef, long scaledAmount) {
        txn(txnRef).scaledAmount = scaledAmount;
    }

    /** Mọi câu trả lời hợp lệ (đúng checksum) mang mã này thay vì kết quả thật, ví dụ "99". {@code null} = bình thường. */
    public void respondWithCode(String responseCode) {
        this.forcedResponseCode = responseCode;
    }

    /** Câu trả lời mang chữ ký SAI (một ký tự bị đổi) — kẻ giả mạo hoặc proxy sửa nội dung. */
    public void tamperSignature(boolean enabled) {
        this.tamperSignature = enabled;
    }

    /**
     * Kẻ tấn công sửa NỘI DUNG sau khi VNPay đã ký: câu trả lời thật được ký rồi bị ghi đè thành "thành công"
     * (ResponseCode 00, TransactionStatus 00), giữ nguyên chữ ký cũ.
     */
    public void forgeSuccessAfterSigning(boolean enabled) {
        this.forgeSuccessAfterSigning = enabled;
    }

    /** Câu trả lời đúng chữ ký nhưng mang mã merchant khác. */
    public void answerWithTmnCode(String otherTmnCode) {
        this.forcedTmnCode = otherTmnCode;
    }

    /** Câu trả lời đúng chữ ký nhưng nói về giao dịch khác. Chuỗi rỗng = không nói về giao dịch nào. */
    public void answerWithTxnRef(String otherTxnRef) {
        this.forcedTxnRef = otherTxnRef;
    }

    /** Câu trả lời đúng chữ ký nhưng của lệnh khác (ví dụ {@code refund} phát lại vào chỗ {@code querydr}). */
    public void answerWithCommand(String otherCommand) {
        this.forcedCommand = otherCommand;
    }

    /** Trả thẳng HTTP status này với thân rỗng (ví dụ 500). {@code null} = bình thường. */
    public void respondWithHttpStatus(Integer status) {
        this.forcedHttpStatus = status;
    }

    /** Trả thẳng thân này (HTTP 200) thay vì câu trả lời có cấu trúc — ví dụ không phải JSON. {@code null} = bình thường. */
    public void respondWithRawBody(String body) {
        this.forcedRawBody = body;
    }

    /** Mọi yêu cầu querydr chờ chừng này TRƯỚC khi trả lời (mô phỏng provider chậm). */
    public void delay(Duration delay) {
        this.delay = delay;
    }

    /** Về trạng thái ban đầu: quên mọi giao dịch và yêu cầu, bỏ mọi điều khiển, thả các yêu cầu đang chờ. */
    public void reset() {
        txns.clear();
        requests.clear();
        forcedResponseCode = null;
        tamperSignature = false;
        forgeSuccessAfterSigning = false;
        forcedTmnCode = null;
        forcedTxnRef = null;
        forcedCommand = null;
        forcedHttpStatus = null;
        forcedRawBody = null;
        delay = Duration.ZERO;
        CountDownLatch old = release;
        release = new CountDownLatch(1);
        old.countDown();
    }

    /** Các yêu cầu querydr đã nhận, theo thứ tự đến. */
    public List<QueryRequest> requests() {
        return List.copyOf(requests);
    }

    /** VNPay có biết giao dịch này không (người dùng đã mở URL của nó). */
    public boolean knows(String txnRef) {
        return txns.containsKey(txnRef);
    }

    @Override
    public void close() {
        release.countDown();
        server.stop(0);
        executor.shutdownNow();
    }

    // ------------------------------------------------------------------
    // API querydr
    // ------------------------------------------------------------------

    private void handleQuery(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"POST".equals(exchange.getRequestMethod())) {
                reply(exchange, 405, "");
                return;
            }
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Map<String, String> request;
            try {
                request = parseObject(body);
            } catch (IOException | RuntimeException unreadable) {
                reply(exchange, 400, "");
                return;
            }
            boolean checksumValid = hmac(hashSecret, pipeJoined(request, REQUEST_SIGNED_FIELDS))
                    .equalsIgnoreCase(request.get("vnp_SecureHash"));
            requests.add(new QueryRequest(body, request, checksumValid,
                    exchange.getRequestHeaders().getFirst("Content-Type")));

            awaitDelay();

            Integer httpStatus = forcedHttpStatus;
            if (httpStatus != null) {
                reply(exchange, httpStatus, "");
                return;
            }
            String rawBody = forcedRawBody;
            if (rawBody != null) {
                reply(exchange, 200, rawBody);
                return;
            }
            reply(exchange, 200, json.writeValueAsString(answer(request, checksumValid)));
        } catch (RuntimeException failure) {
            // Lỗi của chính fake không được giả làm câu trả lời của VNPay
            throw new IllegalStateException("VNPay giả lỗi nội bộ", failure);
        }
    }

    private Map<String, String> answer(Map<String, String> request, boolean checksumValid) {
        String requestedRef = request.getOrDefault("vnp_TxnRef", "");
        Map<String, String> fields = blankAnswer(requestedRef);

        if (!checksumValid) {
            return signed(fields, "97", "Invalid Checksum");
        }
        if (!tmnCode.equals(request.get("vnp_TmnCode"))) {
            return signed(fields, "02", "Invalid TmnCode");
        }
        String forced = forcedResponseCode;
        if (forced != null) {
            return signed(fields, forced, "Forced by test");
        }
        Txn txn = txns.get(requestedRef);
        if (txn == null || !txn.createDate.equals(request.get("vnp_TransactionDate"))) {
            // Hành vi thật của VNPay: ngày sai = không tìm thấy giao dịch
            return signed(fields, "91", "Unable to find the transaction");
        }
        fields.put("vnp_Amount", Long.toString(txn.scaledAmount));
        fields.put("vnp_BankCode", "NCB");
        fields.put("vnp_PayDate", DATE.format(Instant.now()));
        fields.put("vnp_TransactionNo", "14000001");
        fields.put("vnp_TransactionType", txn.transactionType);
        fields.put("vnp_TransactionStatus", txn.status);
        fields.put("vnp_OrderInfo", txn.orderInfo);
        return signed(fields, "00", "QueryDR Success");
    }

    private Map<String, String> blankAnswer(String txnRef) {
        Map<String, String> fields = new LinkedHashMap<>();
        for (String name : RESPONSE_SIGNED_FIELDS) {
            fields.put(name, "");
        }
        fields.put("vnp_ResponseId", UUID.randomUUID().toString().replace("-", ""));
        fields.put("vnp_Command", forcedCommand != null ? forcedCommand : "querydr");
        fields.put("vnp_TmnCode", forcedTmnCode != null ? forcedTmnCode : tmnCode);
        fields.put("vnp_TxnRef", forcedTxnRef != null ? forcedTxnRef : txnRef);
        return fields;
    }

    private Map<String, String> signed(Map<String, String> fields, String code, String message) {
        fields.put("vnp_ResponseCode", code);
        fields.put("vnp_Message", message);
        String signature = hmac(hashSecret, pipeJoined(fields, RESPONSE_SIGNED_FIELDS));
        if (forgeSuccessAfterSigning) {
            fields.put("vnp_ResponseCode", "00");
            fields.put("vnp_TransactionStatus", "00");
        }
        if (tamperSignature) {
            // Đổi ký tự đầu thành một ký tự hex khác
            signature = (signature.charAt(0) == '0' ? '1' : '0') + signature.substring(1);
        }
        fields.put("vnp_SecureHash", signature);
        return fields;
    }

    private void awaitDelay() {
        Duration wait = delay;
        if (wait.isZero()) {
            return;
        }
        try {
            // Chờ trên latch (không phải ngủ cứng): reset()/close() thả ngay. Đây là MÔ PHỎNG độ trễ của provider,
            // không phải đồng bộ hoá test.
            release.await(wait.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------
    // Tiện ích — viết riêng, không dùng chung với code production
    // ------------------------------------------------------------------

    private Map<String, String> parseObject(String body) throws IOException {
        JsonNode root = json.readTree(body);
        if (root == null || !root.isObject()) {
            throw new IOException("không phải JSON object");
        }
        Map<String, String> fields = new LinkedHashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> it = root.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> field = it.next();
            fields.put(field.getKey(), field.getValue().isNull() ? null : field.getValue().asText());
        }
        return fields;
    }

    private static String pipeJoined(Map<String, String> fields, List<String> order) {
        return order.stream()
                .map(name -> fields.get(name) == null ? "" : fields.get(name))
                .collect(Collectors.joining("|"));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.US_ASCII);
    }

    private static String hmac(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA512"));
            return HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private Txn txn(String txnRef) {
        Txn txn = txns.get(txnRef);
        if (txn == null) {
            throw new IllegalArgumentException("VNPay giả chưa biết giao dịch " + txnRef + " (người dùng chưa mở URL?)");
        }
        return txn;
    }

    private String hostPort() {
        InetSocketAddress address = server.getAddress();
        return address.getAddress().getHostAddress() + ":" + address.getPort();
    }

    private static void reply(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            exchange.getResponseBody().write(bytes);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("VNPay giả từ chối: " + message);
        }
    }
}
