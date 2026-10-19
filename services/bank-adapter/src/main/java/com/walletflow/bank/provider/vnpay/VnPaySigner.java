package com.walletflow.bank.provider.vnpay;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

/**
 * Chữ ký và định dạng của VNPay API 2.1.0 — một chỗ duy nhất, vì sai một ký tự ở đây là mọi lần thu đều
 * hỏng (provider từ chối URL) hoặc tệ hơn, câu trả lời giả mạo được chấp nhận.
 *
 * <ul>
 *   <li><b>URL thanh toán:</b> tham số sắp theo tên, giá trị URL-encode US-ASCII (dấu cách thành {@code +}),
 *       HMAC-SHA512 trên CHÍNH chuỗi query đó (đúng mẫu Java của VNPay).</li>
 *   <li><b>API querydr:</b> HMAC-SHA512 trên các giá trị nối bằng {@code |} theo thứ tự cố định do VNPay quy
 *       định — cả chiều gửi lẫn chiều nhận.</li>
 * </ul>
 */
final class VnPaySigner {

    /** Mọi mốc thời gian của VNPay: {@code yyyyMMddHHmmss}, GMT+7 (Việt Nam không có giờ mùa hè). */
    static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.ROOT)
            .withZone(ZoneOffset.ofHours(7));

    private static final String HMAC = "HmacSHA512";

    private VnPaySigner() {
    }

    static String format(Instant instant) {
        return DATE.format(instant);
    }

    /** Query string kèm {@code vnp_SecureHash}. Tham số rỗng bị bỏ, như mẫu của VNPay. */
    static String signedQuery(SortedMap<String, String> params, String hashSecret) {
        // Dùng chung một cách dựng với chiều NHẬN (ipnSignedData): nếu hai chiều lệch nhau dù chỉ quy tắc
        // encode, thì hoặc ta ký sai (VNPay từ chối) hoặc ta kiểm sai (nhận cả chữ ký giả)
        String query = ipnSignedData(params);
        return query + "&vnp_SecureHash=" + hmacSha512(hashSecret, query);
    }

    /**
     * Chuỗi được ký của một IPN: ĐÚNG cách dựng như {@link #signedQuery} nhưng không có phần
     * {@code &vnp_SecureHash=…}. VNPay ký trên chính chuỗi query nó gửi đi, nên muốn kiểm thì phải dựng
     * lại y hệt — kể cả quy tắc bỏ tham số rỗng và encode dấu cách thành {@code +}.
     *
     * <p>Người gọi phải loại {@code vnp_SecureHash} và {@code vnp_SecureHashType} khỏi {@code params}
     * trước: hai trường đó không nằm trong dữ liệu ký.
     */
    static String ipnSignedData(SortedMap<String, String> params) {
        return params.entrySet().stream()
                .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
                .map(entry -> encode(entry.getKey()) + "=" + encode(entry.getValue()))
                .collect(Collectors.joining("&"));
    }

    /** Dữ liệu ký của API: các giá trị theo đúng thứ tự, {@code null} thành chuỗi rỗng, nối bằng {@code |}. */
    static String pipeJoined(Map<String, String> fields, String... orderedNames) {
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < orderedNames.length; i++) {
            if (i > 0) {
                data.append('|');
            }
            String value = fields.get(orderedNames[i]);
            data.append(value == null ? "" : value);
        }
        return data.toString();
    }

    static String hmacSha512(String secret, String data) {
        try {
            Mac mac = Mac.getInstance(HMAC);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC));
            return HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException impossible) {
            // HmacSHA512 là thuật toán bắt buộc của mọi JDK
            throw new IllegalStateException("JDK không có " + HMAC, impossible);
        }
    }

    /** So chữ ký thời gian hằng (không lộ độ dài phần khớp qua thời gian phản hồi); không phân biệt hoa thường. */
    static boolean signatureMatches(String expected, String actual) {
        if (actual == null) {
            return false;
        }
        return MessageDigest.isEqual(expected.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII),
                actual.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.US_ASCII);
    }
}
