package com.walletflow.common.logging;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Che dữ liệu nhạy cảm trong một dòng log TRƯỚC khi nó rời process (roadmap 2.2, ADR-017).
 *
 * <p>Đây là lưới an toàn, không phải cách chính: code không được log bí mật ngay từ đầu (CLAUDE.md). Nhưng
 * với cổng thanh toán, bí mật lọt vào log qua những đường không ai cố ý — message của exception chứa URL
 * có chữ ký, một câu trả lời thô của provider được in khi debug, một header Authorization trong log của thư
 * viện HTTP. Một dòng log đã ship đi (stdout → hệ thống gom log) thì không thu hồi được.
 *
 * <p>Che theo hướng THỪA hơn thiếu: một dãy số ngày giờ tình cờ qua được kiểm tra Luhn bị che thì chỉ
 * mất chút tiện khi đọc log; một số thẻ lọt ra thì là sự cố PCI.
 */
public final class SensitiveDataMasker {

    static final String MASK = "***";

    /**
     * {@code khoá=giá trị} / {@code "khoá": "giá trị"} với khoá là chữ ký hoặc bí mật: chữ ký VNPay, hash
     * secret, mật khẩu, token, API key. Giá trị dừng ở dấu phân cách của query string, JSON hoặc khoảng trắng.
     *
     * <p>Khoá phải bắt đầu ở ĐẦU một cụm chữ/số (lookbehind) và tiền tố tối đa 40 ký tự: không thì regex thử
     * khớp ở mọi vị trí trong một cụm dài (payload base64) rồi backtrack — O(n²) trên mỗi dòng log, đúng loại
     * chậm mà ai cũng chỉ thấy khi production log một message lớn.
     */
    private static final Pattern SECRET_KEY_VALUE = Pattern.compile(
            "(?i)(?<![a-z0-9_\\-])(vnp_SecureHash|[a-z0-9_\\-]{0,40}(?:secret|password|passwd|token|api[_-]?key))"
                    + "(\"?\\s*[=:]\\s*\"?)([^&\\s\"',;}\\]]+)");

    private static final Pattern BEARER = Pattern.compile("(?i)\\b(Bearer)\\s+[A-Za-z0-9\\-._~+/]+=*");

    /** JWT trần (không có tiền tố Bearer): ba đoạn base64url, đoạn đầu luôn bắt đầu bằng {@code eyJ}. */
    private static final Pattern JWT = Pattern.compile("\\beyJ[A-Za-z0-9_-]{5,}\\.[A-Za-z0-9_-]{5,}\\.[A-Za-z0-9_-]*");

    /** Ứng viên số thẻ (PAN): 13–19 chữ số, có thể ngăn bởi dấu cách/gạch ngang như người dùng gõ. */
    private static final Pattern PAN_CANDIDATE = Pattern.compile("(?<![0-9])[0-9](?:[ -]?[0-9]){12,18}(?![0-9])");

    private SensitiveDataMasker() {
    }

    public static String mask(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String masked = BEARER.matcher(text).replaceAll("$1 " + MASK);
        masked = JWT.matcher(masked).replaceAll(MASK);
        masked = SECRET_KEY_VALUE.matcher(masked).replaceAll("$1$2" + MASK);
        return maskCardNumbers(masked);
    }

    /** Chỉ che dãy số qua được kiểm tra Luhn — mã giao dịch, số tiền dài thì giữ nguyên để còn đọc được log. */
    private static String maskCardNumbers(String text) {
        Matcher matcher = PAN_CANDIDATE.matcher(text);
        StringBuilder out = null;
        while (matcher.find()) {
            String digits = matcher.group().replaceAll("[ -]", "");
            if (digits.length() < 13 || !passesLuhn(digits)) {
                continue;
            }
            if (out == null) {
                out = new StringBuilder(text.length());
            }
            // Giữ 4 số cuối như cách ngân hàng hiển thị — đủ để đối chiếu với người dùng, không đủ để dùng
            matcher.appendReplacement(out, Matcher.quoteReplacement(MASK + digits.substring(digits.length() - 4)));
        }
        if (out == null) {
            return text;
        }
        matcher.appendTail(out);
        return out.toString();
    }

    static boolean passesLuhn(String digits) {
        int sum = 0;
        boolean doubleIt = false;
        for (int i = digits.length() - 1; i >= 0; i--) {
            int digit = digits.charAt(i) - '0';
            if (doubleIt) {
                digit *= 2;
                if (digit > 9) {
                    digit -= 9;
                }
            }
            sum += digit;
            doubleIt = !doubleIt;
        }
        return sum % 10 == 0;
    }
}
