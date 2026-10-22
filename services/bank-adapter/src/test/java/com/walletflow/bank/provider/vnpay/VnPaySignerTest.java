package com.walletflow.bank.provider.vnpay;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chữ ký và định dạng của VNPay: sai một ký tự là mọi lần thu bị VNPay từ chối, hoặc tệ hơn một câu trả lời giả
 * được chấp nhận. Mọi giá trị "đáp án đúng" bên dưới được tính bằng {@code openssl dgst -sha512 -hmac} — một
 * cài đặt HMAC độc lập với JDK — chứ KHÔNG bằng chính {@link VnPaySigner}, để test không tự chứng minh mình.
 */
class VnPaySignerTest {

    private static final String SECRET = "wf-test-secret-key";

    @Test
    @DisplayName("HMAC-SHA512 khớp vector chuẩn RFC 4231 (test case 2)")
    void hmac_khop_vector_chuan_rfc_4231() {
        assertThat(VnPaySigner.hmacSha512("Jefe", "what do ya want for nothing?"))
                .isEqualTo("164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea2505549758bf75c05a994a6d034f65f8f0e6fd"
                        + "caeab1a34d4a6b4b636e070a38bce737");
    }

    @Test
    @DisplayName("HMAC-SHA512 khớp đáp án của openssl cho dữ liệu query string")
    void hmac_khop_dap_an_openssl_cho_query() {
        // printf '%s' '<data>' | openssl dgst -sha512 -hmac 'wf-test-secret-key'
        assertThat(VnPaySigner.hmacSha512(SECRET, "vnp_Amount=1000000&vnp_Command=pay&vnp_TmnCode=WFTEST01"))
                .isEqualTo("02166909f1ef4854fac5e8bf9acf566d56f3ae0bbc28f97dbf08fb1da8a36f2caa0329b51d51a5b3b7e6bf10358686eb"
                        + "141f610465ae4c5cc853e70a21572853");
    }

    @Test
    @DisplayName("HMAC-SHA512 mã hoá khoá và dữ liệu bằng UTF-8 (dữ liệu có tiếng Việt, khoá có ký tự ngoài ASCII)")
    void hmac_dung_utf8_cho_khoa_va_du_lieu() {
        // Khoá UTF-8 của "k3y-ü" = 6b33792dc3bc; đưa vào openssl dạng hex vì đối số dòng lệnh trên Windows bị đổi mã:
        // printf '%s' 'abc|Nạp tiền ví||x' | openssl dgst -sha512 -mac HMAC -macopt hexkey:6b33792dc3bc
        assertThat(VnPaySigner.hmacSha512("k3y-ü", "abc|Nạp tiền ví||x"))
                .isEqualTo("ffdf22e792f4f45a6068430640f71d8a6616e8094d804a3132fbcf0ff4fc384144c8048e81233bde97d0fcdceed3c237"
                        + "d5d09ff5ff7d887a9a88743a8483e6cf");
    }

    @Test
    @DisplayName("signedQuery: sắp theo tên tham số, dấu cách thành '+', ký tự đặc biệt được mã hoá, bỏ tham số rỗng/null, "
            + "chữ ký tính trên đúng chuỗi query đó")
    void signed_query_sap_xep_ma_hoa_va_bo_tham_so_rong() {
        // Cố ý thêm sai thứ tự: b trước a
        TreeMap<String, String> params = new TreeMap<>();
        params.put("b", "2");
        params.put("a", "x y&z");
        params.put("c", "");
        params.put("d", null);

        String signed = VnPaySigner.signedQuery(params, SECRET);

        // printf '%s' 'a=x+y%26z&b=2' | openssl dgst -sha512 -hmac 'wf-test-secret-key'
        assertThat(signed).isEqualTo("a=x+y%26z&b=2&vnp_SecureHash="
                + "a70a710a73445ab51fe6918bf9f6911dd107ff66de5b4420a8db2d12bfc97114e0ec4d95ee70ed583bd7eedbae15e451"
                + "2f34db4089fb9bb4dfb6b0fbf749243e");
    }

    @Test
    @DisplayName("pipeJoined: đúng thứ tự khai báo, giá trị null hoặc thiếu thành chuỗi rỗng, nối bằng '|'")
    void pipe_joined_giu_thu_tu_va_null_thanh_rong() {
        Map<String, String> fields = new HashMap<>();
        fields.put("b", "2");
        fields.put("a", "1");
        fields.put("n", null);

        assertThat(VnPaySigner.pipeJoined(fields, "a", "n", "missing", "b")).isEqualTo("1|||2");
        assertThat(VnPaySigner.pipeJoined(fields, "b", "a")).isEqualTo("2|1");
        assertThat(VnPaySigner.pipeJoined(fields, "a")).as("một trường: không có dấu '|'").isEqualTo("1");
    }

    @Test
    @DisplayName("signatureMatches: không phân biệt hoa thường; sai chữ ký, null hoặc khác độ dài đều là false")
    void signature_matches_khong_phan_biet_hoa_thuong_va_tu_choi_null() {
        assertThat(VnPaySigner.signatureMatches("abcdef0123", "abcdef0123")).isTrue();
        assertThat(VnPaySigner.signatureMatches("abcdef0123", "ABCDEF0123")).isTrue();
        assertThat(VnPaySigner.signatureMatches("ABCDEF0123", "abcdef0123")).isTrue();

        assertThat(VnPaySigner.signatureMatches("abcdef0123", "abcdef0124")).isFalse();
        assertThat(VnPaySigner.signatureMatches("abcdef0123", "abcdef012")).as("ngắn hơn").isFalse();
        assertThat(VnPaySigner.signatureMatches("abcdef0123", "abcdef01234")).as("dài hơn").isFalse();
        assertThat(VnPaySigner.signatureMatches("abcdef0123", "")).isFalse();
        assertThat(VnPaySigner.signatureMatches("abcdef0123", null)).as("thiếu chữ ký = không hợp lệ").isFalse();
    }

    @Test
    @DisplayName("Mốc thời gian định dạng yyyyMMddHHmmss theo GMT+7")
    void dinh_dang_ngay_theo_gmt_cong_7() {
        assertThat(VnPaySigner.format(Instant.parse("2026-10-18T07:30:00Z"))).isEqualTo("20261018143000");
        assertThat(VnPaySigner.format(Instant.parse("2026-10-18T17:30:00Z")))
                .as("17:30Z đã sang ngày hôm sau ở Việt Nam")
                .isEqualTo("20261019003000");
        assertThat(VnPaySigner.format(Instant.parse("2026-12-31T16:59:59Z"))).isEqualTo("20261231235959");
        assertThat(VnPaySigner.format(Instant.parse("2026-12-31T17:00:00Z")))
                .as("giao thừa theo giờ Việt Nam")
                .isEqualTo("20270101000000");
    }

    @Test
    @DisplayName("Mốc thời gian có phần lẻ giây bị cắt, không làm tròn lên (VNPay chỉ có độ phân giải giây)")
    void dinh_dang_ngay_cat_phan_le_giay() {
        assertThat(VnPaySigner.format(Instant.parse("2026-10-18T07:30:00.999Z"))).isEqualTo("20261018143000");
    }
}
