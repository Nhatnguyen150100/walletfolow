package com.walletflow.common.logging;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bộ che log (ADR-017): che đủ thứ phải che, và KHÔNG che những thứ vận hành cần đọc (mã giao dịch, số tiền,
 * mã tham chiếu) — một bộ che xoá sạch mọi con số thì người ta sẽ tắt nó đi.
 */
class SensitiveDataMaskerTest {

    @Test
    @DisplayName("Chữ ký VNPay trong URL thanh toán bị che, các tham số khác của URL giữ nguyên")
    void che_chu_ky_vnpay_trong_url() {
        String url = "https://sandbox.vnpayment.vn/paymentv2/vpcpay.html?vnp_Amount=25000000&vnp_TxnRef=ab12cd"
                + "&vnp_SecureHash=9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

        String masked = SensitiveDataMasker.mask(url);

        assertThat(masked)
                .doesNotContain("9f86d081884c7d659a2feaa0c55ad015")
                .contains("vnp_SecureHash=***")
                .contains("vnp_Amount=25000000")
                .contains("vnp_TxnRef=ab12cd");
    }

    @Test
    @DisplayName("Chữ ký trong JSON (câu trả lời querydr) và hash secret ở mọi cách viết đều bị che")
    void che_chu_ky_va_secret_trong_json_va_cau_hinh() {
        assertThat(SensitiveDataMasker.mask("{\"vnp_TxnRef\":\"ab12\",\"vnp_SecureHash\":\"deadbeef\"}"))
                .isEqualTo("{\"vnp_TxnRef\":\"ab12\",\"vnp_SecureHash\":\"***\"}");
        assertThat(SensitiveDataMasker.mask("hash-secret: SUPERSECRETKEY")).isEqualTo("hash-secret: ***");
        assertThat(SensitiveDataMasker.mask("vnp_HashSecret=SUPERSECRETKEY&x=1")).isEqualTo("vnp_HashSecret=***&x=1");
        assertThat(SensitiveDataMasker.mask("password=hunter2 user=alice")).isEqualTo("password=*** user=alice");
        assertThat(SensitiveDataMasker.mask("api_key=k-123")).isEqualTo("api_key=***");
    }

    @Test
    @DisplayName("Bearer token và JWT trần bị che")
    void che_bearer_va_jwt() {
        String jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1c2VyLTEifQ.c2lnbmF0dXJlLWhlcmU";

        assertThat(SensitiveDataMasker.mask("Authorization: Bearer " + jwt))
                .isEqualTo("Authorization: Bearer ***");
        assertThat(SensitiveDataMasker.mask("token cũ " + jwt + " hết hạn"))
                .doesNotContain("eyJzdWIiOiJ1c2VyLTEifQ")
                .contains("token cũ")
                .contains("hết hạn");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"4111111111111111", "4111 1111 1111 1111", "4111-1111-1111-1111", "5555555555554444"})
    @DisplayName("Số thẻ hợp lệ Luhn (kể cả có dấu cách/gạch) bị che, chỉ giữ 4 số cuối")
    void che_so_the(String pan) {
        String masked = SensitiveDataMasker.mask("thẻ " + pan + " bị từ chối");

        String last4 = pan.replaceAll("[ -]", "").substring(12);
        assertThat(masked).isEqualTo("thẻ ***" + last4 + " bị từ chối");
    }

    @Test
    @DisplayName("Không che thứ vận hành cần đọc: dãy số không qua Luhn, số tiền, UUID, mã tham chiếu, chữ 'secret' không có giá trị")
    void khong_che_du_lieu_van_hanh() {
        String line = "Bank: lần thu 3f2a1b0c-9d8e-4f7a-8b6c-5d4e3f2a1b0c số tiền 250000000 mã VNPay 1234567890123 "
                + "— không có secret nào ở đây";

        assertThat(SensitiveDataMasker.mask(line)).isEqualTo(line);
        assertThat(SensitiveDataMasker.passesLuhn("1234567890123")).as("tiền điều kiện của dòng trên").isFalse();
    }

    @Test
    // Luồng riêng: regex backtrack không dừng được bằng interrupt, nhưng test vẫn ĐỎ đúng hạn thay vì treo cả fork
    @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @DisplayName("Dòng log rất dài (payload base64 200 KB) được che trong thời gian tuyến tính, không treo việc ghi log")
    void dong_log_dai_khong_lam_regex_backtrack() {
        String blob = "a".repeat(200_000);
        String line = "payload=" + blob + " password=hunter2";

        long started = System.nanoTime();
        String masked = SensitiveDataMasker.mask(line);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000;

        assertThat(masked).endsWith("password=***").contains(blob);
        // Bản không neo đầu cụm chạy cỡ phút trên dòng này; tuyến tính thì vài chục ms
        assertThat(elapsedMs).as("thời gian che (ms)").isLessThan(2_000);
    }

    @Test
    @DisplayName("null và chuỗi rỗng đi qua nguyên vẹn")
    void null_va_rong() {
        assertThat(SensitiveDataMasker.mask(null)).isNull();
        assertThat(SensitiveDataMasker.mask("")).isEmpty();
    }
}
