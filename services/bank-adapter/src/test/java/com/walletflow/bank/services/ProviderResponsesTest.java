package com.walletflow.bank.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ProviderResponses} đứng giữa câu trả lời của provider và cột {@code jsonb}/{@code VARCHAR(32)}: lúc
 * ghi thì provider ĐÃ trả lời (phiên có thể đã tạo, tiền có thể đã thu), nên một câu UPDATE hỏng vì định dạng
 * biến câu trả lời hợp lệ thành lỗi kỹ thuật. Thuần logic, không cần DB.
 */
class ProviderResponsesTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ProviderResponses responses = new ProviderResponses(mapper);

    // ------------------------------------------------------------------
    // json(raw)
    // ------------------------------------------------------------------
    @ParameterizedTest(name = "[{index}] \"{0}\"")
    @NullAndEmptySource
    @ValueSource(strings = {" ", "   ", "\n\t "})
    @DisplayName("Câu trả lời null/rỗng/chỉ khoảng trắng: thành JSON null (không bao giờ trả null cho tham số native)")
    void cau_tra_loi_trong_thanh_json_null(String raw) {
        assertThat(responses.json(raw)).isEqualTo("null");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {
            "{\"result\":\"CREATED\",\"ref\":\"MOCK-1\"}",
            "{\"nested\":{\"a\":[1,2,3]},\"amount\":5000}",
            "[1,2,3]",
            "42",
            "\"mot chuoi\"",
            "true"})
    @DisplayName("JSON hợp lệ: giữ NGUYÊN VĂN từng byte (không serialize lại)")
    void json_hop_le_giu_nguyen_van(String raw) {
        assertThat(responses.json(raw)).isSameAs(raw).isEqualTo(raw);
    }

    @Test
    @DisplayName("JSON hợp lệ có khoảng trắng/xuống dòng: vẫn giữ nguyên văn — đối soát cần đúng byte provider trả")
    void json_co_khoang_trang_giu_nguyen_van() {
        String raw = "{ \"a\" : 1,\n  \"b\" : [ 2 ] }";

        assertThat(responses.json(raw)).isEqualTo(raw);
    }

    @Test
    @DisplayName("Không phải JSON (query string kiểu VNPay): bọc thành chuỗi JSON, parse lại ra đúng văn bản gốc")
    void query_string_kieu_vnpay_duoc_boc_thanh_chuoi_json() throws Exception {
        String raw = "vnp_ResponseCode=00&vnp_TxnRef=abc&vnp_Amount=500000";

        String wrapped = responses.json(raw);

        JsonNode parsed = mapper.readTree(wrapped);
        assertThat(parsed.isTextual()).as("kết quả phải là một chuỗi JSON hợp lệ").isTrue();
        assertThat(parsed.asText()).isEqualTo(raw);
    }

    @Test
    @DisplayName("JSON kèm rác phía sau: KHÔNG coi là JSON — bọc nguyên cả văn bản, không cắt mất phần rác")
    void json_kem_rac_phia_sau_duoc_boc_nguyen_van() throws Exception {
        String raw = "{\"result\":\"OK\"} rac-phia-sau";

        String wrapped = responses.json(raw);

        assertThat(wrapped).isNotEqualTo(raw);
        assertThat(mapper.readTree(wrapped).asText()).isEqualTo(raw);
    }

    @Test
    @DisplayName("Hai JSON dính nhau: Postgres jsonb từ chối nên phải bọc, và parse lại ra đúng văn bản gốc")
    void hai_json_dinh_nhau_duoc_boc() throws Exception {
        String raw = "{\"a\":1}{\"b\":2}";

        String wrapped = responses.json(raw);

        assertThat(mapper.readTree(wrapped).isTextual()).isTrue();
        assertThat(mapper.readTree(wrapped).asText()).isEqualTo(raw);
    }

    @Test
    @DisplayName("JSON cụt (provider cắt giữa chừng): bọc thành chuỗi, ký tự đặc biệt được escape đúng")
    void json_cut_va_ky_tu_dac_biet_duoc_escape() throws Exception {
        String raw = "{\"msg\":\"co \\\"nhay kep\\\" va xuong dong\n";

        String wrapped = responses.json(raw);

        assertThat(mapper.readTree(wrapped).asText()).isEqualTo(raw);
    }

    // ------------------------------------------------------------------
    // failureCode(code, fallback)
    // ------------------------------------------------------------------
    @ParameterizedTest(name = "[{index}] code=\"{0}\"")
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    @DisplayName("Mã lỗi thiếu/rỗng: dùng mã dự phòng")
    void ma_loi_thieu_thi_dung_du_phong(String code) {
        assertThat(ProviderResponses.failureCode(code, "PAYMENT_FAILED")).isEqualTo("PAYMENT_FAILED");
    }

    @Test
    @DisplayName("Mã lỗi vừa cột: giữ nguyên, kể cả đúng 32 ký tự")
    void ma_loi_vua_cot_giu_nguyen() {
        String exactly32 = "A".repeat(ProviderResponses.FAILURE_CODE_MAX);

        assertThat(ProviderResponses.failureCode("AMOUNT_LIMIT_EXCEEDED", "X")).isEqualTo("AMOUNT_LIMIT_EXCEEDED");
        assertThat(ProviderResponses.failureCode(exactly32, "X")).isEqualTo(exactly32).hasSize(32);
    }

    @Test
    @DisplayName("Mã lỗi dài quá cột (VARCHAR 32): cắt đúng 32 ký tự đầu, không nổ lỗi SQL")
    void ma_loi_qua_dai_bi_cat_o_32_ky_tu() {
        String longCode = "0123456789".repeat(5);   // 50 ký tự

        String result = ProviderResponses.failureCode(longCode, "X");

        assertThat(result).hasSize(32).isEqualTo(longCode.substring(0, 32));
    }

    @Test
    @DisplayName("Mã dự phòng cũng bị cắt nếu quá dài — cột chỉ có 32 ký tự cho mọi nguồn")
    void ma_du_phong_qua_dai_cung_bi_cat() {
        String longFallback = "F".repeat(40);

        assertThat(ProviderResponses.failureCode(null, longFallback)).hasSize(32);
    }
}
