package com.walletflow.bank.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.springframework.stereotype.Component;

/**
 * Chuẩn hoá câu trả lời của provider TRƯỚC khi ghi DB — một chỗ duy nhất.
 *
 * <p>Vì sao quan trọng: lúc ghi, provider ĐÃ trả lời (có thể đã tạo phiên, người dùng có thể đã trả tiền).
 * Một câu UPDATE hỏng vì mã lỗi dài quá cột, hay vì câu trả lời không phải JSON mà cột là {@code jsonb},
 * biến một câu trả lời hợp lệ thành lỗi kỹ thuật: lệnh thử lại rồi vào DLT, saga không bao giờ nghe được
 * gì trong khi phiên thanh toán vẫn tồn tại.
 */
@Component
public class ProviderResponses {

    /** Độ dài cột {@code failure_code} (V1). */
    static final int FAILURE_CODE_MAX = 32;

    private final ObjectMapper objectMapper;
    private final ObjectReader strictReader;

    public ProviderResponses(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.strictReader = objectMapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    /**
     * Câu trả lời thô → JSON hợp lệ cho cột {@code jsonb}. Không bao giờ trả {@code null}: tham số null của
     * câu native có thể bị bind thành {@code bytea}, và {@code CAST(bytea AS jsonb)} hỏng.
     * Không phải JSON (VNPay trả query string) thì bọc thành một chuỗi JSON — giữ nguyên văn để đối soát.
     */
    public String json(String raw) {
        if (raw == null || raw.isBlank()) {
            return "null";
        }
        try {
            strictReader.readTree(raw);
            return raw;
        } catch (JsonProcessingException notJson) {
            try {
                return objectMapper.writeValueAsString(raw);
            } catch (JsonProcessingException impossible) {
                throw new IllegalStateException("Không bọc được câu trả lời của provider thành JSON", impossible);
            }
        }
    }

    /** Mã lỗi vừa cột; thiếu thì dùng {@code fallback}. Mã gốc đầy đủ vẫn nằm trong {@code raw_response}. */
    public static String failureCode(String code, String fallback) {
        String value = code == null || code.isBlank() ? fallback : code;
        return value.length() <= FAILURE_CODE_MAX ? value : value.substring(0, FAILURE_CODE_MAX);
    }
}
