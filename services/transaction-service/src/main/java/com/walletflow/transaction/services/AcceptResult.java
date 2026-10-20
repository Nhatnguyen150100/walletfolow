package com.walletflow.transaction.services;

import java.util.UUID;

/**
 * Câu trả lời cho một lần nhận lệnh tiền (chuyển hoặc nạp).
 *
 * <p>{@code body} là JSON NGUYÊN VĂN đã lưu ở lần đầu — lần gửi lại nhận về đúng từng byte, nên không
 * được serialize lại từ object (CLAUDE.md, ADR-013).
 */
public record AcceptResult(int status, String body, UUID transactionId, boolean replayed) {
}
