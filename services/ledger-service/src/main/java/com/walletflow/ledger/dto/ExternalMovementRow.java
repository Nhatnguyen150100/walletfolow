package com.walletflow.ledger.dto;

import java.util.UUID;

/**
 * Tiền ròng một lệnh tiền đã đưa qua {@code EXTERNAL_BANK} — nguồn của
 * {@code POST /internal/entries/external-movements} (roadmap 3.1 PR2a, ADR-024 §2).
 *
 * <p>Interface projection, cùng lý do như {@link ImbalanceRow}: truy vấn native vì phải
 * {@code JOIN accounts} để lấy {@code currency} và lọc theo {@code external_ref}.
 */
public interface ExternalMovementRow {

    UUID getTransactionId();

    String getCurrency();

    long getNetInMinor();
}
