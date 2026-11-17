package com.walletflow.ledger.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Một bút toán {@code TOPUP} trong đường quét xuyên tenant — nguồn của
 * {@code GET /internal/entries/topups} (roadmap 3.1 PR2a, ADR-024 §3).
 *
 * <p>Interface projection vì truy vấn native: so sánh theo bộ {@code (created_at, id) > (...)} và
 * literal {@code type = 'TOPUP'} phải khớp đúng vị từ của partial index {@code idx_journal_topup_created}
 * (V9) thì planner mới dùng nó.
 */
public interface TopupEntryRow {

    UUID getTenantId();

    UUID getEntryId();

    UUID getTransactionId();

    Instant getPostedAt();
}
