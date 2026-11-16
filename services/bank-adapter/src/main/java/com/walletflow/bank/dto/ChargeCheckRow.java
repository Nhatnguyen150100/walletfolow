package com.walletflow.bank.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Một lệnh tiền nhìn từ phía provider, đã gộp lần THU với mọi lần HOÀN của nó — đơn vị so sánh của
 * đối soát bank-adapter ↔ sổ cái (roadmap 3.1 PR2a, ADR-024 §2).
 *
 * <p>Interface projection vì truy vấn native ({@code LEFT JOIN LATERAL} gộp các lần hoàn). Các cột
 * enum để dạng {@code String}, cùng lý do như {@link ProviderOperationRow}.
 */
public interface ChargeCheckRow {

    /** Id hàng THU — con trỏ phân trang của chiều xuôi. */
    UUID getChargeId();

    UUID getTenantId();

    UUID getTransactionId();

    String getCurrency();

    /**
     * Tiền ròng provider đã giữ cho lệnh này: số thu nếu lần thu {@code SUCCEEDED}, trừ tổng các lần
     * hoàn {@code SUCCEEDED}. Lần thu {@code FAILED} góp 0 — kể cả {@code FAILED(EXPIRED)} mà tiền về
     * muộn: ca đó đã có alert riêng {@code ExpiredChargeNeedsReview} và sổ của adapter chưa ghi nhận nó.
     */
    long getNetInMinor();

    /** Còn thao tác {@code NEW}/{@code PENDING} — kết quả chưa chốt, chưa so được. */
    boolean getInFlight();

    /** Lần đổi gần nhất của lần thu HOẶC bất kỳ lần hoàn nào — mốc cho cửa sổ ổn định. */
    Instant getLastChangedAt();
}
