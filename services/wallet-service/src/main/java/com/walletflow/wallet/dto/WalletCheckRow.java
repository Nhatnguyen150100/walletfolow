package com.walletflow.wallet.dto;

import java.util.UUID;

/**
 * Một ví cùng HAI con số cần đối chiếu, lấy trong một lượt quét (roadmap 3.1, ADR-023 §3).
 *
 * <p>Interface projection chứ không phải {@code record} như các DTO khác trong gói này: truy vấn
 * sinh ra nó là native (cần {@code LEFT JOIN} + {@code GROUP BY} để tổng {@code wallet_entries}),
 * và native query không dựng được record qua constructor như JPQL. Tên getter phải khớp alias cột.
 *
 * <p>Mang sẵn {@code tenantId} và {@code currency} vì job chạy XUYÊN tenant: nó phải gom ví theo
 * đúng cặp đó trước khi hỏi ledger, và phải xử lý từng ví trong {@code TenantContext} của chính ví.
 */
public interface WalletCheckRow {

    UUID getWalletId();

    UUID getTenantId();

    String getCurrency();

    String getStatus();

    /** Con số read model đang hiển thị cho người dùng. */
    long getBalanceMinor();

    /** Tổng lịch sử của chính ví này — vế thứ hai của phép kiểm 2a. */
    long getEntriesSumMinor();
}
