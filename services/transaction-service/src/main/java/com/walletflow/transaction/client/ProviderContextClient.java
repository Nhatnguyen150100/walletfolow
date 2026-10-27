package com.walletflow.transaction.client;

import com.walletflow.common.bank.ProviderContext;

import java.util.Optional;
import java.util.UUID;

/**
 * Đọc ngữ cảnh provider của một lệnh tiền từ bank-adapter (roadmap 2.5 PR3).
 */
public interface ProviderContextClient {

    /**
     * Các lần thu / lần hoàn của một lệnh.
     *
     * <p>{@code Optional.empty()} nghĩa là <b>KHÔNG HỎI ĐƯỢC</b> bank-adapter, không phải "chưa có
     * lần thu nào" — ca đó là một {@link ProviderContext} với danh sách rỗng. Cùng lý do như
     * {@link LedgerEntriesClient}: nhầm hai thứ này khi đang chữa cháy là cách làm mất tiền của
     * khách.
     */
    Optional<ProviderContext> contextOf(UUID transactionId);
}
