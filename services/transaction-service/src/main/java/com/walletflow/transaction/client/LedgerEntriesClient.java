package com.walletflow.transaction.client;

import com.walletflow.common.ledger.TransactionEntries;

import java.util.Optional;
import java.util.UUID;

/**
 * Đọc bút toán của một lệnh tiền từ ledger (roadmap 2.5 PR3).
 *
 * <p>Sau một interface như {@link WalletDirectory}: test thay bằng fake, và chỗ duy nhất biết tới
 * HTTP là một class.
 */
public interface LedgerEntriesClient {

    /**
     * Bút toán của một lệnh.
     *
     * <p>{@code Optional.empty()} nghĩa là <b>KHÔNG HỎI ĐƯỢC</b> ledger, không phải "ledger không có
     * bút toán nào" — ca đó là một {@link TransactionEntries} với danh sách rỗng. Người trực phải
     * phân biệt được hai thứ: "tiền chưa vào sổ" dẫn tới hoàn tiền, "không biết tiền đã vào sổ chưa"
     * thì tuyệt đối không được hoàn.
     */
    Optional<TransactionEntries> entriesOf(UUID transactionId);
}
