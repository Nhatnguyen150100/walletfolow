package com.walletflow.wallet.client;

import com.walletflow.common.ledger.AccountStatement;
import com.walletflow.common.money.ECurrency;

import java.util.Optional;

/**
 * Cổng đọc sổ cái — chỉ dùng khi DỰNG LẠI read model. Đường cập nhật bình thường là event Kafka.
 *
 * <p>Tách thành interface để test thay được bằng sổ cái giả, và để cách gọi (HTTP hôm nay, có thể
 * gRPC sau này) không rò vào logic rebuild.
 */
public interface LedgerClient {

    /**
     * Sao kê của tài khoản {@code accountRef} trong tenant hiện tại.
     *
     * @return rỗng nếu ledger chưa có tài khoản này (ví còn {@code PENDING}, lệnh mở chưa tới)
     */
    Optional<AccountStatement> statementOf(String accountRef, ECurrency currency);
}
