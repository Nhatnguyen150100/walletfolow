package com.walletflow.wallet.client;

import com.walletflow.common.ledger.AccountStatement;
import com.walletflow.common.money.ECurrency;

import java.util.List;
import java.util.Map;
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

    /**
     * Số dư của NHIỀU tài khoản cùng loại tiền trong tenant hiện tại — đường đọc của đối soát tầng 2
     * (roadmap 3.1, ADR-023 §3).
     *
     * <p>Tách khỏi {@link #statementOf} vì câu hỏi khác nhau: đối soát hỏi "số dư bao nhiêu" cho
     * hàng nghìn ví mỗi đêm, và tải toàn bộ lịch sử từng ví để rồi cộng lại là trả giá bằng băng
     * thông cho thứ Postgres cộng sẵn được.
     *
     * <p>Ref không có tài khoản ở ledger thì KHÔNG có mặt trong map trả về — khác hẳn với "số dư 0",
     * và bên gọi phải phân biệt được hai thứ đó (một cái là ví chưa mở xong, cái kia là ví giữ tiền
     * không có trong sổ cái).
     *
     * @param refs tối đa {@code AccountBalancesRequest.MAX_REFS} mỗi lượt — người gọi tự chia lô
     * @return map {@code accountRef -> số dư}, chỉ gồm những ref ledger thực sự có tài khoản
     */
    Map<String, Long> balancesOf(ECurrency currency, List<String> refs);
}
