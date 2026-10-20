package com.walletflow.transaction.services;

import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.outbox.OutboxPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Nơi DUY NHẤT dựng lệnh thu tiền — dùng cho cả lần gửi đầu (nhận lệnh nạp) lẫn các lần gửi lại của
 * job quét saga treo, cùng lý do như {@link LedgerCommandSender}.
 */
@Component
@RequiredArgsConstructor
public class BankCommandSender {

    private final OutboxPublisher outboxPublisher;

    /**
     * Xếp lệnh thu tiền vào outbox, trong transaction của người gọi.
     *
     * <p>Khác {@link LedgerCommandSender#sendTransfer}: gửi lại dùng eventId MỚI nhưng bank-adapter
     * idempotent theo {@code (tenant, transactionId)} và luôn <b>phát lại câu trả lời</b> ứng với
     * trạng thái hiện tại (ADR-016) — nên gửi lại không tạo phiên thanh toán thứ hai, mà còn là cách
     * duy nhất lấy lại một kết quả đã mất trên đường. Đây là lý do job quét được phép gửi lại bước
     * này mà không sợ thu tiền hai lần.
     *
     * @param clientIp IP người dùng lúc bấm nạp — provider dùng để chống gian lận (ADR-017). Là dữ
     *                 liệu cá nhân: truyền đi nhưng KHÔNG log.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void sendCharge(UUID transactionId, UUID walletId, long amountMinor, ECurrency currency,
                           String clientIp, UUID eventId) {
        outboxPublisher.publish(
                WalletTopics.CMD_BANK_CHARGE,
                // Key = ví được nạp: mọi lệnh của một ví đi cùng partition, đúng thứ tự (WalletTopics)
                walletId.toString(),
                eventId,
                "BankCharge",
                new BankChargeCommand(transactionId, walletId, amountMinor, currency, clientIp));
    }
}
