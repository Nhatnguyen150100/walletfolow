package com.walletflow.bank.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.event.BankChargeCommand;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.money.ECurrency;

import java.time.Instant;
import java.util.UUID;

/** Dựng lệnh thu và envelope đúng như transaction-service đặt lên topic. */
public final class BankFixtures {

    private BankFixtures() {
    }

    public static BankChargeCommand charge(UUID transactionId, UUID walletId, long amountMinor) {
        return new BankChargeCommand(transactionId, walletId, amountMinor, ECurrency.VND);
    }

    /** Lệnh thu mới hoàn toàn (giao dịch + ví ngẫu nhiên) — mỗi test tự có dữ liệu riêng. */
    public static BankChargeCommand freshCharge(long amountMinor) {
        return charge(UUID.randomUUID(), UUID.randomUUID(), amountMinor);
    }

    public static String envelopeJson(ObjectMapper objectMapper, UUID tenantId, BankChargeCommand command) {
        try {
            return objectMapper.writeValueAsString(EventEnvelope.of(
                    UUID.randomUUID(), "BankCharge", tenantId, null, command, Instant.now()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
