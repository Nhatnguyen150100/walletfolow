package com.walletflow.ledger.dto;

public record Leg(
        String accountRef,
        long amountMinor
) {
}
