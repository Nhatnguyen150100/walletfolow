package com.walletflow.common.event;

public record Leg(
        String accountRef,
        long amountMinor
) {
}
