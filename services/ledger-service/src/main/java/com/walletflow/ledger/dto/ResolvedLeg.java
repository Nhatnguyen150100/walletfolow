package com.walletflow.ledger.dto;

import com.walletflow.common.event.Leg;

import com.walletflow.ledger.entity.Account;

public record ResolvedLeg(
        Leg leg,
        Account account
) {
}
