package com.walletflow.ledger.dto;

import com.walletflow.ledger.entity.Account;

public record ResolvedLeg(
        Leg leg,
        Account account
) {
}
