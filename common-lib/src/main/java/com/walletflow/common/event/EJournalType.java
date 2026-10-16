package com.walletflow.common.event;

public enum EJournalType {
    TRANSFER,
    TOPUP,
    WITHDRAW,
    REVERSAL // user for compensation of Saga
}
