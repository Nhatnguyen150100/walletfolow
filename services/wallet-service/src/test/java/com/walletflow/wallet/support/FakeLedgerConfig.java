package com.walletflow.wallet.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** Thay {@code HttpLedgerClient} bằng {@link FakeLedger} trong test. */
@TestConfiguration
public class FakeLedgerConfig {

    @Bean
    @Primary
    public FakeLedger fakeLedger() {
        return new FakeLedger();
    }
}
