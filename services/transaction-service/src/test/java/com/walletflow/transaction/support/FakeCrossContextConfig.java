package com.walletflow.transaction.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Thay {@code HttpLedgerEntriesClient} và {@code HttpProviderContextClient} bằng fake trong test
 * (2.5 PR3) — phụ thuộc HTTP sang service khác luôn thay bằng fake, không mock (CLAUDE.md).
 */
@TestConfiguration
public class FakeCrossContextConfig {

    @Bean
    @Primary
    public FakeLedgerEntriesClient fakeLedgerEntriesClient() {
        return new FakeLedgerEntriesClient();
    }

    @Bean
    @Primary
    public FakeProviderContextClient fakeProviderContextClient() {
        return new FakeProviderContextClient();
    }
}
