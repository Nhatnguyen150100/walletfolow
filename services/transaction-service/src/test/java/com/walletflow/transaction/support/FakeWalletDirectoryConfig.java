package com.walletflow.transaction.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** Thay {@code HttpWalletDirectory} bằng {@link FakeWalletDirectory} trong test. */
@TestConfiguration
public class FakeWalletDirectoryConfig {

    @Bean
    @Primary
    public FakeWalletDirectory fakeWalletDirectory() {
        return new FakeWalletDirectory();
    }
}
