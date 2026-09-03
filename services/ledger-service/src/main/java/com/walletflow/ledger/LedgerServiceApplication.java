package com.walletflow.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

@EnableJpaAuditing
@EntityScan("com.walletflow") // nạp cả OutboxEvent/ProcessedEvent ở common-lib
@SpringBootApplication(scanBasePackages = {"com.walletflow.ledger", "com.walletflow.common"})
public class LedgerServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(LedgerServiceApplication.class, args);
    }
}
