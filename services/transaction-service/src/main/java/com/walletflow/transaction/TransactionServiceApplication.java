package com.walletflow.transaction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableJpaAuditing
@EnableScheduling // job quét saga treo
@EntityScan("com.walletflow") // nạp cả OutboxEvent/ProcessedEvent ở common-lib
@SpringBootApplication(scanBasePackages = {"com.walletflow.transaction", "com.walletflow.common"})
public class TransactionServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(TransactionServiceApplication.class, args);
    }
}
