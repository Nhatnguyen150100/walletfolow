package com.walletflow.bank;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling // job đối chiếu lần thu đang chờ
@EntityScan("com.walletflow") // nạp cả OutboxEvent/ProcessedEvent ở common-lib
@SpringBootApplication(scanBasePackages = {"com.walletflow.bank", "com.walletflow.common"})
public class BankAdapterApplication {
    public static void main(String[] args) {
        SpringApplication.run(BankAdapterApplication.class, args);
    }
}
