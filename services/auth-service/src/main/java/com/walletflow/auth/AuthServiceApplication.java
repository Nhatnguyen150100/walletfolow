package com.walletflow.auth;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

/**
 * scanBasePackages bao gồm "com.walletflow.common" để nạp GlobalExceptionHandler dùng chung.
 */
@EnableJpaAuditing
@SpringBootApplication(scanBasePackages = {"com.walletflow.auth", "com.walletflow.common"})
public class AuthServiceApplication {
  public static void main(String[] args) {
    SpringApplication.run(AuthServiceApplication.class, args);
  }
}
