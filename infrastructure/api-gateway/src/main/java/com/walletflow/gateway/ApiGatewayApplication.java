package com.walletflow.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * {@code @EnableScheduling} để {@code JwksKeyLocator} làm mới bộ khoá công khai theo lịch —
 * điều kiện để xoay khoá ký không gây downtime.
 */
@EnableScheduling
@SpringBootApplication
public class ApiGatewayApplication {
  public static void main(String[] args) {
    SpringApplication.run(ApiGatewayApplication.class, args);
  }
}
