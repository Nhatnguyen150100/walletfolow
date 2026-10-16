package com.walletflow.common.kafka;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Cấu hình cho hai cơ chế an toàn của tầng messaging: khai báo topic tường minh và xử lý lỗi + DLT.
 *
 * <p>Giá trị mặc định ở đây hợp cho <b>máy dev một broker</b>. Môi trường thật phải ghi đè
 * {@code replication-factor} (tối thiểu 3) — xem {@code docs/walletflow-production-roadmap.md} §4.2.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "walletflow.kafka")
public class WalletflowKafkaProperties {

  /**
   * Có tự tạo các topic của hệ thống lúc khởi động không.
   *
   * <p>Bật ở dev để một lệnh {@code docker compose up} là có đủ topic. Ở môi trường thật nên TẮT và
   * tạo topic bằng công cụ vận hành: khi đó quyền tạo topic không nằm trong tay application, và cấu
   * hình topic (retention, replication, min.insync.replicas) là quyết định của hạ tầng chứ không
   * phải hiệu ứng phụ của một lần deploy.
   */
  private boolean declareTopics = true;

  /** Số partition cho mỗi topic. Partition key là walletId nên đây là mức song song hoá theo ví. */
  private int partitions = 3;

  /** Một broker ở local thì chỉ có thể là 1. Môi trường thật: 3. */
  private short replicationFactor = 1;

  private final Retry retry = new Retry();

  /**
   * Chính sách thử lại trước khi đẩy message sang DLT.
   *
   * <p>Mặc định ~0,5s → 8s qua 5 lần, tổng cộng khoảng 15 giây. Đủ để vượt qua một lần restart DB
   * hoặc một nhịp rebalance, nhưng không đủ lâu để giữ consumer khỏi tiến lên — giữ quá lâu là tự
   * tạo ra độ trễ cho mọi message xếp sau trên cùng partition.
   */
  @Getter
  @Setter
  public static class Retry {
    private int maxAttempts = 5;
    private long initialIntervalMs = 500;
    private double multiplier = 2.0;
    private long maxIntervalMs = 8_000;
  }
}
