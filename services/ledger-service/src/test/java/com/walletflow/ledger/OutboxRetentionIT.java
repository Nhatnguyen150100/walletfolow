package com.walletflow.ledger;

import com.walletflow.common.outbox.OutboxCleanupJob;
import com.walletflow.common.outbox.OutboxMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chứng minh hai tính chất của việc dọn dẹp outbox, và tính chất thứ hai là tính chất về an toàn
 * tiền.
 *
 * <ol>
 *   <li>Hàng ĐÃ GỬI quá hạn giữ thì bị xoá — nếu không, bảng outbox chỉ có lớn lên mãi và kéo chậm
 *       chính đường ghi của giao dịch.</li>
 *   <li><b>Hàng CHƯA GỬI thì KHÔNG BAO GIỜ bị xoá, cũ đến mấy cũng vậy.</b> Một hàng outbox chưa
 *       gửi là một message chưa đến đích: xoá nó là xoá một lệnh tiền chưa được thực thi, và không
 *       có cách nào biết điều đó đã xảy ra. Một job dọn dẹp viết sai điều kiện {@code WHERE} là
 *       cách mất tiền âm thầm nhất trong cả hệ thống — nên nó cần một test riêng.</li>
 * </ol>
 *
 * <p>Đồng thời kiểm chứng gauge {@code walletflow.outbox.oldest.age.seconds} thật sự phản ánh tồn
 * đọng: đây là chỉ số duy nhất phát hiện được relay đã chết, vì một relay chết khi tải thấp vẫn để
 * lại số lượng hàng chờ nhỏ và trông hoàn toàn bình thường.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        // Relay và job dọn KHÔNG tự chạy: test tự gọi để kết quả tất định
        "outbox.relay.delay-ms=3600000",
        "outbox.cleanup.delay-ms=3600000",
        "outbox.metrics.refresh-ms=3600000",
        "outbox.cleanup.retention-days=7",
        "spring.kafka.listener.auto-startup=false",
        "management.tracing.enabled=false"
})
@Testcontainers
class OutboxRetentionIT {

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("ledgerdb");

    @SuppressWarnings("resource")
    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    OutboxCleanupJob cleanupJob;
    @Autowired
    OutboxMetrics outboxMetrics;
    @Autowired
    MeterRegistry meterRegistry;

    @BeforeEach
    void clearOutbox() {
        jdbcTemplate.execute("DELETE FROM outbox_event");
    }

    // ------------------------------------------------------------------
    // ⭐ Dọn đúng thứ cần dọn, và KHÔNG đụng vào message chưa gửi
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Chỉ xoá event đã publish quá hạn; event chưa publish không bao giờ bị xoá dù rất cũ")
    void chi_xoa_event_da_publish_qua_han() {
        UUID daGuiQuaHan = insert(true, Instant.now().minus(Duration.ofDays(10)));
        UUID daGuiConHan = insert(true, Instant.now().minus(Duration.ofDays(1)));
        UUID chuaGuiRatCu = insert(false, null);

        cleanupJob.deleteOldPublished();

        assertThat(exists(daGuiQuaHan))
                .as("đã gửi + quá 7 ngày -> dọn")
                .isFalse();
        assertThat(exists(daGuiConHan))
                .as("đã gửi nhưng còn trong hạn giữ -> giữ lại làm bằng chứng đối soát")
                .isTrue();
        assertThat(exists(chuaGuiRatCu))
                .as("CHƯA GỬI = lệnh tiền chưa đến đích. Xoá nó là mất tiền âm thầm, "
                        + "bất kể nó cũ bao nhiêu")
                .isTrue();
    }

    // ------------------------------------------------------------------
    // Gauge phản ánh đúng tồn đọng
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Gauge pending và oldest-age phản ánh đúng outbox đang tồn đọng")
    void gauge_phan_anh_dung_ton_dong() {
        insert(true, Instant.now().minus(Duration.ofDays(1)));        // đã gửi -> không tính
        insertWithCreatedAt(false, Instant.now().minus(Duration.ofMinutes(30)));

        outboxMetrics.refresh();

        assertThat(gauge("walletflow.outbox.pending"))
                .as("chỉ đếm hàng CHƯA gửi")
                .isEqualTo(1.0);
        assertThat(gauge("walletflow.outbox.oldest.age.seconds"))
                .as("hàng chờ lâu nhất ~30 phút — đây là chỉ số phát hiện relay chết")
                .isBetween(1700.0, 1900.0);
    }

    @Test
    @DisplayName("Outbox rỗng thì oldest-age về 0, không giữ giá trị cũ")
    void outbox_rong_thi_gauge_ve_khong() {
        insertWithCreatedAt(false, Instant.now().minus(Duration.ofMinutes(30)));
        outboxMetrics.refresh();
        assertThat(gauge("walletflow.outbox.oldest.age.seconds")).isGreaterThan(0.0);

        jdbcTemplate.execute("DELETE FROM outbox_event");
        outboxMetrics.refresh();

        assertThat(gauge("walletflow.outbox.oldest.age.seconds"))
                .as("giữ giá trị cũ sẽ làm alert kêu mãi sau khi sự cố đã hết")
                .isZero();
        assertThat(gauge("walletflow.outbox.pending")).isZero();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private UUID insert(boolean published, Instant publishedAt) {
        return insert(published, publishedAt, Instant.now());
    }

    private UUID insertWithCreatedAt(boolean published, Instant createdAt) {
        return insert(published, null, createdAt);
    }

    private UUID insert(boolean published, Instant publishedAt, Instant createdAt) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                        INSERT INTO outbox_event
                            (id, tenant_id, created_at, updated_at, topic, msg_key, event_type,
                             payload, published, published_at, event_id)
                        VALUES (?, ?, ?, ?, 'test.topic.v1', 'KEY', 'TestEvent', '{}', ?, ?, ?)
                        """,
                id,
                com.walletflow.common.tenant.TenantContext.DEFAULT_TENANT,
                Timestamp.from(createdAt),
                Timestamp.from(createdAt),
                published,
                publishedAt == null ? null : Timestamp.from(publishedAt),
                UUID.randomUUID());
        return id;
    }

    private boolean exists(UUID id) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE id = ?", Integer.class, id);
        return count != null && count > 0;
    }

    private double gauge(String name) {
        return meterRegistry.get(name).gauge().value();
    }
}
