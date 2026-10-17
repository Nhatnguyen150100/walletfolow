package com.walletflow.wallet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.contract.EventSchemas;
import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.event.EnvelopeReader;
import com.walletflow.common.event.EventEnvelope;
import com.walletflow.common.event.LedgerPostedEvent;
import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.event.WalletTopics;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.wallet.entity.Wallet;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.wallet.repositories.WalletEntryRepository;
import com.walletflow.wallet.repositories.WalletRepository;
import com.walletflow.wallet.services.WalletService;
import com.walletflow.wallet.support.Events;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Vòng đầy đủ qua Kafka thật: wallet-service gửi lệnh mở tài khoản đi, nhận câu trả lời và bút toán
 * của ledger về. Đóng vai ledger bằng producer tay — thứ được kiểm ở đây là mối nối (topic, envelope,
 * tenant, listener, outbox relay), không phải logic sổ cái.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.cloud.config.enabled=false",
        "outbox.relay.delay-ms=300",
        "walletflow.wallet.pending-resend.enabled=false",
        "management.tracing.enabled=false"
})
@Testcontainers
class WalletFlowIT {

    private static final UUID TENANT = TenantContext.DEFAULT_TENANT;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("walletdb");

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
    WalletService walletService;
    @Autowired
    WalletRepository walletRepository;
    @Autowired
    WalletEntryRepository entryRepository;
    @Autowired
    EnvelopeReader envelopeReader;
    @Autowired
    ObjectMapper objectMapper;

    @Test
    @DisplayName("Mở ví → lệnh ra Kafka → AccountOpened về → ACTIVE → LedgerPosted (giao 2 lần) → số dư đúng")
    void vong_day_du_qua_kafka() {
        UUID wallet = TenantContext.callWith(TENANT,
                () -> walletService.open(UUID.randomUUID(), ECurrency.VND).wallet().id());

        EventEnvelope<OpenAccountCommand> command =
                awaitMessage(WalletTopics.CMD_OPEN_ACCOUNT, wallet.toString(), OpenAccountCommand.class);
        assertThat(command.payload().accountRef()).isEqualTo(wallet.toString());
        assertThat(command.tenantId()).isEqualTo(TENANT);

        // Đóng vai ledger trả lời
        send(WalletTopics.EVT_ACCOUNT_OPENED, wallet.toString(), UUID.randomUUID(), "AccountOpened",
                Events.opened(wallet, EAccountStatus.ACTIVE, 0), TENANT);
        await(() -> statusOf(wallet) == EWalletStatus.ACTIVE);

        LedgerPostedEvent topUp = Events.topUp(wallet, 100_000);
        send(WalletTopics.EVT_LEDGER_POSTED, wallet.toString(), topUp.entryId(), "LedgerPosted", topUp, TENANT);
        send(WalletTopics.EVT_LEDGER_POSTED, wallet.toString(), topUp.entryId(), "LedgerPosted", topUp, TENANT);
        // Một bút toán KHÁC theo sau, cùng key nên cùng partition, xử lý sau bản lặp. Chờ nó được áp
        // là cách chắc chắn bản lặp đã được consumer xử lý — chờ "số dư = 100 000" thì đúng ngay từ
        // bản đầu, và assert có thể chạy trước khi bản lặp tới.
        LedgerPostedEvent marker = Events.topUp(wallet, 1);
        send(WalletTopics.EVT_LEDGER_POSTED, wallet.toString(), marker.entryId(), "LedgerPosted", marker, TENANT);

        await(() -> entryRepository.countByWalletId(wallet) == 2);
        assertThat(balanceOf(wallet)).as("giao hai lần vẫn chỉ cộng một lần").isEqualTo(100_001);
    }

    @Test
    @DisplayName("LedgerPosted thiếu tenant: vào DLT, không đoán tenant")
    void thieu_tenant_vao_dlt() {
        UUID wallet = UUID.randomUUID();
        LedgerPostedEvent event = Events.topUp(wallet, 1_000);

        send(WalletTopics.EVT_LEDGER_POSTED, wallet.toString(), event.entryId(), "LedgerPosted", event, null);

        String dead = awaitRawValue(WalletTopics.EVT_LEDGER_POSTED + WalletTopics.DLT_SUFFIX,
                event.entryId().toString());
        assertThat(dead).as("message nguyên văn nằm ở DLT chờ người xem").contains(wallet.toString());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------
    private void send(String topic, String key, UUID eventId, String type, Object payload, UUID tenant) {
        EventEnvelope<Object> envelope = EventEnvelope.of(eventId, type, tenant, null, payload, Instant.now());
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(topic, key, objectMapper.writeValueAsString(envelope))).get();
        } catch (Exception e) {
            throw new IllegalStateException("Không gửi được lên Kafka", e);
        }
    }

    private <T> EventEnvelope<T> awaitMessage(String topic, String marker, Class<T> payloadType) {
        String raw = awaitRawValue(topic, marker);
        // Contract test phía producer: lệnh THẬT wallet-service gửi lên Kafka phải đúng schema
        EventSchemas.assertValid(topic, raw);
        return envelopeReader.read(raw, payloadType);
    }

    private String awaitRawValue(String topic, String marker) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "wallet-flow-it-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                    if (record.value().contains(marker)) {
                        return record.value();
                    }
                }
            }
        }
        throw new AssertionError("Không thấy message chứa " + marker + " trên " + topic + " trong " + TIMEOUT);
    }

    private void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("Điều kiện không đạt trong " + TIMEOUT);
    }

    private long balanceOf(UUID wallet) {
        return walletRepository.findByTenantIdAndId(TENANT, wallet).map(Wallet::getBalanceMinor).orElse(-1L);
    }

    private EWalletStatus statusOf(UUID wallet) {
        return walletRepository.findByTenantIdAndId(TENANT, wallet).map(Wallet::getStatus).orElse(null);
    }
}
