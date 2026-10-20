package com.walletflow.bank.support;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Producer/consumer Kafka thô cho IT: gửi lệnh vào đúng như saga làm, và đọc event/DLT đúng như
 * consumer phía sau thấy. Mỗi lần đọc dùng group mới + đọc từ đầu topic và lọc theo "marker"
 * (thường là transactionId) — test không phụ thuộc offset của test khác và không cần ngủ để chờ:
 * chỉ poll tới hạn chót.
 */
public final class BankKafka {

    public static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final String bootstrapServers;

    public BankKafka(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void send(String topic, String key, String value) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Không gửi được message lên " + topic, e);
        }
    }

    /** Message đầu tiên trên {@code topic} có chứa {@code marker}; hết hạn chót thì AssertionError. */
    public ConsumerRecord<String, String> await(String topic, String marker) {
        return awaitMatching(topic, marker, 1).get(0);
    }

    /**
     * Chờ cho tới khi có ít nhất {@code count} message chứa {@code marker}, trả về tất cả những gì đã thấy
     * (theo thứ tự đọc). Dùng khi cần chứng minh "phát lại cùng eventId".
     */
    public List<ConsumerRecord<String, String>> awaitMatching(String topic, String marker, int count) {
        return awaitMatching(topic, value -> value.contains(marker), marker, count);
    }

    /**
     * Như trên nhưng lọc bằng điều kiện tuỳ ý — cần khi một topic chở nhiều loại message của CÙNG giao dịch
     * (completed của lần thu và của lần hoàn đều chứa transactionId).
     */
    public List<ConsumerRecord<String, String>> awaitMatching(String topic, java.util.function.Predicate<String> filter,
                                                              String description, int count) {
        List<ConsumerRecord<String, String>> found = new java.util.ArrayList<>();
        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline && found.size() < count) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (filter.test(record.value())) {
                        found.add(record);
                    }
                }
            }
        }
        if (found.size() < count) {
            throw new AssertionError("Chờ " + count + " message chứa '" + description + "' trên " + topic
                    + " trong " + TIMEOUT + " nhưng chỉ thấy " + found.size());
        }
        return found;
    }

    public static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private KafkaConsumer<String, String> consumer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "bank-it-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(config);
    }
}
