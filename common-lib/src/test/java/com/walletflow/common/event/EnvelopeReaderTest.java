package com.walletflow.common.event;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.walletflow.common.kafka.PoisonMessageException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phân loại message ở biên vào Kafka. Mọi message KHÔNG BAO GIỜ xử lý được phải ra
 * {@link PoisonMessageException} ngay tại đây, để đi thẳng DLT thay vì bị retry và chặn partition.
 */
class EnvelopeReaderTest {

  private final EnvelopeReader reader =
      new EnvelopeReader(JsonMapper.builder().findAndAddModules().build());

  private static final String TENANT = UUID.randomUUID().toString();

  @Test
  @DisplayName("Envelope đầy đủ: đọc được tenant và payload")
  void doc_duoc_envelope_day_du() {
    String json = envelope("\"" + TENANT + "\"", "{\"accountRef\":\"x\",\"currency\":\"VND\"}");

    EventEnvelope<OpenAccountCommand> envelope = reader.read(json, OpenAccountCommand.class);

    assertThat(envelope.tenantId()).hasToString(TENANT);
    assertThat(envelope.payload().accountRef()).isEqualTo("x");
  }

  @Test
  @DisplayName("Thiếu payload: poison ngay, không để handler nổ NullPointerException rồi bị retry")
  void thieu_payload_la_poison() {
    String json = envelope("\"" + TENANT + "\"", "null");

    assertThatThrownBy(() -> reader.read(json, OpenAccountCommand.class))
        .isInstanceOf(PoisonMessageException.class)
        .hasMessageContaining("thiếu payload");
  }

  @Test
  @DisplayName("Thiếu tenant: poison, không đoán tenant mặc định")
  void thieu_tenant_la_poison() {
    String json = envelope("null", "{\"accountRef\":\"x\",\"currency\":\"VND\"}");

    assertThatThrownBy(() -> reader.read(json, OpenAccountCommand.class))
        .isInstanceOf(PoisonMessageException.class)
        .hasMessageContaining("tenantId");
  }

  @Test
  @DisplayName("JSON hỏng: poison")
  void json_hong_la_poison() {
    assertThatThrownBy(() -> reader.read("{khong-phai-json", OpenAccountCommand.class))
        .isInstanceOf(PoisonMessageException.class);
  }

  private static String envelope(String tenantJson, String payloadJson) {
    return "{\"eventId\":\"" + UUID.randomUUID() + "\",\"eventType\":\"OpenAccount\",\"version\":1,"
        + "\"occurredAt\":\"2026-10-16T00:00:00Z\",\"tenantId\":" + tenantJson
        + ",\"traceId\":null,\"payload\":" + payloadJson + "}";
  }
}
