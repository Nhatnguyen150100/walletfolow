package com.walletflow.common.event;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.walletflow.common.kafka.PoisonMessageException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Đọc {@link EventEnvelope} từ chuỗi JSON mà consumer nhận được trên Kafka.
 *
 * <p>Vì sao parse tường minh thay vì để {@code JsonDeserializer} tự làm: consumer sẽ phải khai kiểu
 * payload qua header {@code __TypeId__} của producer hoặc qua cấu hình
 * {@code spring.json.value.default.type}. Cả hai đều biến tên class Java của producer thành một
 * phần của hợp đồng message giữa hai service — đúng thứ mà kiến trúc microservice cần tránh, và chỉ
 * nổ lúc runtime khi ai đó đổi tên class. Ở đây consumer tự khai kiểu payload NÓ muốn đọc, bằng
 * code, kiểm tra được lúc biên dịch.
 */
@Component
@RequiredArgsConstructor
public class EnvelopeReader {

  private final ObjectMapper objectMapper;

  /**
   * @param json        thân message thô trên Kafka
   * @param payloadType kiểu payload mà consumer này muốn đọc
   * @throws PoisonMessageException nếu JSON không đọc được, hoặc thiếu {@code tenantId}. Cả hai đều
   *                                là lỗi KHÔNG BAO GIỜ tự khỏi khi thử lại, nên
   *                                {@code KafkaErrorHandlingAutoConfiguration} khai chúng là
   *                                non-retryable → message đi thẳng DLT thay vì chặn cả partition
   *                                (plan §12, bài toán P9).
   */
  public <T> EventEnvelope<T> read(String json, Class<T> payloadType) {
    JavaType type = objectMapper.getTypeFactory()
        .constructParametricType(EventEnvelope.class, payloadType);

    EventEnvelope<T> envelope;
    try {
      envelope = objectMapper.readValue(json, type);
    } catch (Exception e) {
      throw new PoisonMessageException(
          "Message không đọc được thành EventEnvelope<" + payloadType.getSimpleName() + ">", e);
    }

    // Thiếu tenantId là message KHÔNG XỬ LÝ ĐƯỢC, không phải message dùng tenant mặc định.
    // Đoán tenant cho một lệnh tiền là cách ghi dữ liệu của khách này vào sổ của khách khác.
    if (envelope.tenantId() == null) {
      throw new PoisonMessageException(
          "Message thiếu tenantId (eventId=" + envelope.eventId()
              + ", eventType=" + envelope.eventType() + "). Không đoán tenant cho lệnh tiền.");
    }

    return envelope;
  }
}
