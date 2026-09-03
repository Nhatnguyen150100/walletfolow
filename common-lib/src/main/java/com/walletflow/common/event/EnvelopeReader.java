package com.walletflow.common.event;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
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
   * @throws IllegalArgumentException nếu JSON không đọc được — đây là poison message, người gọi nên
   *                                  để nó đi vào DLT chứ đừng retry (plan §12, bài toán P9)
   */
  public <T> EventEnvelope<T> read(String json, Class<T> payloadType) {
    JavaType type = objectMapper.getTypeFactory()
        .constructParametricType(EventEnvelope.class, payloadType);
    try {
      return objectMapper.readValue(json, type);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Message không đọc được thành EventEnvelope<" + payloadType.getSimpleName() + ">", e);
    }
  }
}
