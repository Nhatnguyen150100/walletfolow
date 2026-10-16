package com.walletflow.common.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kiểm một message Kafka (chuỗi JSON thô, đúng như trên dây) với JSON Schema của topic đó —
 * {@code classpath:/schemas/<topic>.json} ở common-lib (roadmap 1.3).
 *
 * <p>Nằm trong test-jar của common-lib để MỌI service kiểm message của chính mình bằng CÙNG một bộ
 * schema: producer chứng minh thứ nó phát ra đúng hợp đồng, consumer chứng minh nó đọc được mọi thứ
 * hợp đồng cho phép.
 */
public final class EventSchemas {

    private static final JsonSchemaFactory FACTORY =
            JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, JsonSchema> CACHE = new ConcurrentHashMap<>();

    private EventSchemas() {
    }

    /** @return danh sách vi phạm; rỗng nghĩa là hợp lệ */
    public static Set<ValidationMessage> validate(String topic, String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            return schemaFor(topic).validate(node);
        } catch (IOException e) {
            throw new AssertionError("Message không phải JSON hợp lệ: " + json, e);
        }
    }

    /** @throws AssertionError liệt kê đủ mọi vi phạm, kèm message gốc */
    public static void assertValid(String topic, String json) {
        Set<ValidationMessage> violations = validate(topic, json);
        if (!violations.isEmpty()) {
            throw new AssertionError("Message trên " + topic + " vi phạm hợp đồng " + violations
                    + "\nMessage: " + json);
        }
    }

    private static JsonSchema schemaFor(String topic) {
        return CACHE.computeIfAbsent(topic, key -> {
            String path = "/schemas/" + key + ".json";
            try (InputStream stream = EventSchemas.class.getResourceAsStream(path)) {
                if (stream == null) {
                    throw new AssertionError("Không có schema cho topic " + key + " (" + path + ")");
                }
                return FACTORY.getSchema(stream);
            } catch (IOException e) {
                throw new AssertionError("Không đọc được " + path, e);
            }
        });
    }
}
