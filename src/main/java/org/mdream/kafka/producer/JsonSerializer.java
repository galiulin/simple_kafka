package org.mdream.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);

    public static final String OBJECT_MAPPER = "objectMapper";
    private final String encoding = StandardCharsets.UTF_8.name();
    private ObjectMapper mapper;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        mapper = (ObjectMapper) configs.get(OBJECT_MAPPER);
        if (mapper == null) {
            throw new IllegalArgumentException("config property OBJECT_MAPPER wasn't set");
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            if (data == null) {
                return null;
            } else {
                String serializedString = mapper.writeValueAsString(data);
                logger.info("serialized data={}", serializedString);
                return serializedString.getBytes(encoding);
            }
        } catch (Exception e) {
            throw new SerializationException("error when serializing StringValue to byte[]", e);
        }
    }
}
