package org.mdream.kafka.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    public static final String OBJECT_MAPPER = "objectMapper";
    public static final String TYPE_REFERENCE = "typeReference";
    private final String encoding = StandardCharsets.UTF_8.name();
    private ObjectMapper mapper;
    private TypeReference<T> typeReference;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        mapper = (ObjectMapper) configs.get(OBJECT_MAPPER);
        if (mapper == null) {
            throw new IllegalArgumentException("config property OBJECT_MAPPER wasn't set");
        }
        typeReference = (TypeReference<T>) configs.get(TYPE_REFERENCE);
        if (typeReference == null) {
            throw new IllegalArgumentException("config property TYPE_REFERENCE wasn't set");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            if (bytes == null) {
                return null;
            } else {
                var valueAsString = new String(bytes, encoding);
                return mapper.readValue(valueAsString, typeReference);
            }
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to StringValue", e);
        }
    }
}
