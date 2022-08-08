package org.mdream.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static org.mdream.kafka.KafkaConfig.TOPIC_NAME;

@Slf4j
@RequiredArgsConstructor
@Service
public class ProducerService {
    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void send(StringValue value) {
        log.info("value {}", value);
        try {
            String message = objectMapper.writeValueAsString(value);
            kafkaTemplate.send(TOPIC_NAME, value.id(), message);
            log.info("produced: {}", message);
        } catch (JsonProcessingException e) {
            throw new SerializationException("can't serialize message:" + value, e);
        }
    }
}
