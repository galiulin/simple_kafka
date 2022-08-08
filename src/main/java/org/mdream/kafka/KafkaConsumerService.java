package org.mdream.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.mdream.kafka.producer.StringValue;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerService {
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaConfig.TOPIC_NAME, groupId = "springBootConsumerService")
    public void consume(String value) throws JsonProcessingException {
        StringValue result = objectMapper.readValue(value, StringValue.class);
        log.info("consumed: {}", result);
    }
}
