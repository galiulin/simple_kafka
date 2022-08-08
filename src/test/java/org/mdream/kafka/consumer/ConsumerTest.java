package org.mdream.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.assertj.core.util.Streams;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mdream.kafka.KafkaConfig;
import org.mdream.kafka.producer.ProducerService;
import org.mdream.kafka.producer.KafkaBase;
import org.mdream.kafka.producer.StringValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mdream.kafka.KafkaConfig.TOPIC_NAME;

@Slf4j
@SpringBootTest
@ContextConfiguration(initializers = {KafkaBase.Initializer.class})
public class ConsumerTest {

    @Autowired
    private ProducerService dataSender;

    @Autowired
    private ConsumerFactory<Integer, String> consumerFactory;

    @Autowired
    private ObjectMapper mapper;

    @BeforeAll
    public static void init() throws ExecutionException, InterruptedException, TimeoutException {
        KafkaBase.start(List.of(new KafkaConfig().topic()));
    }

    @Test
    public void sendAndReadTest() {
        StringValue value1 = new StringValue(4, "ttttest");
        StringValue value2 = new StringValue(5, "tttttest");
        StringValue value3 = new StringValue(6, "ttttttest");
        List<StringValue> expectValues = List.of(value1, value2, value3);
        expectValues.forEach(dataSender::send);

        Consumer<Integer, String> consumer = consumerFactory.createConsumer(TOPIC_NAME, "consumer");
        consumer.subscribe(List.of(TOPIC_NAME));
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(10_000));

        List<StringValue> stringValues = Streams.stream(records)
                .peek(v -> log.info("consumed test value: {}, key: {}", v.value(), v.key()))
                .map(this::parse).toList();
        assertThat(stringValues).hasSameElementsAs(expectValues);

    }

    private StringValue parse(ConsumerRecord<Integer, String> record) {
        try {
            return mapper.readValue(record.value(), StringValue.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
