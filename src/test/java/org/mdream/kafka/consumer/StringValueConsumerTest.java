package org.mdream.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mdream.kafka.producer.KafkaBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mdream.kafka.consumer.JsonSerializer.OBJECT_MAPPER;
import static org.mdream.kafka.consumer.SimpleKafkaConsumer.TOPIC_NAME;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class StringValueConsumerTest {
    public static final Logger logger = LoggerFactory.getLogger(StringValueConsumerTest.class);
    private static KafkaBase kafkaBase = new KafkaBase();

    @BeforeAll
    public static void init() throws ExecutionException, InterruptedException, TimeoutException {
        kafkaBase.start(List.of(new NewTopic(TOPIC_NAME, 1, (short) 1)));
    }

    @AfterAll
    public static void shutdown() throws Exception {
        kafkaBase.close();
    }

    @Test
    public void dataHandlerTest() {
        List<StringValue> stringValues = IntStream.range(0, 15).boxed()
                .map(idx -> new StringValue(idx, "test:" + idx))
                .toList();
        putValuesToKafka(stringValues);

        var consumer = new SimpleKafkaConsumer(kafkaBase.getBootstrapServers());

        List<StringValue> actualStringValues = new CopyOnWriteArrayList<>();
        var dataConsumer = new StringValueConsumer(consumer, actualStringValues::add);

        CompletableFuture.runAsync(dataConsumer::startConsuming);

        await().atMost(30, TimeUnit.SECONDS).until(() -> actualStringValues.size() == stringValues.size());
        assertThat(actualStringValues).hasSameElementsAs(stringValues);
        dataConsumer.stopConsuming();
    }

    private void putValuesToKafka(List<StringValue> stringValues) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaProducerTest");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBase.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(OBJECT_MAPPER, new ObjectMapper());

        logger.info("sending values, counter:{}", stringValues.size());
        try (var kafkaProducer = new KafkaProducer<Integer, StringValue>(properties)) {
            for (var value : stringValues) {
                kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, value.id(), value));
            }
        }
        logger.info("done");
    }
}
