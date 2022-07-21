package org.mdream.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.mdream.kafka.producer.JsonSerializer.OBJECT_MAPPER;

public class SimpleKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaProducer.class);
    private final KafkaProducer<Integer, StringValue> kafkaProducer;
    public static final String TOPIC_NAME = "simple-topic-1";

    public SimpleKafkaProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16386);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33_554_432); //byte
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1_000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(OBJECT_MAPPER, new ObjectMapper());

        kafkaProducer = new KafkaProducer<>(props);

        var shutdownHook = new Thread(() -> {
            logger.info("closing kafka producer");
            kafkaProducer.close();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public KafkaProducer<Integer, StringValue> getKafkaProducer() {
        return kafkaProducer;
    }
}
