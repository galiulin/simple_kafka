package org.mdream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Properties;

public class SimpleKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(SimpleKafkaProducer.class);
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%d", System.getenv().getOrDefault("KAFKA_REMOTE_HOST", "127.0.0.1"), 9092));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);) {
            for (int i = 0; i < 5000; i++) {
                ProducerRecord<Integer, String> record = new ProducerRecord<>("simple-topic-1", i % 4, "message=" + i);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("topic {} partition {} offsets {} time {} ",
                                metadata.topic(), metadata.partition(), metadata.offset(), new Timestamp(metadata.timestamp())
                        );
                    } else {
                        logger.error("producing error", exception);
                    }
                });
                Thread.sleep(300);
            }
            producer.flush();
        }
    }
}
