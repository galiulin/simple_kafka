package org.mdream.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DemoConsumer.class);

    public static void main(String[] args) {
        var consumer = new SimpleKafkaConsumer(String.format("%s:%d", System.getenv().getOrDefault("KAFKA_REMOTE_HOST", "127.0.0.1"), 9092));
        var dataConsumer = new StringValueConsumer(consumer, value -> logger.info("value:{}", value));
        dataConsumer.startConsuming();
    }
}
