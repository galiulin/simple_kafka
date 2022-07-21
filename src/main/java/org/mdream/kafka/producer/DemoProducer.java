package org.mdream.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoProducer {
    private static final Logger log = LoggerFactory.getLogger(DemoProducer.class);

    public static void main(String[] args) {
        var producer = new SimpleKafkaProducer(String.format("%s:%d", System.getenv().getOrDefault("KAFKA_REMOTE_HOST", "127.0.0.1"), 9092));

        var dataProducer = new DataSender(producer, stringValue -> log.info("asked, value:{}", stringValue));
        try (var valueSource = new StringValueSource(dataProducer::dataHandler)) {
            valueSource.generate();
            Thread.sleep(1000 * 30);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
