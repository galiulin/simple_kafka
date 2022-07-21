package org.mdream.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.mdream.kafka.consumer.SimpleKafkaConsumer.MAX_POLL_INTERVAL_MS;

public class StringValueConsumer {
    private static final Logger logger = LoggerFactory.getLogger(StringValueConsumer.class);

    private final SimpleKafkaConsumer consumer;
    private final Duration timeout = Duration.ofMillis(2_000);
    private final Consumer<StringValue> dataConsumer;
    private final ScheduledExecutorService executrs = Executors.newScheduledThreadPool(1);

    public StringValueConsumer(SimpleKafkaConsumer consumer, Consumer<StringValue> dataConsumer) {
        this.consumer = consumer;
        this.dataConsumer = dataConsumer;
    }

    public void startConsuming() {
        executrs.scheduleAtFixedRate(this::poll, 0, MAX_POLL_INTERVAL_MS * 2L, TimeUnit.MILLISECONDS);
    }

    private void poll() {
        logger.info("poll records");
        ConsumerRecords<Integer, StringValue> records = consumer.getConsumer().poll(timeout);

        logger.info("polled record.counter:{}", records.count());
        for (ConsumerRecord<Integer, StringValue> record : records) {
            try {
                var key = record.key();
                var value = record.value();
                logger.info("key:{}, value:{}, record:{}", key, value, record);
                dataConsumer.accept(value);
            } catch (Exception e) {
                logger.error("can't parse record:{}", record, e);
            }
        }
    }

    public void stopConsuming() {
        executrs.shutdown();
    }

    private void sleep() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
