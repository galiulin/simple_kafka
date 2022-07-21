package org.mdream.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static org.mdream.kafka.producer.SimpleKafkaProducer.TOPIC_NAME;

public class DataSender {
    private static final Logger logger = LoggerFactory.getLogger(DataSender.class);
    private final Consumer<StringValue> sendAsk;
    private final SimpleKafkaProducer producer;

    public DataSender(SimpleKafkaProducer producer, Consumer<StringValue> sendAsk) {
        this.sendAsk = sendAsk;
        this.producer = producer;
    }

    public void dataHandler(StringValue value) {
        logger.info("value {}", value);
        try {
            producer.getKafkaProducer().send(new ProducerRecord<>(TOPIC_NAME, value.id(), value),
                    (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("message wasn't send", exception);
                        } else {
                            logger.info("message id:{} was send, offset:{}", value.id(), metadata.offset());
                            sendAsk.accept(value);
                        }
                    });
        } catch (Exception exception) {
            logger.error(exception.getMessage(), exception);
        }
    }
}
