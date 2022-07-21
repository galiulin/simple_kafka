package org.mdream.kafka.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import static org.mdream.kafka.consumer.JsonDeserializer.OBJECT_MAPPER;
import static org.mdream.kafka.consumer.JsonDeserializer.TYPE_REFERENCE;


public class SimpleKafkaConsumer {

    private final KafkaConsumer<Integer, StringValue> kafkaConsumer;

    public static final String TOPIC_NAME = "simple-topic-1";
    public static final String GROUP_ID_CONFIG_NAME = "group_1";
    public static final int MAX_POLL_INTERVAL_MS = 300;

    public SimpleKafkaConsumer(String bootstrapServers) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG_NAME);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, makeGroupInstanceIdConfig());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(OBJECT_MAPPER, new ObjectMapper());
        props.put(TYPE_REFERENCE, new TypeReference<StringValue>() {
        });

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
    }

    public KafkaConsumer<Integer, StringValue> getConsumer() {
        return kafkaConsumer;
    }

    public String makeGroupInstanceIdConfig() {
        try {
            var hostName = InetAddress.getLocalHost().getHostName();
            return String.join("-", GROUP_ID_CONFIG_NAME, hostName, String.valueOf(new Random().nextInt(100)));
        } catch (Exception e) {
            throw new ConsumerException("can't make GroupInstanceIdConfig", e);
        }
    }

}
