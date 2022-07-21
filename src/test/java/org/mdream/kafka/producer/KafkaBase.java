package org.mdream.kafka.producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

public class KafkaBase implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaBase.class);

    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"));
    private String bootstrapServers;

    public void start(Collection<NewTopic> topics) throws ExecutionException, InterruptedException, TimeoutException {
        kafka.start();
        bootstrapServers = kafka.getBootstrapServers();

        logger.info("topics creation...");
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            var result = admin.createTopics(topics);

            for (var topicResult : result.values().values()) {
                topicResult.get(10, TimeUnit.SECONDS);
            }
        }
        logger.info("topics created");
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @Override
    public void close() throws Exception {
        kafka.close();
    }
}
