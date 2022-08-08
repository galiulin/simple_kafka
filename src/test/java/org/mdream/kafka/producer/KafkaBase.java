package org.mdream.kafka.producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
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

    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"));
    private static volatile boolean started = false;

    public static void start(Collection<NewTopic> topics) throws ExecutionException, InterruptedException, TimeoutException {
        if (started) {
            return;
        }

        kafka.start();

        logger.info("topics creation...");
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            var result = admin.createTopics(topics);

            for (var topicResult : result.values().values()) {
                topicResult.get(10, TimeUnit.SECONDS);
            }
        }
        logger.info("topics created");
        started = true;
    }

    @Override
    public void close() throws Exception {
        kafka.close();
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            if (started) {
                TestPropertyValues.of("spring.kafka.bootstrap-servers:" + kafka.getBootstrapServers()).applyTo(applicationContext.getEnvironment());
            }
        }
    }

}
