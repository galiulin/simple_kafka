package org.mdream.kafka.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class DataSenderTest {

    private static KafkaBase kafkaBase = new KafkaBase();

    @BeforeAll
    public static void init() throws ExecutionException, InterruptedException, TimeoutException {
        kafkaBase.start(List.of(new NewTopic(SimpleKafkaProducer.TOPIC_NAME, 1, (short) 1)));
    }

    @AfterAll
    public static void shutdown() throws Exception {
        kafkaBase.close();
    }

    @Test
    public void dataHandlerTest() {
        List<StringValue> stringValues = IntStream.range(0, 10).boxed()
                .map(idx -> new StringValue(idx, "test:" + idx))
                .toList();

        var myProducer = new SimpleKafkaProducer(kafkaBase.getBootstrapServers());

        List<StringValue> factStringValues = new CopyOnWriteArrayList<>();
        var dataProducer = new DataSender(myProducer, factStringValues::add);
        var valueSource = new ValueSource() {
            @Override
            public void generate() {
                for (int i = 0; i < stringValues.size(); i++) {
                    dataProducer.dataHandler(stringValues.get(i));
                }
            }
        };

        valueSource.generate();

        await().atMost(60, TimeUnit.SECONDS).until(() -> factStringValues.size() == stringValues.size());
        assertThat(factStringValues).hasSameElementsAs(stringValues);
    }
}
