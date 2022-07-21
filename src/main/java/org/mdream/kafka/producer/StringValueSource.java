package org.mdream.kafka.producer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class StringValueSource implements ValueSource, AutoCloseable {
    private final AtomicInteger counter = new AtomicInteger(1);
    private final Consumer<StringValue> valueConsumer;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public StringValueSource(Consumer<StringValue> valueConsumer) {
        this.valueConsumer = valueConsumer;
    }

    @Override
    public void generate() {
        executorService.scheduleAtFixedRate(() -> valueConsumer.accept(makeValue()), 0, 1, TimeUnit.SECONDS);
    }

    private StringValue makeValue() {
        var id = counter.getAndIncrement();
        return new StringValue(id, "string value " + id);
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
