package io.github.ithamal.queue.service;

import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.handler.MessageHandler;

import java.util.concurrent.TimeUnit;

/**
 * @author: ken.lin
 * @since: 2023-09-28 13:41
 */
public interface ConsumersContainer {

    void binding(ConsumerGroup consumerGroup, MessageHandler handler);

    void unbinding(ConsumerGroup consumerGroup, MessageHandler handler);

    void start();

    void shutdown();

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
}
