package io.github.ithamal.queue.service.impl;

import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.handler.MessageHandler;
import io.github.ithamal.queue.service.ConsumerGroupContainer;
import io.github.ithamal.queue.service.ConsumersContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author: ken.lin
 * @since: 2023-09-28 13:45
 */
public class DefaultConsumersContainer implements ConsumersContainer {

    private final static Logger logger = LoggerFactory.getLogger(DefaultConsumersContainer.class);

    private final ConcurrentHashMap<ConsumerGroup, ConsumerGroupContainer> containerMap = new ConcurrentHashMap<>();

    @Override
    public void binding(ConsumerGroup consumerGroup, MessageHandler handler) {
        ConsumerGroupContainer container = containerMap.computeIfAbsent(consumerGroup,
                it -> new DefaultConsumerGroupContainer(consumerGroup));
        container.registerHandler(handler);
        logger.info("The message handler '{}' has been registered", handler);
    }

    @Override
    public void unbinding(ConsumerGroup consumerGroup, MessageHandler handler) {
        ConsumerGroupContainer container = containerMap.computeIfAbsent(consumerGroup,
                it -> new DefaultConsumerGroupContainer(consumerGroup));
        container.unregister(handler);
    }

    @Override
    public void start() {
        for (ConsumerGroupContainer container : containerMap.values()) {
            try {
                container.start();
            } catch (Exception e) {
                String groupName = container.getConsumerGroup().getName();
                logger.error("An exception occurred during the startup of consumer container '"+ groupName +"'", e);
            }
        }
    }

    @Override
    public void shutdown() {
        for (ConsumerGroupContainer container : containerMap.values()) {
            try {
                container.shutdown();
            } catch (Exception e) {
                String groupName = container.getConsumerGroup().getName();
                logger.error("An exception occurred during the shutdown of consumer container '"+ groupName +"'", e);
            }
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = false;
        for (ConsumerGroupContainer container : containerMap.values()) {
            try {
                result = result || container.awaitTermination(timeout, unit);
            } catch (Exception e) {
                String groupName = container.getConsumerGroup().getName();
                logger.error("An exception occurred during the termination of consumer container '"+ groupName +"'", e);
            }
        }
        return result;
    }
}
