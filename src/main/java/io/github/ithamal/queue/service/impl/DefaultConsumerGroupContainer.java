package io.github.ithamal.queue.service.impl;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.handler.MessageHandler;
import io.github.ithamal.queue.handler.executor.MessageHandlerExecutor;
import io.github.ithamal.queue.handler.executor.MessageHandlerSyncExecutor;
import io.github.ithamal.queue.service.ConsumerGroupContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author: ken.lin
 * @since: 2023-09-28 09:46
 */
public class DefaultConsumerGroupContainer implements ConsumerGroupContainer {

    private final static Logger logger = LoggerFactory.getLogger(DefaultConsumerGroupContainer.class);

    private ScheduledThreadPoolExecutor consumeTaskPoolExecutor;

    private final ConsumerGroup consumerGroup;

    private final List<MessageHandler<?>> handlerList = new ArrayList<>();


    private final Object lock = new Object();

    public DefaultConsumerGroupContainer(ConsumerGroup consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    @Override
    public ConsumerGroup getConsumerGroup() {
        return consumerGroup;
    }

    @Override
    public void registerHandler(MessageHandler<?> handler) {
        if (handlerList.contains(handler)) {
            return;
        }
        handlerList.add(handler);
    }

    @Override
    public void unregister(MessageHandler<?> handler) {
        handlerList.remove(handler);
    }


    @Override
    public void start() {
        String groupName = consumerGroup.getSetting().getGroupName();
        int threads = consumerGroup.getSetting().getConsumerNum();
        ThreadFactory threadFactory = new CustomizableThreadFactory("consumer-group-" + groupName);
        consumeTaskPoolExecutor = new ScheduledThreadPoolExecutor(threads, threadFactory);
        for (Consumer consumer : consumerGroup.getConsumers()) {
            consumeTaskPoolExecutor.execute(new ConsumeTask(consumer));
        }
    }

    @Override
    public void shutdown() {
        synchronized (lock) {
            consumeTaskPoolExecutor.shutdownNow();
        }
    }


    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return consumeTaskPoolExecutor.awaitTermination(timeout, unit);
    }


    private class ConsumeTask implements Runnable {

        private final Consumer consumer;

        private final MessageHandlerExecutor messageHandlerExecutor;

        public ConsumeTask(Consumer consumer) {
            this.consumer = consumer;
            this.messageHandlerExecutor = new MessageHandlerSyncExecutor(handlerList);
        }

        /**
         * 拉取并消费消息
         */
        @Override
        public void run() {
            try {
                ConsumerSetting setting = consumerGroup.getSetting();
                Collection<Message<?>> messages = consumer.poll(setting.getPollSize());
                if (messages.isEmpty()) {
                    loop(false);
                } else {
                    CompletableFuture<Void> future = messageHandlerExecutor.handle(messages, consumer);
                    future.get();
                    loop(true);
                }
            } catch (Exception e) {
                logger.error("Occur exception during handle messages from consumer [" + consumer.getName() + "]", e);
                loop(false);
            }
        }

        private void loop(boolean immediate) {
            ConsumerSetting setting = consumerGroup.getSetting();
            if (setting.getPollInterval() > 0) {
                consumeTaskPoolExecutor.schedule(this, setting.getPollInterval(), TimeUnit.SECONDS);
            } else {
                if (immediate) {
                    consumeTaskPoolExecutor.execute(this);
                } else {
                    consumeTaskPoolExecutor.schedule(this, 1, TimeUnit.SECONDS);
                }
            }
        }
    }
}
