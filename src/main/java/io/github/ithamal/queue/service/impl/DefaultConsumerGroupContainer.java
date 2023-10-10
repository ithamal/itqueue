package io.github.ithamal.queue.service.impl;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.config.MessageHandlerSetting;
import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.handler.MessageHandler;
import io.github.ithamal.queue.handler.executor.MessageHandlerConcurrentExecutor;
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

    private ThreadPoolExecutor threadPoolExecutor;

    private final ConsumerGroup consumerGroup;

    private final List<MessageHandler<?>> handlerList = new ArrayList<>();


    private final Object lock = new Object();

    private MessageHandlerExecutor handlerExecutor;

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
        MessageHandlerSetting handlerSetting = consumerGroup.getSetting().getHandler();
        if (handlerSetting == null || handlerSetting.getThreads() <= 0) {
            this.handlerExecutor = new MessageHandlerSyncExecutor(handlerList);
        } else {
            int threads = handlerSetting.getThreads();
            String threadPrefix = "message-handler-" + groupName;
            this.handlerExecutor = new MessageHandlerConcurrentExecutor(threadPrefix, handlerList, threads);
        }
        int minThreads = consumerGroup.getSetting().getMinThreads();
        int maxThreads = consumerGroup.getSetting().getMaxThreads();
        ThreadFactory threadFactory = new CustomizableThreadFactory("consumer-group-" + groupName);
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        threadPoolExecutor = new ThreadPoolExecutor(minThreads, maxThreads, 10, TimeUnit.SECONDS, queue, threadFactory);
        for (Consumer consumer : consumerGroup.getConsumers()) {
            threadPoolExecutor.execute(new ConsumeTask(consumer));
        }
    }

    @Override
    public void shutdown() {
        synchronized (lock) {
            threadPoolExecutor.shutdownNow();
        }
    }


    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return threadPoolExecutor.awaitTermination(timeout, unit);
    }


    private class ConsumeTask implements Runnable {

        private final Consumer consumer;

        public ConsumeTask(Consumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            ConsumerSetting setting = consumerGroup.getSetting();
            Collection<Message<?>> messages = consumer.poll(setting.getPollSize());
            CompletableFuture<Void> future = handlerExecutor.handle(messages, consumer);
            future.thenApply(r -> {
                rePushTask();
                return null;
            }).exceptionally(e -> {
                logger.error("Occur exception when consumer [" + consumer.getName() + "] handling", e);
                rePushTask();
                return null;
            });
            try {
                if (!setting.getHandleAsync()) {
                    future.get();
                }
            } catch (Exception e) {
                logger.error("Occur exception when consumer [" + consumer.getName() + "] handling", e);
            }
        }

        private void rePushTask() {
            synchronized (lock) {
                if (!threadPoolExecutor.isShutdown() && !threadPoolExecutor.isTerminating()) {
                    threadPoolExecutor.execute(this);
                }
            }
        }
    }
}
