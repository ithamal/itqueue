package io.github.ithamal.queue.handler.executor;

import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.handler.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: ken.lin
 * @since: 2023-09-28 14:26
 */

public class MessageHandlerConcurrentExecutor implements MessageHandlerExecutor {

    private final static Logger logger = LoggerFactory.getLogger(MessageHandlerConcurrentExecutor.class);

    private final Collection<MessageHandler<?>> handlers;

    private final ThreadPoolExecutor threadPoolExecutor;

    public MessageHandlerConcurrentExecutor(String threadNamePrefix, Collection<MessageHandler<?>> handlers, int threads) {
        this.handlers = handlers;
        ThreadFactory threadFactory = new CustomizableThreadFactory(threadNamePrefix);
        this.threadPoolExecutor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(), threadFactory);
    }

    @Override
    public CompletableFuture<Void> handle(Collection<Message<?>> messages, Consumer consumer) {
        if (this.threadPoolExecutor.getCorePoolSize() > 1) {
            double partitions = (double) this.threadPoolExecutor.getCorePoolSize();
            SettableListenableFuture<Void> future = new SettableListenableFuture<>();
            AtomicInteger counter = new AtomicInteger();
            long size = (long) Math.ceil(messages.size() / partitions);
            for (int i = 0; i < partitions; i++) {
                List<Message> subMessages = messages.stream().skip(i * size).limit(size).collect(Collectors.toList());
                threadPoolExecutor.execute(new Task(subMessages, consumer, counter, (int) partitions, future));
            }
            return future.completable();
        } else {
            return CompletableFuture.runAsync(() -> {
                for (MessageHandler handler : handlers) {
                    handler.handle(messages, consumer);
                }
            });
        }
    }

    public class Task implements Runnable {

        private final Collection<Message> messages;

        private final Consumer consumer;

        private final AtomicInteger counter;

        private final int completeCount;

        private final SettableListenableFuture<Void> future;

        public Task(Collection<Message> messages, Consumer consumer, AtomicInteger counter, int completeCount,
                    SettableListenableFuture<Void> future) {
            this.messages = messages;
            this.consumer = consumer;
            this.counter = counter;
            this.completeCount = completeCount;
            this.future = future;
        }

        @Override
        public void run() {
            try {
                for (MessageHandler handler : handlers) {
                    handler.handle(messages, consumer);
                }
            } catch (Exception e) {
                logger.error("消息并发处理异常:" + consumer, e);
            } finally {
                if (counter.incrementAndGet() == completeCount) {
                    future.set(null);
                }
            }
        }
    }


}
