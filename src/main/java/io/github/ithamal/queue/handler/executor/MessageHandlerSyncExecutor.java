package io.github.ithamal.queue.handler.executor;

import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.handler.MessageHandler;
import org.springframework.scheduling.annotation.AsyncResult;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author: ken.lin
 * @since: 2023-09-28 14:18
 */
public class MessageHandlerSyncExecutor implements MessageHandlerExecutor {

    private final Collection<MessageHandler<?>> handlers;

    public MessageHandlerSyncExecutor(Collection<MessageHandler<?>> handlers) {
        this.handlers = handlers;
    }

    @Override
    public CompletableFuture<Void> handle(Collection<Message<?>> messages, Consumer consumer) {
        try {
            for (MessageHandler handler : handlers) {
                handler.handle(messages, consumer);
            }
            return AsyncResult.<Void>forValue(null).completable();
        } catch (Exception e) {
            return AsyncResult.<Void>forExecutionException(e).completable();
        }
    }
}
