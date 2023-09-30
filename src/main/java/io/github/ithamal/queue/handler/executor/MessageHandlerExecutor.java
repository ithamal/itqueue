package io.github.ithamal.queue.handler.executor;

import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author: ken.lin
 * @since: 2023-09-28 14:24
 */
public interface MessageHandlerExecutor {

    CompletableFuture<Void> handle(Collection<Message<?>> messages, Consumer consumer) ;
}
