package io.github.ithamal.queue.handler;

import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.Message;

import java.util.Collection;

/**
 * @author: ken.lin
 * @since: 2023-09-28 09:38
 */
public interface MessageHandler<T> {

    void handle(Collection<Message<T>> messages, Consumer consumer);
}
