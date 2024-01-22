package io.github.ithamal.queue.handler;

import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.Message;

import java.util.Collection;
import java.util.Collections;

/**
 * @author: ken.lin
 * @since: 2023-09-28 15:46
 */
public abstract class MessageHandlerAdapter<T> implements MessageHandler<T> {

    public abstract void handle(Message<T> message);

    public void handle(Message<T> message, Consumer consumer) {
        handle(message);
        consumer.ack(Collections.singletonList(message.getId()));
    }

    @Override
    public void handle(Collection<Message<T>> messages, Consumer consumer) {
        for (Message<T> message : messages) {
            handle(message, consumer);
        }
    }

}
