package io.github.ithamal.queue.support.jdk;

import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.core.Producer;

import java.util.Collection;
import java.util.Queue;

/**
 * @author: ken.lin
 * @since: 2023-09-28 15:49
 */
public class JdkQueueProducer implements Producer {

    private final Queue<Message> queue;

    public JdkQueueProducer(Queue<Message> queue) {
        this.queue = queue;
    }

    @Override
    public void put(Message message) {
        queue.add(message);
    }

    @Override
    public void batchPut(Collection<Message> messages) {
        queue.addAll(messages);
    }
}
