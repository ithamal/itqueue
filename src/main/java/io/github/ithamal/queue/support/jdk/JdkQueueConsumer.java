
package io.github.ithamal.queue.support.jdk;


import io.github.ithamal.queue.core.AbstractConsumer;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.core.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;

/**
 * @author: ken.lin
 * @since: 2023-09-28 15:49
 */
public class JdkQueueConsumer extends AbstractConsumer {

    private final Queue<Message> queue;

    public JdkQueueConsumer(String name, ConsumerGroup group, Queue<Message> queue) {
        super(name, group);
        this.queue = queue;
    }

    @Override
    public Collection<Message<?>> poll(int size) {
        Collection<Message<?>> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Message message = queue.poll();
            if (message == null) {
                break;
            }
            list.add(message);
        }
        return list;
    }

    @Override
    public void ack(Collection<Long> ids) {

    }
}
