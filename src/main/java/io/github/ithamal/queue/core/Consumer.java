package io.github.ithamal.queue.core;

import java.util.Collection;

/**
 * @author: ken.lin
 * @since: 2023-09-28 09:30
 */
public interface Consumer {

    String getName();

    ConsumerGroup getGroup();

    Collection<Message<?>> poll(int size);

    void ack(Collection<Long> ids);
}
