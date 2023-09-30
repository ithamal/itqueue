package io.github.ithamal.queue.core;

import java.util.Collection;

/**
 * @author: ken.lin
 * @since: 2023-09-28 09:32
 */
public interface Producer {

    void put(Message message);

    void batchPut(Collection<Message> messages);
}
