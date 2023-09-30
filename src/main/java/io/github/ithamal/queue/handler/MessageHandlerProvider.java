package io.github.ithamal.queue.handler;

import java.util.List;

/**
 * @author: ken.lin
 * @since: 2023-09-29 23:46
 */
public interface MessageHandlerProvider {

    List<MessageHandler<?>> getHandlers();
}
