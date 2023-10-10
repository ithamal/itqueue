package io.github.ithamal.queue.boot;

import io.github.ithamal.queue.annotation.MessageHandlerBind;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.handler.MessageHandlerAdapter;
import org.springframework.stereotype.Component;

/**
 * @author: ken.lin
 * @since: 2023-09-30 00:06
 */
@Component
@MessageHandlerBind(queues = "test")
public class TestMessageHandler extends MessageHandlerAdapter<String> {

    @Override
    public void handle(Message<String> message) {
        System.out.println(message);
    }
}
