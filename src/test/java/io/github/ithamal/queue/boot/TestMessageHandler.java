package io.github.ithamal.queue.boot;

import io.github.ithamal.queue.annotation.MessageHandlerBind;
import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.handler.MessageHandlerAdapter;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;

/**
 * @author: ken.lin
 * @since: 2023-09-30 00:06
 */
@Component
@MessageHandlerBind("test")
public class TestMessageHandler extends MessageHandlerAdapter<String> {

    @Override
    public void handle(Message<String> message, Consumer consumer) {
        System.out.println(consumer.getName() + "-" + message);
        consumer.ack(Collections.singletonList(message.getId()));
    }


    @Override
    public void handle(Message<String> message) {

    }

}
