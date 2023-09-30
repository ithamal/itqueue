package io.github.ithamal.queue.support.spring;

import io.github.ithamal.queue.annotation.MessageHandlerBind;
import io.github.ithamal.queue.handler.MessageHandler;
import io.github.ithamal.queue.handler.MessageHandlerProvider;
import org.springframework.context.ApplicationContext;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: ken.lin
 * @since: 2023-09-29 23:52
 */
public class SpringMessageHandlerProvider implements MessageHandlerProvider {

    @Resource
    private ApplicationContext applicationContext;

    @Override
    public List<MessageHandler<?>> getHandlers() {
        return Arrays.stream(applicationContext.getBeanNamesForAnnotation(MessageHandlerBind.class))
                .map(it -> (MessageHandler<?>) applicationContext.getBean(it)
                ).collect(Collectors.toList());
    }
}
