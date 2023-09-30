package io.github.ithamal.queue.support.spring;

import io.github.ithamal.queue.service.ConsumersContainer;
import io.github.ithamal.queue.service.impl.DefaultConsumersContainer;
import org.springframework.context.SmartLifecycle;

/**
 * @author: ken.lin
 * @since: 2023-09-29 23:00
 */
public class ConsumersContainerLifecycle implements SmartLifecycle {

    private final ConsumersContainer consumersContainer = new DefaultConsumersContainer();

    private volatile boolean isRunning = false;

    @Override
    public void start() {
        consumersContainer.start();
        isRunning = true;
    }

    @Override
    public void stop() {
        consumersContainer.shutdown();
        isRunning = false;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    public ConsumersContainer getConsumerServer() {
        return consumersContainer;
    }
}
