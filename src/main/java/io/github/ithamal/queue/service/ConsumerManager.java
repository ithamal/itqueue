package io.github.ithamal.queue.service;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.factory.QueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: ken.lin
 * @since: 2023-09-29 13:35
 */
public class ConsumerManager {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerManager.class);

    private final Collection<QueueFactory> queueFactories;

    private final ConcurrentHashMap<String, ConsumerGroup> consumerMap = new ConcurrentHashMap<>();

    public ConsumerManager(Collection<QueueFactory> queueFactories) {
        this.queueFactories = queueFactories;
    }

    public void register(ConsumerSetting consumerSetting) {
        QueueFactory queueFactory = selectQueueFactory(consumerSetting.getImplClass());
        ConsumerGroup consumerGroup = queueFactory.createConsumerGroup(consumerSetting);
        consumerMap.put(consumerSetting.getName(), consumerGroup);
        logger.info("Consumer group [{}] has been registered", consumerSetting.getName());

    }

    public ConsumerGroup getConsumerGroup(String name) {
        return consumerMap.get(name);
    }

    private QueueFactory selectQueueFactory(String implClass) {
        for (QueueFactory queueFactory : queueFactories) {
            if (queueFactory.getName().equals(implClass)) {
                return queueFactory;
            }
        }
        throw new RuntimeException("没有找到队列工厂:" + implClass);
    }
}
