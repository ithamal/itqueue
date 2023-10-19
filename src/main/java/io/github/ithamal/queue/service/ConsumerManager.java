package io.github.ithamal.queue.service;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.factory.QueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

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
        String name = consumerSetting.getName();
        QueueFactory queueFactory = selectQueueFactory(consumerSetting.getImplClass());
        ConsumerGroup consumerGroup = queueFactory.createConsumerGroup(consumerSetting);
        if (consumerMap.containsKey(name)) {
            throw new IllegalArgumentException("duplication of consumer group:" + name);
        }
        consumerMap.put(name, consumerGroup);
        logger.info("Consumer group [{}] has been registered", name);

    }

    public ConsumerGroup getConsumer(String name) {
        return consumerMap.get(name);
    }

    public List<ConsumerGroup> findConsumers(String pattern) {
        List<ConsumerGroup> list = new ArrayList<>();
        for (Map.Entry<String, ConsumerGroup> entry : consumerMap.entrySet()) {
            if (Pattern.compile(pattern).matcher(entry.getKey()).matches()) {
                list.add(entry.getValue());
            }
        }
        return list;
    }

    private QueueFactory selectQueueFactory(String implClass) {
        for (QueueFactory queueFactory : queueFactories) {
            if (queueFactory.support(implClass)) {
                return queueFactory;
            }
        }
        throw new RuntimeException("没有找到队列工厂:" + implClass);
    }
}
