package io.github.ithamal.queue.service;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.factory.QueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: ken.lin
 * @since: 2023-09-29 13:35
 */
public class ConsumerManager {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerManager.class);

    private final Collection<QueueFactory> queueFactories;

    private final ConcurrentHashMap<String, ConsumerGroup> consumerGroupMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, List<ConsumerGroup>> queueConsumerGroupMap = new ConcurrentHashMap<>();

    public ConsumerManager(Collection<QueueFactory> queueFactories) {
        this.queueFactories = queueFactories;
    }

    public void register(ConsumerSetting consumerSetting) {
        String groupName = consumerSetting.getName();
        QueueFactory queueFactory = selectQueueFactory(consumerSetting.getImplClass());
        ConsumerGroup consumerGroup = queueFactory.createConsumerGroup(consumerSetting);
        if (consumerGroupMap.containsKey(groupName)) {
            throw new IllegalArgumentException("duplication of consumer group:" + groupName);
        }
        String queue = consumerSetting.getQueue();
        consumerGroupMap.put(groupName, consumerGroup);
        queueConsumerGroupMap.computeIfAbsent(queue, it -> new ArrayList<>()).add(consumerGroup);
        logger.info("Consumer group [{}] has been registered", groupName);

    }

    public ConsumerGroup getConsumerGroup(String name) {
        return consumerGroupMap.get(name);
    }


    public List<ConsumerGroup> findConsumerGroupByQueue(String name) {
        List<ConsumerGroup> groups = queueConsumerGroupMap.get(name);
        return groups == null ? Collections.emptyList() : groups;
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
