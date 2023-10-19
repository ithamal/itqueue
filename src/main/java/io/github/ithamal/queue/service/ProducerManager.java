package io.github.ithamal.queue.service;

import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.core.Producer;
import io.github.ithamal.queue.factory.QueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: ken.lin
 * @since: 2023-09-29 13:35
 */
public class ProducerManager {

    private final static Logger logger = LoggerFactory.getLogger(ProducerManager.class);

    private final Collection<QueueFactory> queueFactories;

    private final ConcurrentHashMap<String, Producer> producerMap = new ConcurrentHashMap<>();

    public ProducerManager(Collection<QueueFactory> queueFactories) {
        this.queueFactories = queueFactories;
    }

    public void register(ProducerSetting producerSetting) {
        QueueFactory queueFactory = selectQueueFactory(producerSetting.getImplClass());
        Producer producer = queueFactory.createProducer(producerSetting);
        producerMap.put(producerSetting.getName(), producer);
        logger.info("Producer [{}] has been registered", producerSetting.getName());
    }

    public Producer getProducer(String name) {
        return producerMap.get(name);
    }

    private QueueFactory selectQueueFactory(String implClass) {
        for (QueueFactory queueFactory : queueFactories) {
            if (queueFactory.support(implClass)) {
                return queueFactory;
            }
        }
        throw new RuntimeException("Not found implement class "+ implClass +" of queue factory" );
    }
}
