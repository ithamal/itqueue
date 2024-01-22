package io.github.ithamal.queue.support.redis;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.core.Producer;
import io.github.ithamal.queue.factory.QueueFactory;
import io.github.ithamal.queue.support.redis.list.RedisListQueueFactory;
import io.github.ithamal.queue.support.redis.zset.RedisZSetQueueFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author: ken.lin
 * @since: 2023-10-19 14:11
 */
public class RedisComposeQueueFactory implements QueueFactory {

    private final List<QueueFactory> queueFactories;

    public RedisComposeQueueFactory(RedisConnectionFactory connectionFactory) {
        queueFactories = Arrays.asList(
                new RedisListQueueFactory(connectionFactory),
                new RedisZSetQueueFactory(connectionFactory)
        );
    }

    @Override
    public boolean support(String implClass) {
        return implClass.startsWith("redis");
    }

    @Override
    public Producer createProducer(ProducerSetting setting) {
        for (QueueFactory queueFactory : queueFactories) {
            if (queueFactory.support(setting.getImplClass())) {
                return queueFactory.createProducer(setting);
            }
        }
        throw new IllegalArgumentException("The implement class '" + setting.getImplClass() + "' of redis queue factory isn't supported");
    }

    @Override
    public ConsumerGroup createConsumerGroup(ConsumerSetting setting) {
        for (QueueFactory queueFactory : queueFactories) {
            if (queueFactory.support(setting.getImplClass())) {
                return queueFactory.createConsumerGroup(setting);
            }
        }
        throw new IllegalArgumentException("The implement class '" + setting.getImplClass() + "' of redis queue factory isn't supported");
    }
}
