package io.github.ithamal.queue.support.redis.zset;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.core.Producer;
import io.github.ithamal.queue.factory.QueueFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: ken.lin
 * @since: 2023-09-28 16:05
 */
public class RedisZSetQueueFactory implements QueueFactory {

    private RedisConnectionFactory redisConnectionFactory;

    public RedisZSetQueueFactory(RedisConnectionFactory redisConnectionFactory) {
        this.redisConnectionFactory = redisConnectionFactory;
    }

    @Override
    public boolean support(String implClass) {
        return implClass.equalsIgnoreCase("redisZSet");
    }

    @Override
    public Producer createProducer(ProducerSetting setting) {
        return new RedisZSetQueueProducer(setting, redisConnectionFactory);
    }

    @Override
    public ConsumerGroup createConsumerGroup(ConsumerSetting setting) {
        AtomicReference<List<Consumer>> consumerListRef = new AtomicReference<>();
        ConsumerGroup group = new ConsumerGroup() {
            @Override
            public String getName() {
                return setting.getGroupName();
            }

            @Override
            public ConsumerSetting getSetting() {
                return setting;
            }

            @Override
            public List<Consumer> getConsumers() {
                return consumerListRef.get();
            }
        };
        List<Consumer> consumerList = new ArrayList<>(setting.getConsumerNum());
        for (int i = 0; i < setting.getConsumerNum(); i++) {
            String name = setting.getGroupName() + "#" + i;
            RedisZSetQueueConsumer consumer = new RedisZSetQueueConsumer(name, group, redisConnectionFactory);
            consumerList.add(consumer);
        }
        consumerListRef.set(consumerList);
        return group;
    }
}
