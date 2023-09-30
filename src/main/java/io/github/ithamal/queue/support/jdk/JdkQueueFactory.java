package io.github.ithamal.queue.support.jdk;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.core.Producer;
import io.github.ithamal.queue.factory.QueueFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: ken.lin
 * @since: 2023-09-28 16:05
 */
public class JdkQueueFactory implements QueueFactory {

    @Override
    public String getName() {
        return "jdk";
    }

    @Override
    public Producer createProducer(ProducerSetting setting) {
        Queue<Message> queue = JdkQueueManager.getQueue(setting.getQueue(), k -> new LinkedBlockingQueue<>());
        return new JdkQueueProducer(queue);
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
        Queue<Message> queue = JdkQueueManager.getQueue(setting.getQueue(), k -> new LinkedBlockingQueue<>());
        List<Consumer> consumerList = new ArrayList<>(setting.getConsumerNum());
        for (int i = 0; i < setting.getConsumerNum(); i++) {
            String name = setting.getGroupName() + "#" + i;
            JdkQueueConsumer consumer = new JdkQueueConsumer(name,  group, queue);
            consumerList.add(consumer);
        }
        consumerListRef.set(consumerList);
        return group;
    }
}
