package io.github.ithamal.queue.factory;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.core.Producer;

/**
 * @author: ken.lin
 * @since: 2023-09-28 16:05
 */
public interface QueueFactory {

    String getName();

    Producer createProducer(ProducerSetting setting);

    ConsumerGroup createConsumerGroup(ConsumerSetting setting);
}
