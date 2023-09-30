package io.github.ithamal.queue.core;

import io.github.ithamal.queue.config.ConsumerSetting;

import java.util.List;

/**
 * @author: ken.lin
 * @since: 2023-09-28 09:31
 */
public interface ConsumerGroup {

    String getName();

    ConsumerSetting getSetting();

    List<Consumer> getConsumers();
}
