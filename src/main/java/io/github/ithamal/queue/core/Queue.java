package io.github.ithamal.queue.core;

import lombok.*;

/**
 * @author: ken.lin
 * @since: 2023-10-18 09:05
 */
public interface Queue {

    void size();

    void clip(int capacity);

    void createConsumerGroup(String groupName);

    void delConsumerGroup(String groupName);

    void delConsumer(String consumerName);

    void delMsg(Long msgId);
}
