package io.github.ithamal.queue.core;

/**
 * @author: ken.lin
 * @since: 2023-09-28 21:20
 */
public abstract class AbstractConsumer implements Consumer {

    private final String name;

    private final ConsumerGroup group;

    public AbstractConsumer(String name, ConsumerGroup group) {
        this.group = group;
        this.name = name;
    }

    @Override
    public ConsumerGroup getGroup() {
        return group;
    }

    @Override
    public String getName() {
        return name;
    }
}
