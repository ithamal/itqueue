package io.github.ithamal.queue.config;

import io.github.ithamal.queue.sequence.ProcessID;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.Assert;

/**
 * @author: ken.lin
 * @since: 2023-09-28 13:43
 */
@Getter
@Setter
public class ProducerSetting {

    private String name;

    private String queue;

    private String prefix;

    private String serializer;

    private String implClass;

    private int reserveDays = 0;

    /**
     * 节点id，用于区分消息id，如果为空将会区， 最大值：16383
     */
    private Integer nodeId;

    public void afterProperties() {
        Assert.notNull(queue, "Producer setting [queue] isn't specified");
        Assert.notNull(implClass, "Producer setting [implClass] isn't specified");
        Assert.isTrue(nodeId == null || nodeId < 16383, "Producer setting [nodeId] must less then 16383");
        name = name != null ? name : queue;
        prefix = prefix != null ? prefix : "queue:";
        serializer = serializer != null ? serializer : "json";
        nodeId = nodeId == null ? 16383 : nodeId;
    }
}
