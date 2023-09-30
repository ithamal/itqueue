package io.github.ithamal.queue.config;

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

    public void afterProperties() {
        Assert.notNull(queue, "Producer setting [queue] isn't specified");
        Assert.notNull(implClass, "Producer setting [implClass] isn't specified");
        name = name != null ? name : queue;
        prefix = prefix != null ? prefix : "queue:";
        serializer = serializer != null ? serializer : "json";
    }
}
