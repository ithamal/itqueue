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
public class ConsumerSetting {

    private String name;

    private String queue;

    private String prefix;

    private Integer pollSize;

    private Integer minThreads;

    private Integer maxThreads;

    private Boolean handleAsync;

    private MessageHandlerSetting handler;

    private String groupName;

    private Integer consumerNum;

    private String serializer;

    private String retryLater;

    private Boolean deleteAfterAck;

    private String implClass;

    public void afterProperties() {
        Assert.notNull(queue, "Consumer setting [queue] isn't specified");
        Assert.notNull(groupName, "Consumer setting [groupName] isn't specified");
        Assert.notNull(implClass, "Consumer setting [implClass] isn't specified");
        name = name != null ? name : queue;
        prefix = prefix != null ? prefix : "queue:";
        pollSize = pollSize != null ? pollSize : 10;
        minThreads = minThreads != null ? minThreads : 1;
        maxThreads = maxThreads != null ? maxThreads : 10;
        consumerNum = consumerNum != null ? consumerNum : 1;
        handleAsync = handleAsync != null ? handleAsync : false;
        deleteAfterAck = deleteAfterAck != null ? deleteAfterAck : false;
        serializer = serializer != null ? serializer : "json";
        retryLater = retryLater != null ? retryLater : "10s,30s,1m,2m,3m,4m,5m,6m,7m,8m,9m,10m,20m,30m,1h,2h";
//        retryLater = retryLater != null ? retryLater : "30s,60s";
        handler = handler != null ? handler : new MessageHandlerSetting();
        handler.afterProperties();
    }
}
