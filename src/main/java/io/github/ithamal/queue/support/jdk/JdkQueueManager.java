package io.github.ithamal.queue.support.jdk;

import io.github.ithamal.queue.core.Message;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * @author: ken.lin
 * @since: 2023-09-28 16:09
 */
public class JdkQueueManager {

    private final static ConcurrentHashMap<String, Queue<Message>> queueMap = new ConcurrentHashMap<>();

    public static Queue<Message> getQueue(String name, Function<String, Queue<Message>> supplier){
        return queueMap.computeIfAbsent(name, supplier);
    }
}
