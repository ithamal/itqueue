package io.github.ithamal.queue.core;

import io.github.ithamal.queue.util.sequence.MsgId;
import lombok.Getter;
import lombok.Setter;

/**
 * @author: ken.lin
 * @since: 2023-09-28 16:23
 */
@Getter
@Setter
public class SimpleMessage<T> implements Message<T> {

    private Long id;

    private Long time;

    private T payload;

    private Integer retries;

    public static <T> SimpleMessage<T> create(T payload){
        SimpleMessage<T> obj = new SimpleMessage<>();
        obj.id = MsgId.create().getValue();
        obj.time = System.currentTimeMillis();
        obj.payload = payload;
        return obj;
    }

    @Override
    public String toString() {
        return "SimpleMessage{" +
                "id=" + id +
                ", time=" + time +
                ", payload=" + payload +
                '}';
    }
}
