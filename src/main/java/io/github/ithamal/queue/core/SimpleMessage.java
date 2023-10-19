package io.github.ithamal.queue.core;

import io.github.ithamal.queue.sequence.MsgId;
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

    private T payload;

    private Long time;

    public static <T> SimpleMessage<T> create(T payload){
        SimpleMessage<T> obj = new SimpleMessage<>();
        obj.payload = payload;
        obj.time = System.currentTimeMillis();
        return obj;
    }

    @Override
    public String toString() {
        return "SimpleMessage{" +
                "id=" + id +
                ", time=" + getTime() +
                ", payload=" + payload +
                '}';
    }

    @Override
    public Long getTime() {
        return time;
    }
}
