package io.github.ithamal.queue.core;


/**
 * @author: ken.lin
 * @since: 2023-09-28 09:19
 */
public interface Message<T> {

    Long getId();

    void setId(Long id);

    Long getTime();

    T getPayload();
}
