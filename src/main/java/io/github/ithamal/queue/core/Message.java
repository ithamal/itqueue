package io.github.ithamal.queue.core;


/**
 * @author: ken.lin
 * @since: 2023-09-28 09:19
 */
public interface Message<T> {

    Long getId();

    Long getTime();

    T getPayload();

    Integer getRetries();

    void setRetries(Integer retries);
}
