package io.github.ithamal.queue.annotation;

import java.lang.annotation.*;

/**
 * @author ken.lin
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageHandlerBind {

    /**
     * 队列名
     * @return
     */
    String[] queues() default {};

    /**
     * 消费组名
     * @return
     */
    String[] consumerGroups() default {};
}
