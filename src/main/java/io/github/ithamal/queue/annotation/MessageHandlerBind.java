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
     * 消费组名称
     * @return
     */
    String[] value() ;

    /**
     * 限定消费组
     * @return
     */
    String consumerGroup() default "";
}
