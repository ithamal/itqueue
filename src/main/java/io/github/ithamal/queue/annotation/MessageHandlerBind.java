package io.github.ithamal.queue.annotation;

import java.lang.annotation.*;

/**
 * @author ken.lin
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageHandlerBind {

    String[] consumerGroups();
}
