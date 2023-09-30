package io.github.ithamal.queue.config;

import lombok.Getter;
import lombok.Setter;

/**
 * @author: ken.lin
 * @since: 2023-09-28 13:43
 */
@Getter
@Setter
public class MessageHandlerSetting {

    private Integer threads;

    public void afterProperties() {
        threads = threads != null ? threads : 0;
    }
}
