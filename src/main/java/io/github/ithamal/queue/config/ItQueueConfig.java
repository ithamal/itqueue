package io.github.ithamal.queue.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.InitializingBean;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: ken.lin
 * @since: 2023-09-29 13:21
 */
@Getter
@Setter
public class ItQueueConfig implements InitializingBean {


    private List<ProducerSetting> producers;

    private List<ConsumerSetting> consumers;

    @Override
    public void afterPropertiesSet() throws Exception {
        if(producers == null){
            producers = new ArrayList<>();
        }
        if(consumers == null){
            consumers = new ArrayList<>();
        }
        for (ProducerSetting producer : producers) {
            producer.afterProperties();
        }
        for (ConsumerSetting consumer : consumers) {
            consumer.afterProperties();
        }
    }
}
