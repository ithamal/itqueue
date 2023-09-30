package io.github.ithamal.queue.boot;

import io.github.ithamal.queue.SpringTestApplication;
import io.github.ithamal.queue.core.*;
import io.github.ithamal.queue.service.ConsumerManager;
import io.github.ithamal.queue.service.ProducerManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author: ken.lin
 * @since: 2023-09-19 17:28
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringTestApplication.class)
public class SpringBootTests {

    @Resource
    private ProducerManager producerManager;

    @Resource
    private ConsumerManager consumerManager;

    @Test
    public void test() throws Exception {
        Producer producer = producerManager.getProducer("test");
        for (int i = 0; i < 10; i++) {
            producer.put(SimpleMessage.create(i + "-" + new Date()));
        }
        ConsumerGroup consumerGroup = consumerManager.getConsumerGroup("test");
        for (Consumer consumer : consumerGroup.getConsumers()) {
            Collection<Message<?>> mesasges = consumer.poll(2);
            for (Message<?> mesasge : mesasges) {
                System.out.println(mesasge);
            }
        }
        TimeUnit.SECONDS.sleep(30);

    }
}
