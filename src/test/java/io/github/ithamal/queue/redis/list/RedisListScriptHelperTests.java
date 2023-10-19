package io.github.ithamal.queue.redis.list;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.support.redis.list.RedisListQueueFactory;
import io.github.ithamal.queue.support.redis.list.RedisListScriptHelper;
import io.github.ithamal.queue.support.redis.RedisQueueKeysBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.time.Duration;
import java.util.List;

/**
 * @author: ken.lin
 * @since: 2023-09-29 07:24
 */
public class RedisListScriptHelperTests {

    @Test
    public void testPut() {
        LettuceConnectionFactory connectionFactory = createConnectionFactory();
        RedisQueueKeysBuilder keysBuilder = new RedisQueueKeysBuilder("mq:", "test");
        for (int i = 0; i < 10; i++) {
            RedisListScriptHelper.print(RedisListScriptHelper.put(connectionFactory, keysBuilder,
                    String.valueOf(i).getBytes(), ("a" + i).getBytes()));
        }
    }


    @Test
    public void testPoll() {
        // mq:test:inbound -> zset
        LettuceConnectionFactory connectionFactory = createConnectionFactory();
        Consumer consumer = createConsumer();
        Duration[] retryLaterList = new Duration[]{Duration.ofSeconds(10), Duration.ofSeconds(30)};
        RedisQueueKeysBuilder keysBuilder = new RedisQueueKeysBuilder("queue:", "test");
        for (int i = 0; i < 10; i++) {
            List<byte[]> list = RedisListScriptHelper.poll(connectionFactory, keysBuilder, consumer, retryLaterList, 2);
            for (byte[] bytes : list) {
                System.out.println(new String(bytes));
            }
        }
    }


    @Test
    public void testAck() {
        LettuceConnectionFactory connectionFactory = createConnectionFactory();
        Consumer consumer = createConsumer();
        RedisQueueKeysBuilder keysBuilder = new RedisQueueKeysBuilder("mq:", "test");
        long ack = RedisListScriptHelper.ack(connectionFactory, keysBuilder, consumer, true, true, 11);
        System.out.println(ack);
    }

    private LettuceConnectionFactory createConnectionFactory() {
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
        connectionFactory.setHostName("localhost");
        connectionFactory.setPort(6379);
        connectionFactory.setPassword("");
        connectionFactory.setDatabase(0);
        connectionFactory.afterPropertiesSet();
        return connectionFactory;
    }

    private Consumer createConsumer() {
        ConsumerSetting consumerSetting = new ConsumerSetting();
        consumerSetting.setQueue("test");
        consumerSetting.setGroupName("default");
        consumerSetting.setConsumerNum(2);
        consumerSetting.setImplClass("redisList");
        consumerSetting.afterProperties();
        RedisListQueueFactory factory = new RedisListQueueFactory(createConnectionFactory());
        ConsumerGroup consumerGroup = factory.createConsumerGroup(consumerSetting);
        return consumerGroup.getConsumers().get(0);
    }

    public static void main(String[] args) {
        System.out.println((double) 2310.1821485400003);
        System.out.println((double) 2310.1821485400004);
    }
}
