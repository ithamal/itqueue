package io.github.ithamal.queue.redis.zset;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.support.redis.RedisQueueKeysBuilder;
import io.github.ithamal.queue.support.redis.zset.RedisZSetQueueFactory;
import io.github.ithamal.queue.support.redis.zset.RedisZSetScriptHelper;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.time.Duration;
import java.util.List;

/**
 * @author: ken.lin
 * @since: 2023-09-29 07:24
 */
public class RedisZSetScriptHelperTests {

    @Test
    public void testSequenceId() {
        LettuceConnectionFactory connectionFactory = createConnectionFactory();
        RedisQueueKeysBuilder keysBuilder = new RedisQueueKeysBuilder("mq:", "test");
        for (int i = 0; i < 10; i++) {
            RedisZSetScriptHelper.print(RedisZSetScriptHelper.generateSequenceId(connectionFactory, keysBuilder, "a", 60));
        }
    }

    @Test
    public void testPut() {
        LettuceConnectionFactory connectionFactory = createConnectionFactory();
        RedisQueueKeysBuilder keysBuilder = new RedisQueueKeysBuilder("mq:", "test");
        for (int i = 0; i < 10; i++) {
            RedisZSetScriptHelper.print(RedisZSetScriptHelper.put(connectionFactory, keysBuilder,
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
            List<byte[]> list = RedisZSetScriptHelper.poll(connectionFactory, keysBuilder, consumer, retryLaterList, 2);
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
        long ack = RedisZSetScriptHelper.ack(connectionFactory, keysBuilder, consumer, true, true, 11);
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
        consumerSetting.setImplClass("RedisZSet");
        consumerSetting.afterProperties();
        RedisZSetQueueFactory factory = new RedisZSetQueueFactory(createConnectionFactory());
        ConsumerGroup consumerGroup = factory.createConsumerGroup(consumerSetting);
        return consumerGroup.getConsumers().get(0);
    }

    public static void main(String[] args) {
        System.out.println((double) 2310.1821485400003);
        System.out.println((double) 2310.1821485400004);
    }
}
