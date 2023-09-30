package io.github.ithamal.queue.simple;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.core.*;
import io.github.ithamal.queue.support.redis.list.RedisListQueueFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author: ken.lin
 * @since: 2023-09-28 16:20
 */
public class RedisListTests {

    @BeforeEach
    public void before(){
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.INFO);
    }

    @Test
    public void testConsumer() {
        // 生产者配置
        ProducerSetting producerSetting = new ProducerSetting();
        producerSetting.setQueue("test");
        producerSetting.afterProperties();
        // 消费组配置
        ConsumerSetting consumerSetting = new ConsumerSetting();
        consumerSetting.setQueue("test");
        consumerSetting.setGroupName("group1");
        consumerSetting.setConsumerNum(2);
        consumerSetting.afterProperties();
        //
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
        connectionFactory.setHostName("localhost");
        connectionFactory.setPort(6379);
        connectionFactory.setPassword("");
        connectionFactory.setDatabase(4);
        connectionFactory.afterPropertiesSet();
        // 测试
        RedisListQueueFactory queueFactory = new RedisListQueueFactory(connectionFactory);
        Producer producer = queueFactory.createProducer(producerSetting);
        for (int i = 0; i < 10; i++) {
            producer.put(SimpleMessage.create("message-" + i));
        }
        ConsumerGroup consumerGroup = queueFactory.createConsumerGroup(consumerSetting);
        for (int i = 0; i < 20; i++) {
            for (Consumer consumer : consumerGroup.getConsumers()) {
                Collection<Message<?>> messages = consumer.poll(2);
                for (Message<?> message : messages) {
                    System.out.println("第" + i + "批:" + consumer.getName() + "----" + message);
                }
                consumer.ack(messages.stream().map(Message::getId).collect(Collectors.toList()));
            }
        }
    }

}