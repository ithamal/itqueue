package io.github.ithamal.queue.jdk;

import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.core.*;
import io.github.ithamal.queue.service.ConsumerGroupContainer;
import io.github.ithamal.queue.service.impl.DefaultConsumerGroupContainer;
import io.github.ithamal.queue.support.jdk.JdkQueueFactory;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author: ken.lin
 * @since: 2023-09-28 16:20
 */
public class JdkQueueTests {

    @Test
    public void testConsumer() {
        // 生产者配置
        ProducerSetting producerSetting = new ProducerSetting();
        producerSetting.setQueue("test");
        producerSetting.setImplClass("jdk");
        producerSetting.afterProperties();
        // 消费组配置
        ConsumerSetting consumerSetting = new ConsumerSetting();
        consumerSetting.setQueue("test");
        consumerSetting.setGroupName("group1");
        consumerSetting.setConsumerNum(5);
        consumerSetting.setImplClass("jdk");
        consumerSetting.afterProperties();
        // 测试
        JdkQueueFactory queueFactory = new JdkQueueFactory();
        Producer producer = queueFactory.createProducer(producerSetting);
        for (int i = 0; i < 10; i++) {
            producer.put(SimpleMessage.create("message-" + i));
        }
        ConsumerGroup consumerGroup = queueFactory.createConsumerGroup(consumerSetting);
        for (int i = 0; i < 20; i++) {
            for (Consumer consumer : consumerGroup.getConsumers()) {
                Collection<Message<?>> messages = consumer.poll(2);
                for (Message<?> message : messages) {
                    System.out.println(consumer.getName() + "----" + message);
                }
            }
        }
    }

    @Test
    public void testConsumerContainer() throws InterruptedException {
        // 生产者配置
        ProducerSetting producerSetting = new ProducerSetting();
        producerSetting.setQueue("test");
        producerSetting.afterProperties();
        // 消费组配置
        ConsumerSetting consumerSetting = new ConsumerSetting();
        consumerSetting.setQueue("test");
        consumerSetting.setGroupName("group1");
        consumerSetting.setConsumerNum(5);
        consumerSetting.setMinThreads(1);
        consumerSetting.setMaxThreads(2);
        consumerSetting.afterProperties();
        // 测试
        JdkQueueFactory jdkQueueFactory = new JdkQueueFactory();
        Producer producer = jdkQueueFactory.createProducer(producerSetting);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 10; i++) {
            executorService.submit(()->{
                for (int j = 0; j < 10; j++) {
                    producer.put(SimpleMessage.create("message-" + j));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        ConsumerGroup consumerGroup = jdkQueueFactory.createConsumerGroup(consumerSetting);
        ConsumerGroupContainer container = new DefaultConsumerGroupContainer(consumerGroup);
        container.registerHandler((messages, consumer) -> {
            for (Message<?> message : messages) {
                System.out.println(consumer.getName() + "----" + message);
            }
        });
        container.start();
        TimeUnit.SECONDS.sleep(10);
        System.out.println("结束...");
        container.shutdown();
        container.awaitTermination(10, TimeUnit.MINUTES);
        System.out.println("已退出");
    }

    public static void main(String[] args) {
        // 1.2
        double d = 123.456;
        System.out.println(Long.toBinaryString(Double.doubleToRawLongBits(d)));

        String str = "0 10000000000 1001000111101011100001010001111010111000010100011110".replace(" ","");
        System.out.println(str);
        BigInteger num = new BigInteger(str, 2);
        System.out.println(Double.longBitsToDouble(num.longValue()));
    }
}
