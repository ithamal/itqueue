package io.github.ithamal.queue.boot;

import io.github.ithamal.queue.annotation.MessageHandlerBind;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.config.ItQueueConfig;
import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.factory.QueueFactory;
import io.github.ithamal.queue.handler.MessageHandler;
import io.github.ithamal.queue.handler.MessageHandlerProvider;
import io.github.ithamal.queue.service.ConsumerManager;
import io.github.ithamal.queue.service.ConsumersContainer;
import io.github.ithamal.queue.service.ProducerManager;
import io.github.ithamal.queue.support.jdk.JdkQueueFactory;
import io.github.ithamal.queue.support.redis.list.RedisListQueueFactory;
import io.github.ithamal.queue.support.spring.ConsumersContainerLifecycle;
import io.github.ithamal.queue.support.spring.SpringMessageHandlerProvider;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author: ken.lin
 * @since: 2023-09-27 10:12
 */
@Configuration
@AutoConfigureAfter(RedisAutoConfiguration.class)
@ConditionalOnProperty(prefix = "itqueue", name = "enable", havingValue = "true", matchIfMissing = true)
public class ItQueueAutoConfig {

    @Bean
    @ConfigurationProperties(prefix = "itqueue")
    public ItQueueConfig queueConfig() {
        return new ItQueueConfig();
    }

    /**
     * @return JDK队列工厂
     */
    @Bean
    public JdkQueueFactory jdkQueueFactory() {
        return new JdkQueueFactory();
    }

    /**
     * @param connectionFactory redis连接工厂
     * @return Redis队列工厂
     */
    @Bean
    @ConditionalOnBean(RedisConnectionFactory.class)
    public RedisListQueueFactory redisListQueueFactory(RedisConnectionFactory connectionFactory) {
        return new RedisListQueueFactory(connectionFactory);
    }

    /**
     * @param queueFactories 队列工厂集合
     * @return 生产者管理器
     */
    @Bean
    public ProducerManager producerManager(List<QueueFactory> queueFactories, ItQueueConfig queueConfig) {
        ProducerManager producerManager = new ProducerManager(queueFactories);
        for (ProducerSetting producerSetting : queueConfig.getProducers()) {
            producerManager.register(producerSetting);
        }
        return producerManager;
    }

    /**
     * @param queueFactories 队列工厂集合
     * @return 消费组管理器
     */
    @Bean
    public ConsumerManager consumerManager(List<QueueFactory> queueFactories, ItQueueConfig queueConfig) {
        ConsumerManager consumerManager = new ConsumerManager(queueFactories);
        for (ConsumerSetting consumerSetting : queueConfig.getConsumers()) {
            consumerManager.register(consumerSetting);
        }
        return consumerManager;
    }

    @Bean
    public MessageHandlerProvider messageHandlerProvider() {
        return new SpringMessageHandlerProvider();
    }

    /**
     * @return 消费组服务容器
     */
    @Bean
    public ConsumersContainerLifecycle consumersContainer(ConsumerManager consumerManager, MessageHandlerProvider messageHandlerProvider) {
        ConsumersContainerLifecycle consumersContainerLifecycle = new ConsumersContainerLifecycle();
        ConsumersContainer consumersContainer = consumersContainerLifecycle.getConsumerServer();
        for (MessageHandler<?> handler : messageHandlerProvider.getHandlers()) {
            MessageHandlerBind annotation = getHandlerAnnotation(handler);
            for (String pattern : annotation.value()) {
                List<ConsumerGroup> consumerGroups = consumerManager.findConsumers(pattern);
                for (ConsumerGroup consumerGroup : consumerGroups) {
                    consumersContainer.binding(consumerGroup, handler);
                }
            }
        }
        return consumersContainerLifecycle;
    }


    private static MessageHandlerBind getHandlerAnnotation(MessageHandler<?> handler) {
        Class<?> aClass = handler.getClass();
        MessageHandlerBind annotation = aClass.getAnnotation(MessageHandlerBind.class);
        if (annotation != null) {
            return annotation;
        }
        aClass = aClass.getSuperclass();
        if (aClass != null) {
            annotation = aClass.getAnnotation(MessageHandlerBind.class);
            if (annotation != null) {
                return annotation;
            }
        }
        return null;
    }
}
