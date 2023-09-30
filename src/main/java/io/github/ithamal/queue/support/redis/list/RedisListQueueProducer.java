package io.github.ithamal.queue.support.redis.list;

import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.core.Producer;
import io.github.ithamal.queue.support.redis.RedisSerializerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author: ken.lin
 * @since: 2023-09-28 17:52
 */
@SuppressWarnings("unchecked")
public class RedisListQueueProducer implements Producer {

    private final RedisConnectionFactory connectionFactory;

    private final RedisQueueKeysBuilder keysBuilder;

    private final RedisSerializer serializer;

    public RedisListQueueProducer(ProducerSetting setting, RedisConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        this.keysBuilder = new RedisQueueKeysBuilder(setting.getPrefix(), setting.getQueue());
        this.serializer = RedisSerializerFactory.getSerializer(setting.getSerializer());
    }

    @Override
    public void put(Message message) {
        byte[] valueBytes = serializer.serialize(message);
        assert valueBytes != null;
        byte[] bucketKey = keysBuilder.buildBucketKey(new Date(message.getTime()));
        byte[] inboundKey = keysBuilder.buildInboundKey();
        byte[] idKey = String.valueOf(message.getId()).getBytes();
        try (RedisConnection connection = connectionFactory.getConnection()) {
            connection.hSet(bucketKey, idKey, valueBytes);
            connection.lPush(inboundKey, idKey);
            connection.expire(bucketKey, TimeUnit.DAYS.toSeconds(30));
        }
    }

    @Override
    public void batchPut(Collection<Message> messages) {
        for (Message message : messages) {
            put(message);
        }
    }
}
