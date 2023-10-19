package io.github.ithamal.queue.support.redis.zset;

import io.github.ithamal.queue.config.ProducerSetting;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.core.Producer;
import io.github.ithamal.queue.sequence.MsgIdGenerator;
import io.github.ithamal.queue.support.redis.RedisQueueKeysBuilder;
import io.github.ithamal.queue.support.redis.RedisSerializerFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.Collection;

/**
 * @author: ken.lin
 * @since: 2023-09-28 17:52
 */
@SuppressWarnings("unchecked")
public class RedisZSetQueueProducer implements Producer {

    private final RedisConnectionFactory connectionFactory;

    private final RedisQueueKeysBuilder keysBuilder;

    private final RedisSerializer serializer;

    private final ProducerSetting setting;

    private final MsgIdGenerator msgIdGenerator;

    public RedisZSetQueueProducer(ProducerSetting setting, RedisConnectionFactory connectionFactory) {
        this.setting = setting;
        this.msgIdGenerator = new MsgIdGenerator(setting.getNodeId());
        this.connectionFactory = connectionFactory;
        this.keysBuilder = new RedisQueueKeysBuilder(setting.getPrefix(), setting.getQueue());
        this.serializer = RedisSerializerFactory.getSerializer(setting.getSerializer());
    }

    @Override
    public void put(Message message) {
        if (message.getId() == null) {
            message.setId(msgIdGenerator.create().getValue());
        }
        byte[] valueBytes = serializer.serialize(message);
        assert valueBytes != null;
        byte[] msgIdBytes = String.valueOf(message.getId()).getBytes();
        RedisZSetScriptHelper.put(connectionFactory, keysBuilder, msgIdBytes, valueBytes);
    }

    @Override
    public void batchPut(Collection<Message> messages) {
        for (Message message : messages) {
            put(message);
        }
    }
}
