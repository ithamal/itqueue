
package io.github.ithamal.queue.support.redis.zset;


import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.sequence.MsgId;
import io.github.ithamal.queue.support.redis.RedisQueueKeysBuilder;
import io.github.ithamal.queue.support.redis.RedisSerializerFactory;
import io.github.ithamal.queue.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author: ken.lin
 * @since: 2023-09-28 15:49
 */
public class RedisZSetQueueConsumer implements Consumer {

    private final static Logger logger = LoggerFactory.getLogger(RedisZSetQueueConsumer.class);

    private final String name;

    private final ConsumerGroup group;

    private final RedisConnectionFactory connectionFactory;

    private final RedisQueueKeysBuilder keysBuilder;

    private final RedisSerializer serializer;

    private final Duration[] retryLater;

    public RedisZSetQueueConsumer(String name, ConsumerGroup group, RedisConnectionFactory connectionFactory) {
        this.name = name;
        this.group = group;
        this.connectionFactory = connectionFactory;
        ConsumerSetting setting = group.getSetting();
        this.keysBuilder = new RedisQueueKeysBuilder(setting.getPrefix(), setting.getQueue());
        this.serializer = RedisSerializerFactory.getSerializer(setting.getSerializer());
        this.retryLater = TimeUtil.parseLater(setting.getRetryLater());
    }

    @Override
    public ConsumerGroup getGroup() {
        return group;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Collection<Message<?>> poll(int size) {
        List<byte[]> idBytesList = RedisZSetScriptHelper.poll(connectionFactory, keysBuilder, this, retryLater, size);
        return loadMessages(idBytesList);
    }

    @Override
    public void ack(Collection<Long> ids) {
        boolean isDeleted = getGroup().getSetting().getDeleteAfterAck();
        boolean isArchive = getGroup().getSetting().getIsArchive();
        for (Long id : ids) {
            long ack = RedisZSetScriptHelper.ack(connectionFactory, keysBuilder, this, isDeleted, isArchive, id);
            logger.debug("ack success, index：{}", ack);
        }
    }

    private List<Message<?>> loadMessages(List<byte[]> idList) {
        if (idList.isEmpty()) {
            return Collections.emptyList();
        }
        HashSet<String> bucketKeySet = new HashSet<>();
        for (byte[] id : idList) {
            Date time = new MsgId(Long.parseLong(new String(id))).getTime();
            String bucketKey = new String(keysBuilder.buildBucketKey(time));
            bucketKeySet.add(bucketKey);
        }
        byte[][] idBytesArray = idList.toArray(new byte[0][]);
        List<byte[]> messageBytesList = new ArrayList<>(idList.size());
        try (RedisConnection connection = connectionFactory.getConnection()) {
            for (String bucketKey : bucketKeySet) {
                List<byte[]> bytesList = connection.hMGet(bucketKey.getBytes(), idBytesArray);
                messageBytesList.addAll(bytesList);
            }
        }
        return messageBytesList.stream().filter(Objects::nonNull).map(bytes -> {
            Message<?> message = (Message<?>) serializer.deserialize(bytes);
            return message;
        }).collect(Collectors.toList());
    }
}
