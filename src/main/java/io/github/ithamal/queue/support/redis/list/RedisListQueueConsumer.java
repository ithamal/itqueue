
package io.github.ithamal.queue.support.redis.list;


import io.github.ithamal.queue.config.ConsumerSetting;
import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.core.ConsumerGroup;
import io.github.ithamal.queue.core.Message;
import io.github.ithamal.queue.support.redis.RedisSerializerFactory;
import io.github.ithamal.queue.util.TimeUtil;
import io.github.ithamal.queue.util.sequence.MsgId;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author: ken.lin
 * @since: 2023-09-28 15:49
 */
public class RedisListQueueConsumer implements Consumer {

    private final String name;

    private final ConsumerGroup group;

    private final RedisConnectionFactory connectionFactory;

    private final RedisQueueKeysBuilder keysBuilder;

    private final RedisSerializer serializer;

    private final Duration[] retryLater;

    public RedisListQueueConsumer(String name, ConsumerGroup group, RedisConnectionFactory connectionFactory) {
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
        byte[][] idBytesArray = pollIdList(size);
        if (idBytesArray.length > 0) {
            return loadMessages(idBytesArray);
        } else {
            idBytesArray = outbound(size);
            return loadMessages(idBytesArray);
        }
    }

    @Override
    public void ack(Collection<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        ConsumerSetting setting = group.getSetting();
        List<byte[]> keysAndArgList = new ArrayList<>();
        HashSet<String> bucketKeySet = new HashSet<>();
        for (Long id : ids) {
            Date time = new MsgId(id).getTime();
            byte[] bucketKey = keysBuilder.buildBucketKey(time);
            String bucketKeyStr = new String(bucketKey);
            if (!bucketKeySet.contains(bucketKeyStr)) {
                keysAndArgList.add(0, bucketKey);
                bucketKeySet.add(bucketKeyStr);
            }
            keysAndArgList.add(String.valueOf(id).getBytes());
        }
        byte[] outboundKey = keysBuilder.buildOutboundKey(this.name);
        byte[] archiveKey = keysBuilder.buildArchiveKey(new Date());
        byte[] tryNumKey = keysBuilder.buildTryNumKey();
        keysAndArgList.add(0, tryNumKey);
        keysAndArgList.add(0, archiveKey);
        keysAndArgList.add(0, outboundKey);
        int keysNum = bucketKeySet.size() + 3;
        byte[][] keysAndArgs = new byte[keysAndArgList.size()][];
        keysAndArgList.toArray(keysAndArgs);
        byte[] scriptBytes = RedisListLuaScriptManager.get(setting.getDeleteAfterAck() ? "ack-delete.lua" : "ack-archive.lua");
        try (RedisConnection connection = connectionFactory.getConnection()) {
            connection.eval(scriptBytes, ReturnType.BOOLEAN, keysNum, keysAndArgs);
            if (!setting.getDeleteAfterAck()) {
                connection.expire(archiveKey, TimeUnit.DAYS.toSeconds(7));
            }
        }
    }

    private byte[][] outbound(int size) {
        byte[] inboundKey = keysBuilder.buildInboundKey();
        byte[] outboundKey = keysBuilder.buildOutboundKey(this.name);
        byte[] script = RedisListLuaScriptManager.get("outbound.lua");
        long nextTryTime = TimeUtil.getNowTimestamp() + retryLater[0].toMillis();
        byte[][] keysAndArgs = new byte[][]{
                inboundKey,
                outboundKey,
                String.valueOf(size).getBytes(),
                String.valueOf(nextTryTime).getBytes(),
        };
        List<byte[]> idBytesList;
        try (RedisConnection connection = connectionFactory.getConnection()) {
            idBytesList = connection.eval(script, ReturnType.MULTI, 2, keysAndArgs);
        }
        assert idBytesList != null;
        byte[][] idBytesArr = new byte[idBytesList.size()][];
        idBytesList.toArray(idBytesArr);
        return idBytesArr;
    }

    private byte[][] pollIdList(int size) {
        byte[] outboundKey = keysBuilder.buildOutboundKey(this.name);
        long start = 0;
        long end = TimeUtil.getNowTimestamp();
        Set<byte[]> idBytesSet;
        try (RedisConnection connection = connectionFactory.getConnection()) {
            idBytesSet = connection.zRangeByScore(outboundKey, start, end, 0, size);
        }
        byte[][] idBytesArr = new byte[idBytesSet.size()][];
        idBytesSet.toArray(idBytesArr);
        return idBytesArr;
    }

    private List<Message<?>> loadMessages(byte[][] idArray) {
        if (idArray.length == 0) {
            return Collections.emptyList();
        }
        HashSet<String> bucketKeySet = new HashSet<>();
        for (byte[] idBytes : idArray) {
            long id = Long.parseLong(new String(idBytes));
            Date time = new MsgId(id).getTime();
            String bucketKey = new String(keysBuilder.buildBucketKey(time));
            bucketKeySet.add(bucketKey);
        }
        byte[] outboundKey = keysBuilder.buildOutboundKey(getName());
        byte[] tryNumKey = keysBuilder.buildTryNumKey();
        List<byte[]> mesasgeBytesList = new ArrayList<>(idArray.length);
        Map<Long, Long> messageTryNumMap = new HashMap<>();
        try (RedisConnection connection = connectionFactory.getConnection()) {
            // 批量提取消息
            for (String bucketKey : bucketKeySet) {
                List<byte[]> subMessageByteList = connection.hMGet(bucketKey.getBytes(), idArray);
                assert subMessageByteList != null;
                mesasgeBytesList.addAll(subMessageByteList);
            }
            // 加载次数增长
            List<byte[]> deadIdList = new ArrayList<>();
            Set<RedisZSetCommands.Tuple> tuples = new HashSet<>();
            for (byte[] id : idArray) {
                Long tryNum = connection.hIncrBy(tryNumKey, id, 1);
                if (tryNum != null && tryNum > 1) {
                    if(tryNum <= retryLater.length) {
                        long longId = Long.parseLong(new String(id));
                        messageTryNumMap.put(longId, tryNum);
                        double nextTryTime = TimeUtil.getNowTimestamp() + retryLater[tryNum.intValue() - 1].toMillis();
                        tuples.add(new DefaultTuple(id, nextTryTime));
                    }else{
                        deadIdList.add(id);
                    }
                }
            }
            // 更新下次重试时间
            if(!tuples.isEmpty()) {
                connection.zAdd(outboundKey, tuples);
            }
            // 死信队列
            if(!deadIdList.isEmpty()){
                transferToDead(connection, bucketKeySet, outboundKey, tryNumKey, deadIdList);
            }

        }
        // 转换结果返回
        return mesasgeBytesList.stream().filter(Objects::nonNull).map(bytes -> {
            Message<?> message = (Message<?>) serializer.deserialize(bytes);
            assert message != null;
            Long tryNum = messageTryNumMap.get(message.getId());
            if (tryNum != null) {
                message.setRetries(tryNum.intValue() - 1);
            } else {
                message.setRetries(0);
            }
            return message;
        }).collect(Collectors.toList());
    }

    private void transferToDead(RedisConnection connection, HashSet<String> bucketKeySet,
                        byte[] outboundKey, byte[] tryNumKey, List<byte[]> deadIdList) {
        List<byte[]> deadKeyAndArgList = new ArrayList<>(deadIdList.size() + 3 + bucketKeySet.size());
        byte[] deadKey = keysBuilder.buildDeadKey(getName());
        deadKeyAndArgList.add(outboundKey);
        deadKeyAndArgList.add(deadKey);
        deadKeyAndArgList.add(tryNumKey);
        for (String bucketKey : bucketKeySet) {
            deadKeyAndArgList.add(bucketKey.getBytes());
        }
        int keysNum = deadKeyAndArgList.size();
        deadKeyAndArgList.addAll(deadIdList);
        byte[][] keysAndArgs = new byte[deadKeyAndArgList.size()][];
        deadKeyAndArgList.toArray(keysAndArgs);
        byte[] scriptBytes = RedisListLuaScriptManager.get("dead.lua");
        connection.eval(scriptBytes, ReturnType.BOOLEAN, keysNum, keysAndArgs);
    }
}
