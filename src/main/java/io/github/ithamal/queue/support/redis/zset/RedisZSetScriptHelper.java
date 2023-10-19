package io.github.ithamal.queue.support.redis.zset;

import io.github.ithamal.queue.core.Consumer;
import io.github.ithamal.queue.sequence.MsgId;
import io.github.ithamal.queue.sequence.Score;
import io.github.ithamal.queue.support.redis.RedisQueueKeysBuilder;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: ken.lin
 * @since: 2023-10-18 10:52
 */
public class RedisZSetScriptHelper {

    public static long generateSequenceId(RedisConnectionFactory connectionFactory, RedisQueueKeysBuilder builder, String key, int expire) {
        List<byte[]> keysAndArgs = new ArrayList<>();
        keysAndArgs.add(builder.buildSequenceKey(key));
        keysAndArgs.add(String.valueOf(expire).getBytes());
        byte[] script = RedisZSetLuaScriptManager.get("sequence.lua");
        try (RedisConnection connection = connectionFactory.getConnection()) {
            return connection.eval(script, ReturnType.INTEGER, 1, keysAndArgs.toArray(new byte[0][]));
        }
    }

    public static long put(RedisConnectionFactory connectionFactory, RedisQueueKeysBuilder builder, byte[] key, byte[] value) {
        long timestamp = System.currentTimeMillis();
        String sequenceKey = new SimpleDateFormat("yyMMddHHmmss").format(new Date(timestamp));
        int sequenceId = (int) generateSequenceId(connectionFactory, builder, sequenceKey, 5);
        Score score = Score.fromTimestamp(timestamp, 0, sequenceId);
        List<byte[]> keysAndArgs = new ArrayList<>();
        keysAndArgs.add(builder.buildBucketKey(new Date()));
        keysAndArgs.add(builder.buildInboundKey());
        keysAndArgs.add(key);
        keysAndArgs.add(value);
        keysAndArgs.add(Double.toString(score.getDoubleValue()).getBytes());
        byte[] script = RedisZSetLuaScriptManager.get("put.lua");
        try (RedisConnection connection = connectionFactory.getConnection()) {
            return connection.eval(script, ReturnType.INTEGER, 3, keysAndArgs.toArray(new byte[0][]));
        }
    }

    public static List<byte[]> poll(RedisConnectionFactory connectionFactory,
                                    RedisQueueKeysBuilder builder,
                                    Consumer consumer,
                                    Duration[] retryLaterList,
                                    int size) {
        byte[] inboundKey = builder.buildInboundKey();
        byte[] outboundKey = builder.buildOutboundKey(consumer.getName());
        byte[] consumerGroupKey = builder.buildConsumerGroupKey(consumer.getGroup().getName());
        byte[] consumeNumKey = builder.buildConsumeNumKey(consumer.getName());
        byte[] deadKey = builder.buildDeadKey(consumer.getName());
        List<byte[]> keysAndArgs = new ArrayList<>();
        keysAndArgs.add(inboundKey);
        keysAndArgs.add(outboundKey);
        keysAndArgs.add(consumerGroupKey);
        keysAndArgs.add(consumeNumKey);
        keysAndArgs.add(deadKey);
        keysAndArgs.add(String.valueOf(size).getBytes());
        keysAndArgs.add(String.valueOf(System.currentTimeMillis() / 1000).getBytes());
        for (Duration retryLater : retryLaterList) {
            keysAndArgs.add(String.valueOf(retryLater.getSeconds()).getBytes());
        }
        byte[] script = RedisZSetLuaScriptManager.get("outbound.lua");
        try (RedisConnection connection = connectionFactory.getConnection()) {
            return connection.eval(script, ReturnType.MULTI, 5, keysAndArgs.toArray(new byte[0][]));
        }
    }

    public static long ack(RedisConnectionFactory connectionFactory,
                           RedisQueueKeysBuilder builder,
                           Consumer consumer,
                           boolean isDelete,
                           boolean isArchive,
                           long msgId) {
        byte[] bucketKey = builder.buildBucketKey(new MsgId(msgId).getTime());
        byte[] inboundKey = builder.buildInboundKey();
        byte[] outboundKey = builder.buildOutboundKey(consumer.getName());
        byte[] consumerGroupPattern = builder.buildConsumerGroupPattern();
        byte[] consumeNumKey = builder.buildConsumeNumKey(consumer.getName());
        byte[] deadKey = builder.buildDeadKey(consumer.getName());
        byte[] archiveKey = builder.buildArchiveKey(new Date());
        List<byte[]> keysAndArgs = new ArrayList<>();
        keysAndArgs.add(bucketKey);
        keysAndArgs.add(inboundKey);
        keysAndArgs.add(outboundKey);
        keysAndArgs.add(consumerGroupPattern);
        keysAndArgs.add(consumeNumKey);
        keysAndArgs.add(deadKey);
        keysAndArgs.add(archiveKey);
        keysAndArgs.add(isDelete ? "1".getBytes() : "0".getBytes());
        keysAndArgs.add(isArchive ? "1".getBytes() : "0".getBytes());
        keysAndArgs.add(String.valueOf(msgId).getBytes());
        byte[] script = RedisZSetLuaScriptManager.get("ack.lua");
        try (RedisConnection connection = connectionFactory.getConnection()) {
            return connection.eval(script, ReturnType.INTEGER, 7, keysAndArgs.toArray(new byte[0][]));
        }
    }

    public static void print(Object result) {
        if (result instanceof List) {
            for (Object o : ((List) result)) {
                if (o instanceof byte[]) {
                    System.out.println(new String((byte[]) o));
                } else {
                    System.out.println(result);
                }
            }
        } else {
            System.out.println(result);
        }
    }
}
