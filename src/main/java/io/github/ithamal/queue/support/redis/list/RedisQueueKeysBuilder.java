package io.github.ithamal.queue.support.redis.list;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: ken.lin
 * @since: 2023-09-28 20:34
 */
public class RedisQueueKeysBuilder {

    private String prefix;

    private String queue;

    public RedisQueueKeysBuilder(String prefix, String queue) {
        this.prefix = prefix;
        this.queue = queue;
    }


    /**
     * 原始数据键，Hash结构
     * 例：queue:test:data#0928
     *
     * @param time
     * @return
     */
    public byte[] buildBucketKey(Date time) {
        String mmdd = new SimpleDateFormat("MMdd").format(time);
        return (prefix + queue + ":bucket#" + mmdd).getBytes();
    }

    /**
     * 入栈键，List结构
     * 例：queue:test:inbound
     *
     * @return
     */
    public byte[] buildInboundKey() {
        return (prefix + queue + ":inbound").getBytes();
    }

    /**
     * 消费, ZSet结构
     *
     * @return
     */
    public byte[] buildOutboundKey(String consumerName) {
        return (prefix + queue + ":outbound:" + consumerName).getBytes();
    }


    /**
     * 归档队列，Hash结构
     *
     * @return
     */
    public byte[] buildArchiveKey(Date time) {
        String mmdd = new SimpleDateFormat("MMdd").format(time);
        return (prefix + queue + ":archive#" + mmdd).getBytes();
    }


    /**
     * 重试计数器，Hash结构
     *
     * @return
     */
    public byte[] buildTryNumKey() {
        return (prefix + queue + ":try-num").getBytes();
    }


    /**
     * 重试队列，List结构
     *
     * @return
     */
    public byte[] buildDeadKey(String consumerName) {
        return (prefix + queue + ":dead:" + consumerName).getBytes();
    }
}
