package io.github.ithamal.queue.support.redis;

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
        return (prefix + queue + ":bucket:#" + mmdd).getBytes();
    }

    /**
     * 入栈
     * 例：queue:test:inbound
     *
     * @return
     */
    public byte[] buildInboundKey() {
        return (prefix + queue + ":inbound").getBytes();
    }

    /**
     * 消费
     *
     * @return
     */
    public byte[] buildOutboundKey(String consumerName) {
        return (prefix + queue + ":outbound:" + consumerName).getBytes();
    }

    /**
     * 消费组
     *
     * @return
     */
    public byte[] buildConsumerGroupKey(String consumerGroupName) {
        return (prefix + queue + ":consumer_group:" + consumerGroupName).getBytes();
    }

    /**
     * 消费组匹配模式
     *
     * @return
     */
    public byte[] buildConsumerGroupPattern() {
        return (prefix + queue + ":consumer_group:*" ).getBytes();
    }

    /**
     * 消费计数
     *
     * @return
     */
    public byte[] buildConsumeNumKey(String consumerName) {
        return (prefix + queue + ":consume_num:" + consumerName).getBytes();
    }

    /**
     *  死信
     *
     * @return
     */
    public byte[] buildDeadKey(String consumerName) {
        return (prefix + queue + ":dead:" + consumerName ).getBytes();
    }

    /**
     *  归档
     *
     * @return
     */
    public byte[] buildArchiveKey(Date time) {
        String mmdd = new SimpleDateFormat("MMdd").format(time);
        return (prefix + queue + ":archive:#" + mmdd).getBytes();
    }

    /**
     *  序号前缀
     *
     * @return
     */
    public byte[] buildSequenceKey(String key) {
        return (prefix + queue + ":sequence:" + key).getBytes();
    }
}
