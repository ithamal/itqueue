package io.github.ithamal.queue.util.sequence;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * 可回归的序号生成器
 */
public class ReuseSequenceNumber implements SequenceNumber{

    public static final AtomicInteger DEFAULT_VALUE = new AtomicInteger(0);
    public static final int MAX_VALUE = 0x7FFFFFFF;

    private AtomicInteger value;

    public ReuseSequenceNumber() {
        this.value = DEFAULT_VALUE;
    }

    /**
     * 获取下一个sequenceNumber
     */
    @Override
    public int next() {
        return value.updateAndGet((v) -> {
            // 同步检查
            if (v >= MAX_VALUE) {
                return 1;
            }
            return v + 1;
        });
    }

    @Override
    public int getValue() {
        return value.get();
    }

    /**
     * 重置最小值
     */
    synchronized public void reset() {
        this.value = DEFAULT_VALUE;
    }

}

