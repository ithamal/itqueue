package io.github.ithamal.queue.sequence;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * 可回归的序号生成器
 */
public class SimpleSequenceNumber implements SequenceNumber{

    public static final int MAX_VALUE = 0xffff;

    private AtomicInteger value;

    public SimpleSequenceNumber(int initialValue) {
        this.value = new AtomicInteger(initialValue);
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

}

