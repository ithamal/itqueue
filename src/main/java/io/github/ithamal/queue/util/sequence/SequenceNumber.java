package io.github.ithamal.queue.util.sequence;


public interface SequenceNumber {

    /**
     * 获取下一个number
     * @return
     */
    int next();

    /**
     * 获取当前值
     * @return
     */
    int getValue();
}
