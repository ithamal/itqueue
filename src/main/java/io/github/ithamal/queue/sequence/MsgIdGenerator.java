package io.github.ithamal.queue.sequence;

/**
 * @author: ken.lin
 * @since: 2023-10-16 16:42
 */
public class MsgIdGenerator {

    private final int nodeId;

    private final SequenceNumber sequenceNumber;

    public MsgIdGenerator(int nodeId){
        this(nodeId, new SimpleSequenceNumber(0));
    }

    public MsgIdGenerator(int nodeId, SequenceNumber sequenceNumber) {
        this.nodeId = nodeId;
        this.sequenceNumber = sequenceNumber;
    }

    public MsgId create() {
        long timeMillis = System.currentTimeMillis();
        int sequenceVal = sequenceNumber.next();
        return MsgId.fromTimestamp(timeMillis, nodeId, sequenceVal);
    }

    public static void main(String[] args) {
        SequenceNumber sequenceNumber = new SimpleSequenceNumber(0);
        MsgIdGenerator generator = new MsgIdGenerator(0, sequenceNumber);
        for (int i = 0; i < 1000; i++) {
            System.out.println(generator.create().getValue());
//            System.out.println(Long.toUnsignedString(generator.create().getValue()));
        }
    }
}
