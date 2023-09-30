package io.github.ithamal.queue.util.sequence;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

public class MsgId implements Serializable {

    private long value;

    private static final ReuseSequenceNumber sequenceNumber = new ReuseSequenceNumber();

    public MsgId(long value) {
        this.value = value;
    }

    public static MsgId create() {
        return fromTimestamp(System.currentTimeMillis(), ProcessID.getProcessId() , sequenceNumber.next());
    }

    public static MsgId fromTimestamp(long timeMillis) {
        return fromTimestamp(timeMillis,  ProcessID.getProcessId() , sequenceNumber.next());
    }

    public static MsgId fromTimestamp(long timeMillis, int gateId, int sequenceId) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeMillis);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int seconds = cal.get(Calendar.SECOND);
        int extGateId = gateId + cal.get(Calendar.MILLISECOND);
        return new MsgId(toLong(month, day, hour, minute, seconds, extGateId, sequenceId));
    }

    public static MsgId fromBytes(byte[] bytes) {
        return new MsgId(ByteBuffer.wrap(bytes).getLong());
    }

    private static long toLong(int month, int day, int hour, int minutes, int seconds, int gateId, int sequenceId) {
        long result = 0;
        result |= (long) month << 59;
        result |= (long) day << 54;
        result |= (long) hour << 49;
        result |= (long) minutes << 43;
        result |= (long) seconds << 37;
        result |= (long) gateId & 0x7fff  << 15;
        result |= (long) sequenceId & 0xffff;
        return result;
    }

    public Date getTime() {
        int month = (int) (value >> 59 & 0xf);
        int day = (int) ((value >> 54) & 0x1f);
        int hour = (int) ((value >> 49) & 0x1f);
        int minutes = (int) ((value >> 43) & 0x3f);
        int seconds = (int) ((value >> 37) & 0x3f);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.DATE, day);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minutes);
        calendar.set(Calendar.SECOND, seconds);
        calendar.set(Calendar.MILLISECOND, 0);
        if(calendar.getTime().after(new Date())){
            calendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR) -1);
        }
        return calendar.getTime();
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putLong(value);
        return bytes;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 100; i++) {
            long value = MsgId.create().getValue();
            System.out.println(value);
            System.out.println(new MsgId(value).getTime().toLocaleString());
            Thread.sleep(1000);
        }
    }

}
