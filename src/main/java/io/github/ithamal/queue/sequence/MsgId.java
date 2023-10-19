package io.github.ithamal.queue.sequence;

import org.springframework.util.Assert;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MsgId implements Serializable {

    private long value;

    public MsgId(long value) {
        this.value = value;
    }

    public static MsgId create(int nodeId, int sequenceVal) {
        return fromTimestamp(System.currentTimeMillis(), nodeId, sequenceVal);
    }

    public static MsgId fromTimestamp(long timestamp, int nodeId, int sequenceVal) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp);
        int year = cal.get(Calendar.YEAR) % 100;
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int seconds = cal.get(Calendar.SECOND);
        return new MsgId(toLong(year, month, day, hour, minute, seconds, nodeId, sequenceVal));
    }

    public static MsgId fromBytes(byte[] bytes) {
        return new MsgId(ByteBuffer.wrap(bytes).getLong());
    }

    private static long toLong(int year, int month, int day, int hour, int minutes, int seconds,  int nodeId, long sequenceId) {
        if (nodeId > 0x3fff) throw new IllegalArgumentException();
        if (nodeId + sequenceId > 0x7ffffff) throw new IllegalArgumentException();
        long result = 0;
        result = ((long) year) << 56;
        result |= ((long) month) << 49;
        result |= ((long) day) << 44;
        result |= ((long) hour) << 39;
        result |= ((long) minutes) << 33;
        result |= ((long) seconds) << 27;
        result |= (sequenceId + nodeId) & 0x7ffffff;
        return result;
    }

    public Date getTime() {
        int year =  (int) (value >> 56 & 0xff);
        year +=  (Calendar.getInstance().get(Calendar.YEAR) / 100) * 100;
        int month = (int) (value >> 49 & 0xf);
        int day = (int) ((value >> 44) & 0x1f);
        int hour = (int) ((value >> 39) & 0x1f);
        int minutes = (int) ((value >> 33) & 0x3f);
        int seconds = (int) ((value >> 27) & 0x3f);
        long sequenceId = (int) (value & 0x7ffffff);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.DATE, day);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minutes);
        calendar.set(Calendar.SECOND, seconds);
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

    public static void main(String[] args) {
        System.out.println();
        long value = toLong(23,12, 31, 23, 59, 59, 16383, 0x7ffffff - 16383);
//        long value = toLong(22,10, 18, 10, 11, 12, 16383, 0x7ffffff - 16383);
        value = create(16383, 0x7ffffff - 16383).getValue();
        System.out.println(value);
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new MsgId(value).getTime()));
    }


}

