package io.github.ithamal.queue.sequence;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Score implements Serializable {

    private long value;

    public Score(double value) {
        this.value = Double.doubleToLongBits(value);
    }

    public Score(long value) {
        this.value = value;
    }


    public static Score create(int nodeId, int sequenceVal) {
        return fromTimestamp(System.currentTimeMillis(), nodeId, sequenceVal);
    }

    public static Score fromTimestamp(long timeMillis, int nodeId, int sequenceVal) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeMillis);
        int year = cal.get(Calendar.YEAR) % 100;
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int seconds = cal.get(Calendar.SECOND);
        return new Score(toLong(year, month, day, hour, minute, seconds, nodeId, sequenceVal));
    }

    public static Score fromBytes(byte[] bytes) {
        return new Score(ByteBuffer.wrap(bytes).getLong());
    }

    public byte[] toBytes(){
        byte[] bytes = new byte[64];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    private static long toLong(int year, int month, int day, int hour, int minutes, int seconds, int nodeId, long sequenceId) {
        if (nodeId > 0x3fff) throw new IllegalArgumentException();
        if (nodeId + sequenceId > 0x7ffffff) throw new IllegalArgumentException();
        long result = 0;
        result |= ((long) year & 0xff) << 52; // 8
        result |= ((long) month) << 48; // 4
        result |= ((long) day) << 43; // 5
        result |= ((long) hour) << 38; // 5
        result |= ((long) minutes) << 32; // 6
        result |= ((long) seconds) << 26; // 6
        result |= (sequenceId + nodeId) & 0x1ffffff;
        return result;
    }

    private static double toDouble(int year, int month, int day, int hour, int minutes, int seconds, int nodeId, long sequenceId) {
        return Double.longBitsToDouble(toLong(year, month, day, hour, minutes, seconds, nodeId, sequenceId));
    }

    public Date getTime() {
        long longVal = Double.doubleToLongBits(value);
        int year = (int) (longVal >> 52 & 0xff);
        int month = (int) (longVal >> 48 & 0xf);
        int day = (int) ((longVal >> 43) & 0x1f);
        int hour = (int) ((longVal >> 38) & 0x1f);
        int minutes = (int) ((longVal >> 32) & 0x3f);
        int seconds = (int) ((longVal >> 26) & 0x3f);
        long sequenceId = (int) (longVal & 0x1ffffff);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.DATE, day);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minutes);
        calendar.set(Calendar.SECOND, seconds);
//        if (calendar.getTime().after(new Date())) {
//            calendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR) - 1);
//        }
        return calendar.getTime();
    }

    public double getDoubleValue() {
        return Double.longBitsToDouble(value);
    }

    public long getLongValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    public static void main(String[] args) throws InterruptedException {
        // 127
        double before = 0;
        for (int year = 23; year <= 23; year++) {
            double value = toDouble(year, 12, 31, 23, 59, 59, 16383, 0x7ffffff - 16383);
            System.out.println(value);
            double current = value;
            if (current <= before) {
                System.out.println("异常:" + current + "," + before);
            }
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Score(value).getTime()));
        }
        for (int i = 0; i < 1000000; i++) {
            double current = Score.create(1, i).getDoubleValue();
            if (current <= before) {
                System.out.println("异常:" + current + "," + before);
            }
//            System.out.println(current);
            before = current;
        }
        System.out.println("完成");
    }


}

