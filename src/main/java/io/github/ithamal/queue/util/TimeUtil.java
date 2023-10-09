package io.github.ithamal.queue.util;

import java.time.Duration;
import java.util.Arrays;

/**
 * @author: ken.lin
 * @since : 2023-02-23 23:22
 */
public class TimeUtil {

    public static Duration parseDuration(String s) {
        s = s.trim();
        if (s.contains("ms")) {
            return Duration.ofMillis(Integer.parseInt(s.replace("ms", "")));
        }
        if (s.contains("m")) {
            return Duration.ofMinutes(Integer.parseInt(s.replace("m", "")));
        }
        if (s.contains("h")) {
            return Duration.ofHours(Integer.parseInt(s.replace("h", "")));
        }
        if (s.contains("d")) {
            return Duration.ofDays(Integer.parseInt(s.replace("d", "")));
        }
        if (s.contains("s")) {
            return Duration.ofSeconds(Integer.parseInt(s.replace("s", "")));
        }
        return Duration.ofSeconds(Integer.parseInt(s));
    }

    public static long getNowTimestamp(){
        return System.currentTimeMillis();
    }

    public static Duration[] parseLater(String later) {
        return Arrays.stream(later.split(",")).map(TimeUtil::parseDuration).toArray(Duration[]::new);
    }
}
