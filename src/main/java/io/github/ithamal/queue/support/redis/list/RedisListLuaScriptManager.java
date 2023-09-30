package io.github.ithamal.queue.support.redis.list;

import lombok.SneakyThrows;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: ken.lin
 * @since: 2023-09-29 07:14
 */
public class RedisListLuaScriptManager {

    private final static Map<String, byte[]> scriptMap = new ConcurrentHashMap<>();

    @SneakyThrows
    public static byte[] get(String name) {
        return scriptMap.computeIfAbsent(name, k -> {
            InputStream inputStream = RedisListQueueConsumer.class.getResourceAsStream("./lua/" + name);
            if (inputStream == null) {
                throw new RuntimeException("脚本文件[" + name + "]不存在");
            }
            try {
                return StreamUtils.copyToByteArray(inputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
