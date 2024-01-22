package io.github.ithamal.queue.support.redis;

import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: ken.lin
 * @since: 2023-09-26 15:17
 */
public class RedisSerializerFactory {

    private final static ConcurrentHashMap<String, RedisSerializer<?>> serializerMap = new ConcurrentHashMap<>();

    public static RedisSerializer getSerializer(String name) {
        return serializerMap.computeIfAbsent(name, key -> {
            if (name.equals("jdk")) {
                return new JdkSerializationRedisSerializer();
            }
            if (name.equals("string")) {
                return new StringRedisSerializer();
            }
            if (name.equals("json")) {
                return new GenericJackson2JsonRedisSerializer();
            }
            try {
                return (RedisSerializer<?>) Class.forName(name).newInstance();
            } catch (Throwable e) {
                // serialize class "x" load failed, didn't supported
                throw new RuntimeException("The serialize class '"+ name +"' failed to load as it's not supported");
            }
        });
    }
}
