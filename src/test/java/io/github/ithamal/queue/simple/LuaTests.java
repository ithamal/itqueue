package io.github.ithamal.queue.simple;

import io.github.ithamal.queue.support.redis.list.RedisListLuaScriptManager;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.List;

/**
 * @author: ken.lin
 * @since: 2023-09-29 07:24
 */
public class LuaTests {

    @Test
    public void test() {
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
        connectionFactory.setHostName("localhost");
        connectionFactory.setPort(6379);
        connectionFactory.setPassword("");
        connectionFactory.setDatabase(4);
        connectionFactory.afterPropertiesSet();
        byte[] script = RedisListLuaScriptManager.get("outbound.lua");
        System.out.println(script);

        System.currentTimeMillis();
        byte[][] keysAndArgs = new byte[][]{
                "a".getBytes(),
                "b".getBytes(),
                "2".getBytes(),
                String.valueOf(System.currentTimeMillis()).getBytes(),
        };
        try(RedisConnection connection = connectionFactory.getConnection()){
            List list = (List)connection.eval(script, ReturnType.MULTI, 2, keysAndArgs);
            for (Object o : list) {
                if(o instanceof byte[]){
                    System.out.println(new String((byte[]) o));
                }else{
                    System.out.println(o);
                }
            }
        }
    }
}
