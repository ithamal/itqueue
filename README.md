### Maven依赖
```xml
<groupId>io.github.ithamal</groupId>
<artifactId>itqueue</artifactId>
<version>1.0.0</version>
```

### 特性
- 完整的消息模式，支持手动拉取和自动拉取模式
- 支持消息重试、死信队列等特性
- 支持多消费者、多线程消费消息
- 支持Redis-List数据类型存储数据
- 与spring-data-redis无缝集成

### Redis-List实现的分叉数据
- 原始数据： {队列名}-bucket#日期MMdd (Hash结构)
- 待消费数据：{队列名}-inbound (List结构)
- 已消费(待ACK)：{队列名}-outbound:消费者名 (ZSet结构)
- 重试次数：{队列名}-try-num (Hash结构)
- 死信数据：{队列名}-dead:消费者名 (Hash结构)
- 归档数据(ACK后)：{队列名}-archive#日期MMdd (Hash结构)

### springboot配置示例
```yaml
itqueue:
  producers:
    - queue: test
      prefix: "mq:"
      implClass: redisList
      serializer: json
  consumers:
    - queue: test
      prefix: "mq:"
      groupName: group1
      implClass: redisList
      consumerNum: 4
      pollSize: 10
      retryLater: 10s,30s,1m,2m,3m,4m,5m,6m,7m,8m,9m,10m,20m,30m,1h,2h
      handleAsync: false
      minThreads: 1
      maxThreads: 4


spring:
  redis:
    host: localhost
    port: 6379
    password:
    database: 4

```

### 生产消息
```java

@Resource
private ProducerManager producerManager;

@Test
public void test() throws Exception {
    Producer producer = producerManager.getProducer("test");
    for (int i = 0; i < 10; i++) {
        producer.put(SimpleMessage.create(i + "-" + new Date()));
    }
    TimeUnit.SECONDS.sleep(30);
}
```

### 消费消息 (Handler模式)
```java
@Component
@MessageHandlerBind(consumerGroups = "test")
public class TestMessageHandler extends MessageHandlerAdapter<String> {

    @Override
    public void handle(Message<String> message) {
        System.out.println(message);
    }
}
```

### 消费消息 （手动模式）
```java

@Resource
private ConsumerManager consumerManager;

@Test
public void test() throws Exception {
    ConsumerGroup consumerGroup = consumerManager.getConsumerGroup("test");
    for (Consumer consumer : consumerGroup.getConsumers()) {
        Collection<Message<?>> mesasges = consumer.poll(2);
        for (Message<?> mesasge : mesasges) {
            System.out.println(mesasge);
        }
    }
    TimeUnit.SECONDS.sleep(30);

}
```
