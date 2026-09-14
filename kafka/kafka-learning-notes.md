# Kafka + Spring Boot 学习实验完整笔记

> 环境：Ubuntu Server (Proxmox VM, 192.168.31.220) + Docker Compose 单节点 Kafka 4.3.1 (KRaft)  
> 开发：Win10 (Proxmox VM) + Spring Boot 4.0 + spring-kafka 4.0.7  
> 整理日期：2026-09

---

## 目录

1. [环境描述](#一环境描述)
2. [常用 Docker / Kafka 命令](#二常用-docker--kafka-命令)
3. [实验清单 & 流程示意](#三实验清单--流程示意)
4. [踩坑记录](#四踩坑记录)
5. [核心概念速查](#五核心概念速查)
6. [Kafka 学习 Prompt 示例库](#六kafka-学习-prompt-示例库)
7. [附录：快速参考卡片](#附录快速参考卡片)

---

## 一、环境描述

| 组件 | 详情 |
|------|------|
| Kafka | 单节点，KRaft 模式，apache/kafka:4.3.1 |
| 部署方式 | Docker Compose，Ubuntu Server（Proxmox VM） |
| IP | 192.168.31.220，端口 9092 |
| docker-compose.yml | `~/personal_infra_lab/kafka/docker-compose.yml` |
| Spring Boot | 4.0，spring-kafka 4.0.7，spring-boot-starter-kafka |
| 开发机 | Win10（另一台 Proxmox VM） |
| 配置文件 | `application.properties` |
| 核心连接 | `bootstrap-servers=192.168.31.220:9092` |

---

## 二、常用 Docker / Kafka 命令

### 进入容器 & 工具路径

```bash
# apache/kafka:4.3.1 镜像中命令行工具不在 PATH，需用完整路径
/opt/kafka/bin/kafka-topics.sh
/opt/kafka/bin/kafka-console-consumer.sh
/opt/kafka/bin/kafka-console-producer.sh
/opt/kafka/bin/kafka-consumer-groups.sh
/opt/kafka/bin/kafka-transactions.sh
```

### Topic 管理

```bash
# 创建 topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic test --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 列出所有 topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# 查看 topic 详情（分区、副本、ISR）
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic test --bootstrap-server localhost:9092

# 修改分区数（只能增不能减）
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --alter \
  --topic test --partitions 6 --bootstrap-server localhost:9092
```

### Console Consumer（调试用）

```bash
# 从头消费，read_committed（事务验证用）
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic test --bootstrap-server localhost:9092 \
  --from-beginning --consumer-property isolation.level=read_committed

# 普通消费（能看到 uncommitted 消息）
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic test --bootstrap-server localhost:9092 --from-beginning
```

### Producer（调试用）

```bash
# 交互式发送消息
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic test --bootstrap-server localhost:9092

# 带 key 发送（key 和 value 用 tab 分隔）
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic test --bootstrap-server localhost:9092 \
  --property "key.separator=-"
```

### Consumer Group 状态

```bash
# 查看 group 列表
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list

# 查看指定 group 的消费进度
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group demo-group --describe

# 重置 offset（需要 consumer 不在线）
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group demo-group --reset-offsets --to-earliest --topic test --execute
```

---

## 三、实验清单 & 流程示意

### 实验 1：简单发送 + ab 压测

**目标**：验证 producer 能正常发消息，测吞吐

**application.properties 核心配置：**

```properties
spring.kafka.bootstrap-servers=192.168.31.220:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.consumer.group-id=demo-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=true
```

**Controller 代码：**

```java
@RestController
@RequestMapping("/api")
public class SendController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/send")
    public String send(@RequestParam String msg) {
        kafkaTemplate.send("test", msg);
        return "sent: " + msg;
    }
}
```

**压测命令：**

```bash
# post-data.txt 内容：msg=hello
ab -n 10000 -c 10 -p post-data.txt -T application/x-www-form-urlencoded http://localhost:8080/api/send
```

**结果**：QPS ~1749，无失败

**流程示意：**

```
HTTP POST → Spring Boot Controller → KafkaTemplate.send() → Broker (test topic)
                                                              ↓
                                                    Console Consumer 打印
```

---

### 实验 2：带 Key 发送 → 分区路由验证

**目标**：验证同 key 进同分区

**原理**：Kafka 默认用 `murmur2` hash 对 key 取模分区数

```
hash(key) % partition_count → partition_id
```

**Controller 代码：**

```java
@PostMapping("/send")
public String send(@RequestParam String key, @RequestParam String msg) {
    kafkaTemplate.send("test", key, msg);
    return "sent: " + key + " -> " + msg;
}
```

**验证方法：**

```bash
curl -X POST "http://localhost:8080/api/send?key=user1&msg=hello1"
curl -X POST "http://localhost:8080/api/send?key=user1&msg=hello2"
curl -X POST "http://localhost:8080/api/send?key=user1&msg=hello3"
curl -X POST "http://localhost:8080/api/send?key=user2&msg=world1"
```

观察 consumer 输出，确认 user1 的 3 条消息都在同一个 partition。

**验证 partition 归属：**

```bash
# 用 console consumer 带 partition 信息打印
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic test --bootstrap-server localhost:9092 --from-beginning \
  --property print.partition=true --property print.key=true
```

**流程示意：**

```
key="user1" ──→ hash(user1) % 3 = partition 1 ──→ 所有 user1 消息进 partition 1
key="user2" ──→ hash(user2) % 3 = partition 0 ──→ 所有 user2 消息进 partition 0
```

---

### 实验 3：双实例 → Rebalance 分区分摊

**目标**：验证 consumer group 内 rebalance 时分区自动分配

**操作：**

```bash
# 启动两个 Spring Boot 实例，分别监听 8080 和 8081
java -jar app.jar --server.port=8080
java -jar app.jar --server.port=8081
```

两个实例 `group-id=demo-group`，topic `test` 有 3 个 partition。

**预期**：3 个 partition 分摊到 2 个实例

**触发 rebalance**：停掉一个实例，观察另一个实例日志，分区重新分配。

**验证：**

```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group demo-group --describe
```

**输出示例：**

```
GROUP           TOPIC  PARTITION  CURRENT-OFFSET  LAG  CONSUMER-ID
demo-group      test   0          100             0    instance-A
demo-group      test   1          100             0    instance-A
demo-group      test   2          98              2    instance-B
```

**流程示意：**

```
启动前：  partition 0,1,2 全部分配给实例 A

启动实例 B 后（rebalance）：
  实例 A → partition 0, 1
  实例 B → partition 2

停掉实例 B 后（rebalance）：
  实例 A → partition 0, 1, 2（全部收回）
```

---

### 实验 4：手动提交 Offset

**目标**：关闭自动提交，手动控制 offset 提交时机

**配置：**

```properties
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.listener.ack-mode=manual
```

**Consumer 代码：**

```java
@Component
public class ManualAckConsumer {

    @KafkaListener(topics = "test", groupId = "demo-group")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            System.out.println("Received: " + record.value());
            // 业务处理...
            ack.acknowledge();  // 手动提交 offset
        } catch (Exception e) {
            // 不 ack → offset 不提交 → 重启后重新消费
            System.err.println("Processing failed, not acknowledging");
        }
    }
}
```

**验证**：不调 `ack.acknowledge()` 时，重启 consumer 会重新消费同一条消息。

**流程示意：**

```
自动提交：拉取 → 处理 → 后台定时提交（可能丢消息或重复消费）
手动提交：拉取 → 处理成功 → ack.acknowledge() → offset 提交
           处理失败 → 不 ack → 重启后重新消费
```

---

### 实验 5：消费异常 → 重试机制

**目标**：验证异常时 offset 不提交，Spring Kafka 自动重试

**配置：**

```properties
spring.kafka.consumer.enable-auto-commit=false
```

**Consumer 代码：**

```java
@Component
public class RetryConsumer {

    @KafkaListener(topics = "test", groupId = "demo-group")
    public void listen(String value) {
        System.out.println("Processing: " + value);
        if (value.contains("error")) {
            throw new RuntimeException("Simulated processing error");
        }
    }
}
```

**行为**：`DefaultErrorHandler` 默认 `FixedBackOff(0, 9)`，最多重试 9 次

**验证：**

```bash
curl -X POST "http://localhost:8080/api/send?msg=error-test"
```

观察日志：同一条消息被消费多次（默认 9 次），重试耗尽后跳过（seek 到下一条）。

**流程示意：**

```
消息到达 → 消费方法抛异常
  → Spring 捕获 → 重试（默认 9 次，间隔 0ms）
  → 耗尽 → seek 到下一条 → 继续消费后续消息
```

---

### 实验 6：批量消费

**配置：**

```properties
spring.kafka.consumer.max-poll-records=500
spring.kafka.listener.type=batch
```

**Consumer：**

```java
@Component
public class BatchConsumer {

    @KafkaListener(topics = "test", groupId = "demo-group")
    public void listenBatch(List<ConsumerRecord<String, String>> records) {
        System.out.println("Batch size: " + records.size());
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("  record: " + record.value());
        }
    }
}
```

**注意**：批中单条失败 → 整批重试（因为 offset 是按 partition 提交的）

**验证：**

```bash
# 快速发送多条消息
for i in {1..100}; do
  curl -X POST "http://localhost:8080/api/send?msg=msg-$i" &
done
wait
```

观察日志中 `Batch size` 的值。

**流程示意：**

```
poll() 一次拉取最多 500 条 → 作为 List 传入方法
  → 全部处理成功 → 提交 offset（整批）
  → 任一条抛异常 → 整批重试
```

---

### 实验 7：多 Group 订阅同一 Topic（发布订阅）

**配置：**

```properties
# application.properties 中动态指定 group-id
spring.kafka.consumer.group-id=${CONSUMER_GROUP:demo-group}
```

**验证：**

```bash
# 启动两个实例，分别用不同 group
java -jar app.jar --server.port=8080 --CONSUMER_GROUP=group-a
java -jar app.jar --server.port=8081 --CONSUMER_GROUP=group-b
```

**测试：**

```bash
curl -X POST "http://localhost:8080/api/send?msg=broadcast"
```

→ 两个不同 group 的 consumer 各收到一次

**流程示意：**

```
Producer → test topic
              ├─→ group-a (offset 独立) → consumer A 收到
              └─→ group-b (offset 独立) → consumer B 收到
```

---

### 实验 8：死信队列 DLQ

**KafkaConfig.java：**

```java
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic testTopic() {
        return TopicBuilder.name("test").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic testDlqTopic() {
        return TopicBuilder.name("test-dlq").partitions(3).replicas(1).build();
    }

    @Bean
    public ProducerFactory<String, String> dlqProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.220:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix("dlq-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> dlqKafkaTemplate(
            @Qualifier("dlqProducerFactory") ProducerFactory<String, String> dlqProducerFactory) {
        return new KafkaTemplate<>(dlqProducerFactory);
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            @Qualifier("dlqKafkaTemplate") KafkaTemplate<String, String> dlqKafkaTemplate) {
        return new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (record, exception) -> new TopicPartition("test-dlq", record.partition()));
    }

    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        FixedBackOff backOff = new FixedBackOff(1000L, 3);  // 重试 3 次，间隔 1s
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
```

**验证：**

```bash
# 创建 DLQ topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic test-dlq --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 发触发异常的消息
curl -X POST "http://localhost:8080/api/send?msg=fail-msg"

# 查看 DLQ topic（只产生一条死信记录）
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic test-dlq --bootstrap-server localhost:9092 --from-beginning
```

**流程示意：**

```
消息 → consumer 处理失败 → 重试 3 次（间隔 1s）
  → 耗尽 → DeadLetterPublishingRecoverer 转发到 test-dlq
  → 主 consumer offset 提交（不再阻塞主流程）
  → DLQ consumer 独立消费死信消息（可告警/人工处理）
```

---

### 实验 9：生产端事务（EOS）

**目标**：发消息 + 写 DB 原子性，rollback 时消息不可见

**application.properties：**

```properties
spring.kafka.producer.transaction-id-prefix=tx-
spring.kafka.producer.enable-idempotence=true
spring.kafka.producer.acks=all
spring.kafka.producer.retries=2147483647
spring.kafka.producer.transaction-timeout-ms=60000
spring.kafka.consumer.isolation-level=read_committed
spring.kafka.consumer.enable-auto-commit=false
```

**Service：**

```java
@Service
public class TxSendService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final List<String> fakeDb = new ArrayList<>();

    @Transactional
    public void sendWithTx(String msg) {
        kafkaTemplate.send("test-tx", msg);
        fakeDb.add(msg);
        if (msg.contains("fail")) {
            throw new RuntimeException("DB write failed, rollback!");
        }
    }
}
```

**Controller：**

```java
@RestController
@RequestMapping("/api/tx")
public class TxSendController {

    @Autowired
    private TxSendService txSendService;

    @PostMapping("/send")
    public String send(@RequestParam String msg) {
        txSendService.sendWithTx(msg);
        return "ok";
    }
}
```

**验证：**

```bash
# 创建 topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic test-tx --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# consumer（read_committed）
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic test-tx --bootstrap-server localhost:9092 \
  --from-beginning --consumer-property isolation.level=read_committed

# 测试
curl -X POST "http://localhost:8080/api/tx/send?msg=hello-tx"       # → 收到 ✅
curl -X POST "http://localhost:8080/api/tx/send?msg=fail-test"      # → 收不到 ❌
curl -X POST "http://localhost:8080/api/tx/send?msg=after-rollback" # → 收到 ✅
```

**流程示意：**

```
@Transactional 开启
  → producer.beginTransaction()
  → send("test-tx", msg)     ← 消息到 broker，对 read_committed consumer 不可见
  → fakeDb.add(msg)          ← 内存操作
  → 正常 → commitTransaction() → consumer 可见
  → 异常 → abortTransaction()  → consumer 永远看不到
```

---

### 实验 10：消费端事务（Consume-Transform-Produce）

**目标**：消费 + 处理 + 发送原子绑定

**Consumer：**

```java
@Component
public class TxConsumer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "test-tx", groupId = "tx-consumer-group")
    @Transactional
    public void consume(ConsumerRecord<String, String> record) {
        String value = record.value();
        System.out.println("Received: " + value);
        if (value.contains("fail")) {
            throw new RuntimeException("Processing failed, rollback!");
        }
        kafkaTemplate.send("test-tx-out", "processed: " + value);
    }
}
```

**验证：**

```bash
# 创建输出 topic
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic test-tx-out --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 监听输出 topic
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic test-tx-out --bootstrap-server localhost:9092 --from-beginning

# 发消息
curl -X POST "http://localhost:8080/api/tx/send?msg=order-001"      # → 正常 ✅
curl -X POST "http://localhost:8080/api/tx/send?msg=fail-order-002" # → rollback ❌
curl -X POST "http://localhost:8080/api/tx/send?msg=order-003"      # → 正常 ✅
```

**流程示意：**

```
test-tx → consumer 拉取（read_committed）
  → @Transactional 开启
  → 业务处理
  → kafkaTemplate.send("test-tx-out", result)  ← 对下游不可见
  → 正常 → commit: offset 提交 + output 消息可见
  → 异常 → abort: offset 不提交 + output 消息不可见 → 重启后重新消费
```

**关键验证**：`fail-order-002` rollback 后，重启应用，会看到它**被重新消费一次**（offset 没提交）。如果这次不抛异常，就成功处理并出现在 `test-tx-out`。

---

## 四、踩坑记录

| 坑 | 原因 | 解决 |
|----|------|------|
| Boot 4 找不到 KafkaTemplate | 没引 `spring-boot-starter-kafka` | 加依赖，自动配置才生效 |
| `kafka-topics` command not found | 4.3.1 镜像不在 PATH | 用 `/opt/kafka/bin/kafka-topics.sh` 全路径 |
| `ProducerFactory must support transactions` | 自定义 `dlqProducerFactory` 缺幂等配置，被当成主 ProducerFactory | `@Primary` 标注主 ProducerFactory + 补全事务配置 |
| console-consumer 看到 rollback 消息 | 默认 `read_uncommitted` | 加 `--consumer-property isolation.level=read_committed` |
| 多 KafkaTemplate Bean 注入冲突 | 两个 `KafkaTemplate<String, String>` Bean | `@Qualifier` 指定注入哪个 |
| transaction-id-prefix 改了不生效 | Broker 绑定了旧 transaction.id | 重启应用 |
| 单节点 acks=all 实际等同 acks=1 | 只有 1 个 broker | 扩多节点才真正生效 |

---

## 五、核心概念速查

| 概念 | 说明 |
|------|------|
| Partition | 并行单元，同 key 进同 partition，保证分区内有序 |
| Rebalance | group 内 consumer 数量变化时分区重新分配 |
| Offset | consumer 在 partition 中的消费位置 |
| ISR | In-Sync Replicas，和 leader 保持同步的副本集合 |
| ACKs | producer 等待多少个副本确认：`0`/`1`/`all` |
| Idempotence | 幂等 producer，防止网络重试导致重复消息 |
| Transaction | 跨 partition 原子写入，依赖 Transaction Coordinator |
| read_committed | consumer 只消费已提交事务的消息 |
| DLQ | 死信队列，处理失败消息的最终归宿 |
| EOS | Exactly-Once Semantics，幂等 + 事务实现 |
| KRaft | Kafka 自带的共识协议，替代 ZooKeeper |
| Consumer Group | 多个 consumer 共享一个 group-id，分摊分区消费 |
| Producer Fencing | 通过 transaction-id 防止僵尸 producer 重复写入 |

---

## 六、Kafka 学习 Prompt 示例库

> 以下 prompt 可以直接发给 AI 助手，用于推进 Kafka 学习、做实验、排错、设计架构。
> 每个 prompt 标注了适用场景和难度。

---

### 6.1 环境搭建 & 排错类

**P1：搭建单节点 Kafka**
> 我要用 Docker Compose 在 Ubuntu Server 上部署一个单节点 Kafka 4.x（KRaft 模式，不需要 ZooKeeper）。给我完整的 docker-compose.yml 和验证步骤。

**P2：Spring Boot 连接 Kafka**
> 我有一个 Spring Boot 4.0 项目，想接入 Kafka。给我最小化的依赖配置和 application.properties 示例，确保 KafkaTemplate 能自动注入。

**P3：排错模板**
> 我的 Spring Boot 启动报错：[粘贴完整异常栈]。我的环境是 Kafka 4.3.1 + Spring Boot 4.0 + spring-kafka 4.0.7。请帮我定位原因并给出修复方案。

**P4：Docker 镜像命令找不到**
> 我在 apache/kafka:4.3.1 容器里执行 kafka-topics 提示 command not found。帮我找到正确的路径，并列出所有可用命令行工具的完整路径。

**P5：多 broker 集群搭建**
> 帮我写一个 docker-compose.yml，部署 3 节点 Kafka KRaft 集群（1 controller + 3 brokers），并说明如何验证集群状态。

---

### 6.2 实验场景类

**P6：分区路由验证**
> 帮我写一个 Spring Boot Controller 方法，接收 key 和 value 参数，用 KafkaTemplate 带 key 发送到 topic "test"（3 个 partition）。我需要验证同 key 进同分区。给我完整代码和验证步骤。

**P7：自定义分区器**
> 帮我写一个自定义 Partitioner 实现：特定前缀的 key 强制进 partition 0，其余按默认 hash 分配。并写单元测试验证。

**P8：手动 offset 提交**
> 帮我写一个 @KafkaListener 消费者，关闭自动提交，用 Acknowledgment 手动提交 offset。演示处理成功才 ack，处理失败不 ack 的场景。

**P9：批量消费**
> 帮我配置 Spring Kafka 批量消费，每次 poll 最多 500 条，方法参数用 List<ConsumerRecord>。说明批中单条失败时的重试行为，以及如何做到单条失败不影响整批。

**P10：双实例 Rebalance 验证**
> 我想验证 Kafka consumer group 的 rebalance。给我操作步骤：启动两个 Spring Boot 实例（不同端口同 group-id），发消息，观察分区分配，然后停掉一个触发 rebalance。

**P11：多 Group 发布订阅**
> 帮我写两个 @KafkaListener 方法，分别用不同的 group-id 订阅同一个 topic，验证每个 group 独立消费。给我配置和测试 curl 命令。

**P12：消息头（Header）使用**
> 帮我演示 Spring Kafka 中如何使用消息头（Headers）：发送时设置 header，消费时通过 @Header 注解读取。给出完整示例。

---

### 6.3 异常处理 & DLQ 类

**P13：DLQ 完整配置**
> 帮我配置 Spring Kafka 的死信队列：消费异常重试 3 次后转发到 `{original-topic}-dlq` topic。给我 KafkaConfig.java 完整代码，包括自定义 ProducerFactory（事务前缀 dlq-tx-）、DeadLetterPublishingRecoverer、DefaultErrorHandler。

**P14：重试策略对比**
> 对比 Spring Kafka 中 FixedBackOff 和 ExponentialBackOff 的区别，给我配置示例和适用场景。

**P15：消费异常不阻塞**
> 我的 Kafka consumer 处理一条消息一直失败，导致后面所有消息都卡住。怎么配置让它失败 N 次后跳过这条消息继续消费？给我代码。

**P16：按异常类型分类处理**
> 帮我配置 Spring Kafka 的 ExceptionClassifier，不同异常类型走不同策略：业务异常直接进 DLQ（不重试），系统异常重试 5 次后进 DLQ。

---

### 6.4 事务消息类

**P17：生产端事务最小示例**
> 帮我写一个 Spring Boot 事务消息最小示例：@Transactional 注解下同时 kafkaTemplate.send() 和写数据库，异常时 rollback。要求 consumer 用 read_committed 验证 rollback 消息不可见。

**P18：消费端事务（Consume-Transform-Produce）**
> 帮我写 @KafkaListener + @Transactional 的消费端事务示例：从 topic A 消费，处理后发到 topic B，异常时 offset 不提交、output 消息不可见。给我完整代码和验证步骤。

**P19：事务 vs 幂等区别**
> 用通俗的方式解释 Kafka 幂等 producer 和事务 producer 的区别，分别解决什么问题，什么时候用哪个。

**P20：事务超时处理**
> Kafka 事务默认超时 60 秒，如果我的业务处理可能超过这个时间怎么配？transaction.timeout.ms 和 Spring 的 @Transactional timeout 怎么配合？

**P21：事务性能影响**
> Kafka 事务消息相比普通消息吞吐量下降多少？有哪些调优参数可以减少事务开销？

**P22：跨系统事务（Kafka + DB）**
> 我想要真正的"发消息 + 写 DB"原子性，但 Kafka 事务只管 Kafka 内部。有哪些方案可以实现跨系统原子性？对比 ChainedTransactionManager、Outbox 模式的优劣。

---

### 6.5 架构设计 & 进阶类

**P23：业务场景从零设计**
> 我有一个订单系统，下单后需要：1) 写订单表 2) 发通知 3) 更新库存。请用 Kafka 帮我设计整套方案：topic 规划、partition 数、group 设计、异常处理、DLQ 策略、事务 vs Outbox 模式选型。

**P24：Outbox 模式讲解**
> 解释 Kafka Outbox 模式是什么，为什么比直接用 Kafka 事务更适合 DB + 消息的原子性场景。给我一个 Spring Boot + PostgreSQL 的实现示例。

**P25：Kafka Streams 入门**
> 我想学 Kafka Streams 做实时流处理。给我一个最小示例：从 topic A 读取消息，filter 掉某些记录，transform 后写到 topic B。用 Spring Boot 集成。

**P26：高可用部署**
> 我现在的 Kafka 是单节点，怎么扩展到 3 节点集群？KRaft 模式下需要改哪些配置？Controller 和 Broker 角色怎么分配？min.insync.replicas 怎么设？

**P27：性能调优**
> 我的 Kafka producer 吞吐量不够，帮我列出 producer 端、broker 端、consumer 端的关键调优参数，说明每个参数的含义和推荐值。

**P28：消息顺序保证**
> 在 Kafka 中如何保证全局有序？分区内有序？如果消费者是多线程处理，怎么保证同一 partition 内的顺序？给我方案。

**P29：消息幂等设计**
> 我的 Kafka consumer 可能会重复消费同一条消息（rebalance、重试等）。帮我设计幂等消费方案：用 Redis SETNX、数据库唯一约束、或业务去重表。给代码示例。

---

### 6.6 监控 & 运维类

**P30：监控指标**
> Kafka 有哪些关键监控指标？给我 Prometheus + Grafana 的监控方案，列出最重要的 metrics（lag、吞吐、ISR 数量等）。

**P31：消息积压处理**
> 我的 Kafka topic 积压了 100 万条消息，consumer 跟不上。有哪些应急方案？临时扩 partition、加 consumer 实例、跳过消息、批量处理等方案对比。

**P32：消息丢失排查**
> 我的 Kafka 消息疑似丢失。给我一个排查清单：producer 端 acks/retries 配置、broker 端 min.insync.replicas、consumer 端 auto-commit 时机。

---

### 6.7 面试 & 知识梳理类

**P33：面试问题生成**
> 给我 20 道 Kafka 中级面试题，覆盖：分区策略、rebalance 协议、offset 管理、事务原理、ISR 机制、幂等性。每道题给参考答案要点。

**P34：概念对比**
> 对比 Kafka 和 RabbitMQ 的核心差异：消息模型、顺序保证、投递语义、吞吐量、适用场景。用表格呈现。

**P35：学习路线图**
> 我是一个 Java 后端开发，已经会用 Spring Kafka 做基本的 produce/consume。帮我规划下一步的学习路线：从进阶到专家级别，列出每个阶段该学什么、做什么实验。

---

### 6.8 综合实战类

**P36：日志收集系统**
> 帮我设计一个基于 Kafka 的日志收集系统：多个微服务产生日志 → Kafka → 日志存储（Elasticsearch）。设计 topic 结构、partition 策略、consumer group 规划、序列化方案。

**P37：事件溯源（Event Sourcing）**
> 用 Kafka 实现事件溯源模式：所有状态变更都作为事件写入 Kafka topic，服务重启后 replay 事件恢复状态。给我 Spring Boot 实现思路和关键代码。

**P38：CQRS + Kafka**
> 解释 CQRS 模式怎么和 Kafka 结合：写侧写事件到 Kafka，读侧消费事件更新读模型。给我一个订单系统的完整设计。

**P39：秒杀系统**
> 用 Kafka 设计一个秒杀系统的削峰方案：前端请求 → Kafka → 库存服务。设计 topic 分区策略、consumer 并发度、限流方案、超时处理。

---

## 七、两个事务场景对比总结

### 场景一：生产端事务（发消息 + 写 DB）

```
HTTP → [Producer + DB] ══(事务)══> Kafka ──(read_committed)──> Consumer
```

| 维度 | 说明 |
|------|------|
| 典型场景 | 下单后发通知、写 DB + 发事件 |
| 事务保护 | 发消息 + 本地写操作 |
| 回滚后果 | 消息不可见 + DB 不写入 |
| consumer 配置 | `read_committed` |
| producer 配置 | `transaction-id-prefix` + 幂等 |

### 场景二：消费端事务（Consume-Transform-Produce）

```
Kafka(test-tx) ──> [Consumer + Producer] ══(事务)══> Kafka(test-tx-out) ──> 下游
                       ↑ offset 也在事务里
```

| 维度 | 说明 |
|------|------|
| 典型场景 | 流处理、ETL、事件路由 |
| 事务保护 | 消费 offset + 发消息 |
| 回滚后果 | output 消息不可见 + offset 不提交（重消费） |
| consumer 配置 | `read_committed` + `enable-auto-commit=false` |
| producer 配置 | `transaction-id-prefix` + 幂等 |

---

## 附录：快速参考卡片

### Producer 关键参数

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| acks | all | 所有 ISR 确认 |
| retries | Integer.MAX_VALUE | 配合事务/幂等 |
| enable.idempotence | true | 幂等 |
| linger.ms | 5-100 | 批量发送延迟 |
| batch.size | 16384+ | 批量大小 |
| compression.type | snappy/lz4 | 压缩 |
| transaction.timeout.ms | 60000 | 事务超时 |
| max.in.flight.requests.per.connection | 5 | 幂等时 ≤ 5 |

### Consumer 关键参数

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| enable.auto.commit | false | 手动控制 |
| auto.offset.reset | earliest | 无 offset 时从头 |
| max.poll.records | 500 | 批量大小 |
| session.timeout.ms | 45000 | 心跳超时 |
| isolation.level | read_committed | 事务场景 |
| max.poll.interval.ms | 300000 | 单次 poll 处理超时 |

### Spring Kafka 注解速查

| 注解 | 作用 |
|------|------|
| @KafkaListener | 声明消费者方法 |
| @Transactional | 事务边界（需 KafkaTransactionManager） |
| @Payload | 绑定消息体 |
| @Header | 绑定消息头 |
| @Headers | 绑定所有消息头 |

### application.properties 模板（事务全开）

```properties
# producer
spring.kafka.producer.bootstrap-servers=192.168.31.220:9092
spring.kafka.producer.transaction-id-prefix=tx-
spring.kafka.producer.enable-idempotence=true
spring.kafka.producer.acks=all
spring.kafka.producer.retries=2147483647
spring.kafka.producer.transaction-timeout-ms=60000

# consumer
spring.kafka.consumer.bootstrap-servers=192.168.31.220:9092
spring.kafka.consumer.group-id=demo-group
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.isolation-level=read_committed
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.max-poll-records=500

# listener
spring.kafka.listener.type=single
spring.kafka.listener.ack-mode=manual
```

---

> 📌 **复习建议**：按实验顺序（1→10）重跑一遍，每个实验重点关注"验证点"是否和预期一致。踩坑记录在每次操作时对照一遍，避免重复踩。
