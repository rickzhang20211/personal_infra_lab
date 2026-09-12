# Redisson & Redis 实验复盘总结

> 涵盖环境搭建、分布式锁、Lua 脚本、分布式限流、底层线程模型五大板块的全链路技术复盘。

---

## 一、核心问题与业务背景

### 1.1 环境与集成：Spring Boot 4 + Redisson 适配

**痛点**：`redisson-spring-boot-starter` 未适配 Spring Boot 4，无法直接引入。

**解决**：
- 改用纯 `redisson` 依赖 + 手动编写 `RedissonConfig` 配置类
- 通过 `RedissonClient` Bean 连接 Ubuntu 上的 Redis
- 验证持久化、连接可用性，打通后续所有实验的基础环境

**核心收获**：不依赖 Starter 也能精准掌控 Redisson 的初始化与配置。

---

### 1.2 核心场景一：高并发库存扣减

**痛点**：分布式环境下，多实例并发扣库存容易出现 **超卖** 或 **性能瓶颈**。

**探索路径**：对比三种方案
| 方案 | 核心思路 | 问题 |
|------|---------|------|
| 无锁原子操作（`RAtomicLong` + `DECR`） | 利用 Redis 单线程原子性 | 业务复杂时不够灵活，失败有回滚开销 |
| 分布式锁（`RLock` / Redisson 看门狗） | 加锁 → 查库存 → 扣减 → 解锁 | 性能损耗最大（-46%） |
| **Lua 脚本** | 把"查 + 扣"封装成一个原子操作 | **性能最高（1000 QPS，比基线还快）** |

**结论**：Lua 脚本 = 最少网络往返 + 天然原子性 → **生产首选**。

---

### 1.3 核心场景二：分布式限流

**痛点**：单实例本地限流器（如 Guava）在多实例部署下"各限各的"，实际总流量 = 单实例限制 × 实例数，形同虚设。

**解决**：使用 Redisson `RRateLimiter` + `RateType.OVERALL`，基于 Redis 实现 **全局共享令牌桶**，多实例竞争同一份令牌。

**验证**：
- 单实例：50 请求 → 约 9 个通过（桶容量 10）
- 双实例（8080 + 8081）：合计通过 ≈ 17 个（非 9×2），证明共享生效

---

## 二、底层原理与核心架构

### 2.1 Lua 脚本：原子性的基石

**核心原理**：Redis 是单线程执行命令的，`EVAL` 脚本在执行期间不会被其他命令打断。

- **库存扣减**：把"读当前值 → 判断是否够 → 扣减"封装进一段 Lua，避免 `GET` + `DECR` 之间的竞态
- **限流器**：把"回收过期令牌 → 算桶余量 → 判断是否放行 → 记录"封装进一段 Lua
- **价值**：一次网络往返 + 天然原子性，无需额外锁机制

> **一句话**：Redisson 的分布式锁、限流器，底层全部依赖 Lua 脚本保证原子性。

---

### 2.2 Redisson 分布式锁原理（看门狗机制）

| 阶段 | 机制 |
|------|------|
| **加锁** | `SET key UUID:N + PX 30000`（原子命令，值含唯一标识防误删） |
| **看门狗续期** | 业务未执行完时，后台定时（默认每 10s）把锁过期时间重置为 30s |
| **可重入** | Hash 结构记录重入次数 `hincrby` |
| **解锁** | Lua 脚本判断 UUID 匹配 + 重入次数减到 0 才删除 |

**关键点**：看门狗基于 Netty 的 `HashedWheelTimer`（时间轮），**不是 Thread.sleep**。

---

### 2.3 分布式限流器架构（令牌桶 + ZSet 混合）

Redisson `RRateLimiter` 不是简单的"计数器 + 定时补令牌"，而是：

> **Hash 存规则 + String 存剩余令牌 + ZSet 存发放记录 + Lua 懒计算回收**

- **没有后台线程补令牌**：每次 `tryAcquire()` 时由 Lua 脚本"算"该回收多少过期令牌
- **ZSet 作用**：`score = 时间戳`，存储"谁在什么时候拿过令牌"，支持按时间窗口精确回收与计算等待时间

---

### 2.4 Redisson 线程模型（Netty 事件驱动）

| 角色 | 说明 |
|------|------|
| **业务线程**（Tomcat 等） | 调用 Redisson API，提交命令后立即拿到 `RFuture`，不阻塞 |
| **Netty EventLoopGroup** | 少量 I/O 线程（默认 `nettyThreads=32`），负责把所有命令编码为 RESP 协议写入 Socket |
| **业务回调线程池** | 默认 `threads=16`，执行 Redis 响应回来后的用户回调逻辑 |
| **Redis 本身** | 单线程执行 `EVAL` Lua 脚本 |

**核心认知**：`commandExecutor.evalWriteAsync()` 每次 new 的是**命令描述对象（CommandData/Promise）**，不是线程也不是线程池；真正的 EventLoopGroup 和连接池在 `RedissonClient` 创建时一次性初始化并全局复用。

---

## 三、核心调用流程与设计

### 3.1 分布式锁调用链

```
业务线程 tryLock()
   ↓
tryAcquireAsync() → Lua: SETNX + PX
   ↓
成功 → 启动看门狗定时续期（HashedWheelTimer）
   ↓
业务执行...
   ↓
unlock() → Lua: 校验 UUID + 重入计数 → 删除 key
```

### 3.2 限流器 `trySetRate` + `tryAcquire` 完整流程

#### 阶段一：初始化（只一次）

```
trySetRate(OVERALL, 100, 1, SECONDS)
   ↓
trySetRateAsync() → Lua:
   ┌──────────────────────────────┐
   │ hsetnx name rate 100         │
   │ hsetnx name interval 1000    │
   │ hsetnx name type 0           │
   │ (可选) pexpire name ttl      │
   │ return res                   │
   └──────────────────────────────┘
   ↓
仅写入 Hash 配置，不创建 value/permits
```

#### 阶段二：每次获取令牌（核心 Lua）

```
tryAcquire(1)
   ↓
tryAcquireAsync() → Lua 脚本执行：
   │
   ├─ 1. 读配置：HGET rate / interval / type
   │
   ├─ 2. 选 key：OVERALL → value / permits
   │
   ├─ 3. 校验：requestedPermits <= rate
   │
   ├─ 4. 读 currentValue（首次则走"初始化分支"）
   │
   ├─ 5. 回收过期令牌：
   │     ZRANGEBYSCORE permits 0 (now - interval)
   │     → 遍历解包 struct('Bc0I') 累加 released
   │     → ZREMRANGEBYSCORE 删除过期记录
   │     → 精确模式：重新遍历剩余记录算 used，currentValue = rate - used
   │       普通模式：currentValue += released（不超过 rate）
   │     → SET valueName currentValue
   │
   ├─ 6. 判断：
   │     ├─ currentValue < requested（令牌不够）：
   │     │    → ZRANGE 取最早记录 score
   │     │    → return 3 + interval - (now - firstScore)  // 等待时间
   │     │
   │     └─ currentValue >= requested（够）：
   │          → ZADD permits now struct_pack(random, permits)
   │          → DECRBY valueName requested
   │          → return nil  // 成功
   │
   └─ 7. TTL 传播：若 Hash 有 TTL，同步给 value/permits
```

#### 阶段三：Java 侧消费返回值

```
tryAcquire()        → 拿到 nil → true；数字 → false（不阻塞）
tryAcquire(timeout) → 循环：拿到 nil → true；否则 sleep(waitTime)，超时 → false
acquire()           → 循环：拿到 nil → 返回；否则 sleep(waitTime)，永远等
```

> **三者底层 = 同一个 Lua + 不同的 Java 侧等待策略。**

---

### 3.3 `+3` 缓冲的设计意图（细节亮点）

Lua 返回等待时间时：`res = 3 + interval - (now - firstScore)`

- **网络延迟补偿**：客户端 sleep 完再发请求有往返延迟，+3ms 避免卡边界仍失败
- **防忙等**：若返回 0，客户端会立即重试 → 自旋 → 白白消耗 Redis；+3 保证至少 sleep 一次
- **配合 `tryAcquire(timeout)`**：让"晚到一点也没关系"，减少无效重试

---

## 四、关键命令/代码及详解

### 4.1 分布式锁核心 API

```java
RLock lock = redissonClient.getLock("order:lock");
// 尝试加锁，最多等 3s，锁 10s 后自动过期（不启看门狗）
boolean locked = lock.tryLock(3, 10, TimeUnit.SECONDS);
if (locked) {
    try {
        // 业务逻辑
    } finally {
        lock.unlock();
    }
}
```

- **`tryLock(waitTime, leaseTime, unit)`**：等待时间内循环 `SETNX + PX`，成功则启动看门狗
- **看门狗**：内部 Lua 每 10s 执行 `PEXPIRE key 30000`，业务结束前锁不会过期
- **解锁 Lua**：`if redis.call('get', key) == uuid then ... else return 0 end`

---

### 4.2 限流器配置与初始化

```java
RRateLimiter limiter = redissonClient.getRateLimiter("rate-limiter:api:test");
limiter.trySetRate(RateType.OVERALL, 100, 1, RateIntervalUnit.SECONDS);
```

对应底层 Lua：
```lua
redis.call('hsetnx', KEYS[1], 'rate', ARGV[1]);      -- rate = 100
redis.call('hsetnx', KEYS[1], 'interval', ARGV[2]);   -- interval = 1000ms
local res = redis.call('hsetnx', KEYS[1], 'type', ARGV[3]);  -- type = 0 (OVERALL)
if res == 1 and tonumber(ARGV[4]) > 0 then
    redis.call('pexpire', KEYS[1], ARGV[4]);  -- 可选 TTL
end
return res;
```

**关键点**：`hsetnx` 保证只初始化一次，已存在不覆盖；`timeToLive` 默认 0 则不设过期。

---

### 4.3 Redis 中的限流器数据结构

| Key | 类型 | 内容 |
|-----|------|------|
| `rate-limiter:api:test` | Hash | `rate` / `interval` / `type` 配置 |
| `{...}:value` | String | 当前剩余令牌数 |
| `{...}:permits` | ZSet | 发放记录：`score=时间戳, member=random+permits` |

> `{}` 是 Redis Cluster 的 hash tag，保证相关 key 落到同一 slot。

---

### 4.4 ZSet member 的编码格式

```lua
struct.pack('Bc0I', string.len(ARGV[3]), ARGV[3], ARGV[1])
-- B   = 1 字节长度前缀
-- c0  = 随机字节串
-- I   = 4 字节整数（permits 数量）

struct.unpack('Bc0I', v)
-- 反向解包，取出 random 和 permits
```

**为什么用 `struct.pack` 而不直接存字符串**：二进制紧凑，便于遍历累加 permits。

---

### 4.5 观察限流器状态的命令

```bash
# 配置
HGETALL rate-limiter:api:test

# 当前令牌数
GET "{rate-limiter:api:test}:value"

# 发放记录（含时间）
ZRANGE "{rate-limiter:api:test}:permits" 0 -1 WITHSCORES

# 当前窗口内记录数
ZCARD "{rate-limiter:api:test}:permits"
```

---

## 五、避坑指南与调优经验

### 5.1 环境适配

- **Spring Boot 4 + Redisson**：放弃 Starter，手写 `RedissonConfig` + `@Bean RedissonClient`，明确指定 `singleServerConfig()` 的 address、password、database
- **Redis 连接验证**：先 `redis-cli -a xxx PING`，再在 Spring Boot 里写一个 `CommandLineRunner` 执行 `redissonClient.getKeys().count()` 验证

### 5.2 分布式锁

- **务必在 `finally` 中 `unlock()`**，否则业务异常会导致死锁
- **看门狗只在未指定 `leaseTime` 时生效**；指定了 `leaseTime` 则不会自动续期，业务超时必须自行保证
- **锁粒度**：按业务维度（如 `order:{id}`）加锁，避免全局大锁成为瓶颈
- **Redisson 锁性能损耗 ~46%**（相对无锁），仅在需要"跨服务/长事务/多步骤"时使用

### 5.3 限流器

- **`trySetRate` 只初始化一次**：多实例启动时只有第一个成功写入，其余沿用已有配置；要改配置需先 `delete()`
- **`RateType.OVERALL` vs `PER_CLIENT`**：
  - `OVERALL`：全局共享令牌桶 → 多实例总流量受限（**生产推荐**）
  - `PER_CLIENT`：按 Redisson 实例 ID 独立限流 → 每实例各自限制
- **不要在 Netty EventLoop 回调里做重活**（`thenAccept` 默认在 I/O 线程），用 `thenAcceptAsync(..., businessExecutor)` 切回业务线程池
- **Lua 脚本不能写死循环 / 不能太重**：Redis 单线程执行期间阻塞所有其他命令

### 5.4 线程模型认知

- **每次 new `CommandExecutor` ≠ 新建线程池**：只是 new 命令对象，真正的 EventLoopGroup 全局复用
- **同步方法 `tryAcquire()` 的阻塞**：业务线程在 `Future.get()` / `join()` 上等，I/O 仍是 Netty 线程
- **回调默认在 Netty EventLoop 线程**：做 DB 查询、sleep 会堵住所有 Redis 命令收发 → 必须切线程

### 5.5 压测与验证方法论

- **用 ab + 多实例验证分布式限流**：单实例通过数 × 实例数 ≠ 双实例通过数，才能证明共享生效
- **`Failed requests (Length)` 不是 HTTP 错误**：是 ab 发现返回 body 长度不一致（✅ 和 ❌ 长度不同），实际是限流生效的标志
- **精确统计通过/拒绝数**：
  ```powershell
  1..50 | ForEach-Object { (Invoke-WebRequest ...).Content } | Out-File result.txt
  (Get-Content result.txt | Select-String "✅").Count
  (Get-Content result.txt | Select-String "❌").Count
  ```

### 5.6 选型速查

| 需求 | 推荐方案 |
|------|---------|
| 简单计数/标志 | `RAtomicLong` / `RBucket` |
| 高并发库存扣减 | **Lua 脚本**（一次往返 + 原子） |
| 跨服务/长事务互斥 | `RLock`（看门狗自动续期） |
| 接口/用户/IP 限流 | `RRateLimiter`（令牌桶） |
| 防缓存穿透 | `RBloomFilter` |
| 事件驱动解耦 | `RTopic` Pub/Sub |

---

## 六、关键结论速记

> 1. **Lua 脚本** = Redisson 分布式能力的原子性基石（锁、限流、库存）
> 2. **看门狗** = Netty 时间轮 + Lua 续期，不是 Thread.sleep
> 3. **限流器** = Hash 配置 + String 令牌数 + ZSet 发放记录 + Lua 懒回收
> 4. **`+3`** = 网络延迟补偿 + 防忙等 + 配合超时等待的工程经验值
> 5. **线程模型** = 业务线程提交 → Netty EventLoop 发命令 → Redis 单线程执行 → EventLoop 回传 → 业务线程池回调
> 6. **new Executor ≠ new 线程**：对象轻量，EventLoopGroup 全局复用

---

*复盘周期：基于 personal_infra_lab 实验全程*
*核心技术栈：Spring Boot 4 + Redisson 3.x + Redis 7 + Apache Bench*
