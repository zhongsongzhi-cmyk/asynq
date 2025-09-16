# Asynq 延迟消息实现分析

## 概述

Asynq 框架的延迟消息实现基于 Redis 的有序集合（ZSET）和定时轮询机制。延迟消息通过以下核心组件实现：

1. **Client** - 负责创建和调度延迟任务
2. **Forwarder** - 负责将到期的延迟任务从 scheduled 状态转移到 pending 状态
3. **Redis 存储** - 使用 ZSET 存储延迟任务，以时间戳作为分数
4. **Processor** - 负责处理 pending 状态的任务

## 核心组件分析

### 1. 延迟任务创建

当用户使用 `ProcessAt()` 或 `ProcessIn()` 选项创建任务时：

```go
// 在 client.go 的 EnqueueContext 方法中
if opt.processAt.After(now) {
    err = c.schedule(ctx, msg, opt.processAt, opt.uniqueTTL)
    state = base.TaskStateScheduled
}
```

### 2. Redis 存储结构

延迟任务在 Redis 中的存储结构：

- **任务数据**: `asynq:{queue}:t:{task_id}` (Hash)
  - `msg`: 序列化的任务消息
  - `state`: "scheduled"
  - `unique_key`: 唯一键（如果设置了唯一性）

- **调度集合**: `asynq:{queue}:scheduled` (ZSET)
  - 成员: task_id
  - 分数: process_at 时间戳（Unix 时间）

### 3. Forwarder 机制

Forwarder 是一个后台 goroutine，定期检查到期的延迟任务：

```go
// forwarder.go
func (f *forwarder) exec() {
    if err := f.broker.ForwardIfReady(f.queues...); err != nil {
        f.logger.Errorf("Failed to forward scheduled tasks: %v", err)
    }
}
```

### 4. 任务转发逻辑

Forwarder 使用 Lua 脚本批量处理到期任务：

```lua
-- forwardCmd 脚本
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, id in ipairs(ids) do
    local taskKey = ARGV[2] .. id
    local group = redis.call("HGET", taskKey, "group")
    if group and group ~= '' then
        -- 移动到聚合组
        redis.call("ZADD", ARGV[4] .. group, ARGV[1], id)
        redis.call("ZREM", KEYS[1], id)
        redis.call("HSET", taskKey, "state", "aggregating")
    else
        -- 移动到待处理队列
        redis.call("LPUSH", KEYS[2], id)
        redis.call("ZREM", KEYS[1], id)
        redis.call("HSET", taskKey, "state", "pending", "pending_since", ARGV[3])
    end
end
```

## 任务状态流转

1. **Scheduled** → **Pending**: 通过 Forwarder 定时检查
2. **Pending** → **Active**: 通过 Processor 获取任务
3. **Active** → **Completed/Retry/Archived**: 根据处理结果

## 流程图

### 1. 延迟任务创建流程

```mermaid
sequenceDiagram
    participant U as 用户
    participant C as Client
    participant R as Redis
    participant F as Forwarder
    participant P as Processor
    
    U->>C: EnqueueContext(task, ProcessAt(time))
    C->>C: 检查 processAt > now
    C->>R: Schedule(msg, processAt)
    R->>R: 存储任务数据到 Hash
    R->>R: 添加到 scheduled ZSET
    R-->>C: 返回成功
    C-->>U: 返回 TaskInfo
    
    Note over F: 定时器每 5 秒触发
    F->>R: ForwardIfReady()
    R->>R: 执行 Lua 脚本
    R->>R: 查询到期任务
    R->>R: 移动到 pending 队列
    R-->>F: 返回处理数量
    
    P->>R: Dequeue()
    R-->>P: 返回任务
    P->>P: 执行任务处理
    P->>R: 更新任务状态
```

### 2. Redis 数据结构

```mermaid
graph LR
    subgraph "Redis 存储结构"
        A[asynq:queue:t:task_id<br/>Hash] --> A1[msg: 序列化任务]
        A --> A2[state: scheduled]
        A --> A3[unique_key: 唯一键]
        
        B[asynq:queue:scheduled<br/>ZSET] --> B1[成员: task_id]
        B --> B2[分数: process_at 时间戳]
        
        C[asynq:queue:pending<br/>List] --> C1[待处理任务 ID 列表]
    end
```

### 3. 任务状态流转

```mermaid
stateDiagram-v2
    [*] --> Scheduled: 创建延迟任务
    Scheduled --> Pending: Forwarder 检查到期
    Scheduled --> Aggregating: 有 group 的到期任务
    Pending --> Active: Processor 获取任务
    Aggregating --> Active: 聚合完成后
    Active --> Completed: 处理成功
    Active --> Retry: 处理失败，可重试
    Active --> Archived: 处理失败，不可重试
    Retry --> Scheduled: 重新调度
    Completed --> [*]: 任务完成
    Archived --> [*]: 任务归档
```

### 4. Forwarder 详细流程

```mermaid
flowchart TD
    A[Forwarder 启动] --> B[设置定时器 5 秒]
    B --> C[等待定时器触发]
    C --> D[调用 ForwardIfReady]
    D --> E[遍历所有队列]
    E --> F[对每个队列调用 forwardAll]
    F --> G[检查 scheduled 和 retry 集合]
    G --> H[执行 Lua 脚本]
    H --> I[查询到期任务]
    I --> J{任务是否有 group}
    J -->|有| K[移动到聚合组]
    J -->|无| L[移动到 pending 队列]
    K --> M[更新状态为 aggregating]
    L --> N[更新状态为 pending]
    M --> O[继续处理下一批]
    N --> O
    O --> P{还有更多任务?}
    P -->|是| H
    P -->|否| Q[等待下次定时器]
    Q --> C
```

### 5. Lua 脚本执行逻辑

```mermaid
flowchart TD
    A[Lua 脚本开始] --> B[ZRANGEBYSCORE 查询到期任务]
    B --> C[限制最多 100 个任务]
    C --> D[遍历每个任务 ID]
    D --> E[获取任务详细信息]
    E --> F{检查任务是否有 group}
    F -->|有 group| G[添加到聚合组 ZSET]
    F -->|无 group| H[添加到 pending List]
    G --> I[从 scheduled 移除]
    H --> I
    I --> J[更新任务状态]
    J --> K[继续下一个任务]
    K --> L{还有任务?}
    L -->|是| D
    L -->|否| M[返回处理数量]
    M --> N[脚本结束]
```

## 关键特性

### 1. 批量处理
- 每次最多处理 100 个到期任务，避免长时间阻塞
- 使用 Lua 脚本保证原子性操作

### 2. 定时轮询
- 默认每 5 秒检查一次到期任务
- 可通过 `DelayedTaskCheckInterval` 配置

### 3. 状态管理
- 任务状态存储在 Redis Hash 中
- 支持 scheduled、pending、active、completed 等状态

### 4. 唯一性支持
- 支持任务的唯一性约束
- 使用 Redis SET 命令的 NX 选项实现

### 5. 聚合支持
- 支持任务聚合功能
- 相同 group 的任务会被聚合处理

## 性能优化

1. **批量操作**: 使用 Lua 脚本批量处理任务
2. **索引优化**: 使用 ZSET 的时间戳索引快速查询
3. **内存管理**: 及时清理已完成的任务数据
4. **并发控制**: 使用 Redis 的原子操作保证一致性

## 总结

Asynq 的延迟消息实现是一个高效、可靠的解决方案，通过 Redis 的有序集合和定时轮询机制，实现了精确的延迟任务调度。其设计考虑了性能、可靠性和扩展性，是一个成熟的分布式任务队列实现。
