# Asynq 延迟消息实现流程图

## 1. 现有延迟消息实现流程

### 1.1 延迟任务创建流程

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

### 1.2 Redis 数据结构

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

### 1.3 任务状态流转

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

### 1.4 Forwarder 详细流程

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

## 2. 批量延迟任务设计方案

### 2.1 推荐方案：基于内存定时器的混合方案

```mermaid
sequenceDiagram
    participant U as 用户
    participant C as Client
    participant BTM as BatchTaskManager
    participant R as Redis
    participant T as Timer
    
    U->>C: BatchScheduleWithPreciseInterval(tasks, interval, startAt)
    C->>BTM: ScheduleBatch(tasks, interval, startAt)
    BTM->>BTM: 验证任务数量 ≤ 40
    BTM->>R: 存储批量任务组信息
    BTM->>T: 创建第一个任务的定时器
    BTM-->>C: 返回 BatchTask
    C-->>U: 返回批量任务信息
    
    Note over T: 到达第一个任务执行时间
    T->>BTM: 触发 executeBatch
    BTM->>BTM: 更新状态为 running
    
    loop 遍历每个任务
        BTM->>BTM: 计算精确执行时间
        BTM->>BTM: 等待到执行时间
        BTM->>BTM: 执行任务
        BTM->>R: 更新任务状态
    end
    
    BTM->>R: 更新批量任务状态为 completed
```

### 2.2 批量任务管理器架构

```mermaid
graph TB
    subgraph "BatchTaskManager"
        A[批量任务管理器] --> B[任务验证]
        A --> C[Redis 存储]
        A --> D[定时器管理]
        A --> E[任务执行]
        
        B --> B1[检查任务数量 ≤ 40]
        B --> B2[验证时间间隔]
        
        C --> C1[存储批量任务组]
        C --> C2[更新任务状态]
        C --> C3[持久化恢复]
        
        D --> D1[创建第一个任务定时器]
        D --> D2[管理定时器生命周期]
        D --> D3[清理完成的任务]
        
        E --> E1[精确时间控制]
        E --> E2[顺序执行任务]
        E --> E3[错误处理]
    end
    
    subgraph "外部组件"
        F[Client] --> A
        G[Redis] --> C
        H[Logger] --> A
    end
```

### 2.3 批量任务执行流程

```mermaid
flowchart TD
    A[接收批量任务] --> B{任务数量 ≤ 40?}
    B -->|否| C[返回错误]
    B -->|是| D[计算执行时间表]
    
    D --> E[存储到 Redis]
    E --> F[创建第一个任务定时器]
    F --> G[等待第一个任务时间]
    
    G --> H[开始执行批量任务]
    H --> I[更新状态为 running]
    
    I --> J[遍历任务列表]
    J --> K[计算当前任务执行时间]
    K --> L{是否到达执行时间?}
    L -->|否| M[精确等待]
    L -->|是| N[执行任务]
    
    M --> L
    N --> O{任务执行成功?}
    O -->|否| P[更新状态为 failed]
    O -->|是| Q[继续下一个任务]
    
    Q --> R{还有更多任务?}
    R -->|是| J
    R -->|否| S[更新状态为 completed]
    
    P --> T[清理资源]
    S --> T
    T --> U[结束]
```

### 2.4 时间精度控制机制

```mermaid
graph LR
    subgraph "时间精度控制"
        A[任务调度时间] --> B[计算等待时间]
        B --> C{等待时间 > 0?}
        C -->|是| D[time.Sleep 精确等待]
        C -->|否| E[立即执行]
        D --> F[执行任务]
        E --> F
        F --> G[记录实际执行时间]
        G --> H[计算时间误差]
    end
    
    subgraph "误差控制"
        I[目标误差 ≤ 5ms] --> J[监控实际误差]
        J --> K{误差超限?}
        K -->|是| L[记录警告日志]
        K -->|否| M[正常执行]
    end
```

### 2.5 错误处理和恢复机制

```mermaid
stateDiagram-v2
    [*] --> Pending: 创建批量任务
    Pending --> Running: 开始执行
    Running --> Completed: 所有任务成功
    Running --> Failed: 某个任务失败
    Running --> PartialCompleted: 部分任务完成
    
    Failed --> [*]: 清理资源
    Completed --> [*]: 清理资源
    PartialCompleted --> [*]: 清理资源
    
    note right of Running: 支持任务级别的错误处理
    note right of Failed: 记录失败原因和位置
    note right of PartialCompleted: 记录完成的任务数量
```

## 3. 实现对比

### 3.1 方案对比表

| 特性 | 现有延迟机制 | 方案一：批量调度 | 方案二：Redis Stream | 方案三：内存定时器 |
|------|-------------|-----------------|---------------------|-------------------|
| 时间精度 | 5秒检查间隔 | 100ms检查间隔 | 微秒级 | 微秒级 |
| 批量支持 | 不支持 | 支持 | 支持 | 支持 |
| 实现复杂度 | 简单 | 中等 | 复杂 | 中等 |
| 性能 | 中等 | 中等 | 高 | 高 |
| 可靠性 | 高 | 高 | 中等 | 高 |
| 资源消耗 | 低 | 中等 | 高 | 中等 |

### 3.2 推荐方案优势

```mermaid
mindmap
  root((方案三优势))
    高精度
      微秒级时间控制
      误差 ≤ 5ms
      精确等待机制
    可靠性
      Redis 持久化
      应用重启恢复
      状态实时更新
    性能
      内存定时器
      减少 Redis 查询
      批量处理优化
    兼容性
      现有架构兼容
      渐进式实现
      向后兼容
    可控性
      精确时间控制
      错误处理完善
      资源管理优化
```

## 4. 使用示例

### 4.1 基本使用

```go
// 创建批量任务
tasks := []*asynq.Task{
    asynq.NewTask("send_email", []byte("user1@example.com")),
    asynq.NewTask("send_email", []byte("user2@example.com")),
    asynq.NewTask("send_email", []byte("user3@example.com")),
    // ... 最多40个任务
}

// 批量调度，每个任务间隔100ms
batch, err := client.BatchScheduleWithPreciseInterval(
    context.Background(),
    tasks,
    100*time.Millisecond,
    time.Now().Add(1*time.Second), // 1秒后开始
)
```

### 4.2 配置选项

```go
// 自定义配置
config := &BatchTaskConfig{
    MaxBatchSize:     40,
    DefaultInterval:  100 * time.Millisecond,
    MaxTimeError:     5 * time.Millisecond,
    RetryOnFailure:   true,
    MaxRetries:       3,
}

client := asynq.NewClient(redisOpt, asynq.WithBatchConfig(config))
```

这个设计方案能够满足您的需求：批量提交最多40个任务，每个任务间隔100ms（可配置），时间误差控制在5ms以内，同时保持与现有Asynq架构的兼容性。
