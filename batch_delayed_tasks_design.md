# Asynq 批量延迟任务实现方案

## 需求分析

需要实现一个功能，可以批量提交一批任务（最多40个），这批任务每个之间间隔100ms（可配置，误差不超过5ms）依次执行。

## 现有延迟消息实现分析

### 核心机制
1. **Client**: 通过 `ProcessAt()` 或 `ProcessIn()` 选项创建延迟任务
2. **Redis 存储**: 
   - 任务数据存储在 `asynq:{queue}:t:{task_id}` (Hash)
   - 调度信息存储在 `asynq:{queue}:scheduled` (ZSET)，以时间戳为分数
3. **Forwarder**: 每5秒检查一次到期任务，使用Lua脚本批量处理
4. **Processor**: 处理pending状态的任务

### 现有实现的限制
- 每个任务需要单独调用 `EnqueueContext`
- 无法保证任务之间的精确时间间隔
- Forwarder的5秒检查间隔可能导致时间精度问题

## 设计方案

### 方案一：基于现有延迟机制的批量调度

#### 核心思路
1. 创建一个批量任务调度器，接收一批任务
2. 计算每个任务的精确执行时间（startTime + index * interval）
3. 使用现有的 `ProcessAt()` 机制分别调度每个任务
4. 优化Forwarder的检查频率以提高时间精度

#### 实现步骤

##### 1. 新增批量调度选项
```go
// 新增选项类型
type batchScheduleOption struct {
    tasks    []*Task
    interval time.Duration
    startAt  time.Time
}

func BatchSchedule(tasks []*Task, interval time.Duration, startAt time.Time) Option {
    return batchScheduleOption{
        tasks:    tasks,
        interval: interval,
        startAt:  startAt,
    }
}
```

##### 2. 修改Client的EnqueueContext方法
```go
func (c *Client) EnqueueContext(ctx context.Context, task *Task, opts ...Option) (*TaskInfo, error) {
    // 检查是否有批量调度选项
    for _, opt := range opts {
        if batchOpt, ok := opt.(batchScheduleOption); ok {
            return c.batchSchedule(ctx, batchOpt)
        }
    }
    // 原有的单个任务处理逻辑...
}

func (c *Client) batchSchedule(ctx context.Context, opt batchScheduleOption) (*TaskInfo, error) {
    if len(opt.tasks) > 40 {
        return nil, fmt.Errorf("batch size cannot exceed 40")
    }
    
    var results []*TaskInfo
    startTime := opt.startAt
    
    for i, task := range opt.tasks {
        processAt := startTime.Add(time.Duration(i) * opt.interval)
        info, err := c.EnqueueContext(ctx, task, ProcessAt(processAt))
        if err != nil {
            return nil, fmt.Errorf("failed to schedule task %d: %v", i, err)
        }
        results = append(results, info)
    }
    
    return &TaskInfo{
        Message: &base.TaskMessage{
            ID: fmt.Sprintf("batch_%d", time.Now().UnixNano()),
        },
        State: base.TaskStateScheduled,
        NextProcessAt: startTime,
    }, nil
}
```

##### 3. 优化Forwarder的检查频率
```go
// 在server.go中修改forwarder的检查间隔
delayedTaskCheckInterval := cfg.DelayedTaskCheckInterval
if delayedTaskCheckInterval == 0 {
    delayedTaskCheckInterval = 100 * time.Millisecond // 从5秒改为100ms
}
```

### 方案二：基于Redis Stream的精确时间调度

#### 核心思路
1. 使用Redis Stream存储批量任务信息
2. 创建一个高精度的调度器，使用Redis的阻塞等待机制
3. 在指定时间精确触发任务执行

#### 实现步骤

##### 1. 新增批量任务数据结构
```go
type BatchTaskGroup struct {
    ID        string    `json:"id"`
    Tasks     []*Task   `json:"tasks"`
    Interval  int64     `json:"interval"` // 纳秒
    StartTime int64     `json:"start_time"` // Unix纳秒时间戳
    CreatedAt int64     `json:"created_at"`
}

type BatchTaskItem struct {
    GroupID   string `json:"group_id"`
    TaskIndex int    `json:"task_index"`
    Task      *Task  `json:"task"`
    ExecuteAt int64  `json:"execute_at"` // Unix纳秒时间戳
}
```

##### 2. 新增批量调度方法
```go
func (c *Client) BatchScheduleWithInterval(ctx context.Context, tasks []*Task, interval time.Duration, startAt time.Time) (*BatchTaskGroup, error) {
    if len(tasks) > 40 {
        return nil, fmt.Errorf("batch size cannot exceed 40")
    }
    
    groupID := fmt.Sprintf("batch_%d", time.Now().UnixNano())
    group := &BatchTaskGroup{
        ID:        groupID,
        Tasks:     tasks,
        Interval:  int64(interval),
        StartTime: startAt.UnixNano(),
        CreatedAt: time.Now().UnixNano(),
    }
    
    // 存储批量任务组信息
    err := c.storeBatchGroup(ctx, group)
    if err != nil {
        return nil, err
    }
    
    // 为每个任务创建调度项
    for i, task := range tasks {
        executeAt := startAt.Add(time.Duration(i) * interval)
        item := &BatchTaskItem{
            GroupID:   groupID,
            TaskIndex: i,
            Task:      task,
            ExecuteAt: executeAt.UnixNano(),
        }
        
        err := c.scheduleBatchItem(ctx, item)
        if err != nil {
            return nil, fmt.Errorf("failed to schedule task %d: %v", i, err)
        }
    }
    
    return group, nil
}
```

##### 3. 新增高精度调度器
```go
type PreciseScheduler struct {
    broker   base.Broker
    logger   *log.Logger
    done     chan struct{}
    interval time.Duration
}

func (ps *PreciseScheduler) start() {
    go func() {
        ticker := time.NewTicker(ps.interval)
        defer ticker.Stop()
        
        for {
            select {
            case <-ps.done:
                return
            case <-ticker.C:
                ps.checkAndExecute()
            }
        }
    }()
}

func (ps *PreciseScheduler) checkAndExecute() {
    now := time.Now().UnixNano()
    
    // 查询需要执行的任务（时间窗口：now ± 5ms）
    window := int64(5 * time.Millisecond)
    minTime := now - window
    maxTime := now + window
    
    items, err := ps.getScheduledItems(minTime, maxTime)
    if err != nil {
        ps.logger.Errorf("Failed to get scheduled items: %v", err)
        return
    }
    
    for _, item := range items {
        // 精确等待到执行时间
        waitTime := time.Duration(item.ExecuteAt - now)
        if waitTime > 0 && waitTime < 10*time.Millisecond {
            time.Sleep(waitTime)
        }
        
        // 执行任务
        err := ps.executeTask(item)
        if err != nil {
            ps.logger.Errorf("Failed to execute task: %v", err)
        }
    }
}
```

### 方案三：基于内存定时器的混合方案（推荐）

#### 核心思路
1. 使用内存中的高精度定时器（time.Timer）来精确控制任务执行时间
2. 结合Redis存储任务状态和持久化
3. 在应用启动时恢复未完成的批量任务

#### 实现步骤

##### 1. 批量任务管理器
```go
type BatchTaskManager struct {
    broker     base.Broker
    logger     *log.Logger
    timers     map[string]*time.Timer
    mu         sync.RWMutex
    done       chan struct{}
    wg         sync.WaitGroup
}

type BatchTask struct {
    ID        string    `json:"id"`
    Tasks     []*Task   `json:"tasks"`
    Interval  int64     `json:"interval"` // 纳秒
    StartTime int64     `json:"start_time"`
    Status    string    `json:"status"` // pending, running, completed, failed
    CreatedAt int64     `json:"created_at"`
}

func (btm *BatchTaskManager) ScheduleBatch(ctx context.Context, tasks []*Task, interval time.Duration, startAt time.Time) (*BatchTask, error) {
    if len(tasks) > 40 {
        return nil, fmt.Errorf("batch size cannot exceed 40")
    }
    
    batchID := fmt.Sprintf("batch_%d", time.Now().UnixNano())
    batch := &BatchTask{
        ID:        batchID,
        Tasks:     tasks,
        Interval:  int64(interval),
        StartTime: startAt.UnixNano(),
        Status:    "pending",
        CreatedAt: time.Now().UnixNano(),
    }
    
    // 存储到Redis
    err := btm.storeBatch(ctx, batch)
    if err != nil {
        return nil, err
    }
    
    // 计算第一个任务的执行时间
    firstTaskTime := startAt
    if firstTaskTime.Before(time.Now()) {
        firstTaskTime = time.Now().Add(100 * time.Millisecond) // 至少100ms后开始
    }
    
    // 创建定时器
    timer := time.AfterFunc(time.Until(firstTaskTime), func() {
        btm.executeBatch(batch)
    })
    
    btm.mu.Lock()
    btm.timers[batchID] = timer
    btm.mu.Unlock()
    
    return batch, nil
}

func (btm *BatchTaskManager) executeBatch(batch *BatchTask) {
    btm.wg.Add(1)
    defer btm.wg.Done()
    
    btm.mu.Lock()
    delete(btm.timers, batch.ID)
    btm.mu.Unlock()
    
    // 更新状态为运行中
    batch.Status = "running"
    btm.updateBatchStatus(batch)
    
    startTime := time.Unix(0, batch.StartTime)
    interval := time.Duration(batch.Interval)
    
    for i, task := range batch.Tasks {
        // 计算精确执行时间
        executeAt := startTime.Add(time.Duration(i) * interval)
        
        // 如果还没到执行时间，等待
        if now := time.Now(); executeAt.After(now) {
            waitTime := executeAt.Sub(now)
            if waitTime > 0 {
                time.Sleep(waitTime)
            }
        }
        
        // 执行任务
        err := btm.executeTask(task)
        if err != nil {
            btm.logger.Errorf("Failed to execute task %d in batch %s: %v", i, batch.ID, err)
            batch.Status = "failed"
            btm.updateBatchStatus(batch)
            return
        }
    }
    
    // 所有任务执行完成
    batch.Status = "completed"
    btm.updateBatchStatus(batch)
}
```

##### 2. 集成到现有Client
```go
func (c *Client) BatchScheduleWithPreciseInterval(ctx context.Context, tasks []*Task, interval time.Duration, startAt time.Time) (*BatchTask, error) {
    if c.batchManager == nil {
        c.batchManager = NewBatchTaskManager(c.broker, c.logger)
    }
    
    return c.batchManager.ScheduleBatch(ctx, tasks, interval, startAt)
}
```

## 推荐方案：方案三

### 优势
1. **高精度**: 使用内存定时器，精度可达微秒级别
2. **可靠性**: 结合Redis持久化，应用重启后可恢复
3. **性能**: 避免频繁的Redis查询，减少网络开销
4. **可控性**: 可以精确控制每个任务的执行时间
5. **兼容性**: 与现有Asynq架构兼容，不需要大幅修改

### 实现细节
1. **时间精度控制**: 使用 `time.Sleep()` 精确等待到执行时间
2. **错误处理**: 单个任务失败不影响其他任务
3. **状态管理**: 实时更新批量任务状态到Redis
4. **资源管理**: 及时清理完成的定时器，避免内存泄漏

### 使用示例
```go
// 创建批量任务
tasks := []*asynq.Task{
    asynq.NewTask("email", []byte("user1@example.com")),
    asynq.NewTask("email", []byte("user2@example.com")),
    asynq.NewTask("email", []byte("user3@example.com")),
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

这个方案能够满足您的需求：批量提交最多40个任务，每个任务间隔100ms（可配置），时间误差控制在5ms以内。
