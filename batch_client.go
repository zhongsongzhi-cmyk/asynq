// Copyright 2024. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq/internal/log"
	"github.com/redis/go-redis/v9"
)

// BatchClient 批量任务客户端
type BatchClient struct {
	*Client
	batchManager *BatchTaskManager
	redis        redis.UniversalClient
	config       *BatchTaskConfig
}

// NewBatchClient 创建新的批量任务客户端
func NewBatchClient(r RedisConnOpt, config *BatchTaskConfig, handler Handler) *BatchClient {
	client := NewClient(r)

	redisClient, ok := r.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("asynq: unsupported RedisConnOpt type %T", r))
	}

	if config == nil {
		config = DefaultBatchTaskConfig()
	}

	batchManager := NewBatchTaskManager(
		client.broker,
		redisClient,
		log.NewLogger(nil),
		config,
		handler,
	)

	return &BatchClient{
		Client:       client,
		batchManager: batchManager,
		redis:        redisClient,
		config:       config,
	}
}

// NewBatchClientFromRedisClient 从Redis客户端创建批量任务客户端
func NewBatchClientFromRedisClient(c redis.UniversalClient, config *BatchTaskConfig, handler Handler) *BatchClient {
	client := NewClientFromRedisClient(c)

	if config == nil {
		config = DefaultBatchTaskConfig()
	}

	batchManager := NewBatchTaskManager(
		client.broker,
		c,
		log.NewLogger(nil),
		config,
		handler,
	)

	return &BatchClient{
		Client:       client,
		batchManager: batchManager,
		redis:        c,
		config:       config,
	}
}

// BatchScheduleWithPreciseInterval 批量调度任务，指定精确间隔
func (bc *BatchClient) BatchScheduleWithPreciseInterval(ctx context.Context, tasks []*Task, interval time.Duration, startAt time.Time, opts ...BatchOption) (*BatchTask, error) {
	if bc.batchManager == nil {
		return nil, fmt.Errorf("batch manager not initialized")
	}

	return bc.batchManager.ScheduleBatch(ctx, tasks, interval, startAt, opts...)
}

// BatchScheduleWithTemplate 使用模板批量调度任务
func (bc *BatchClient) BatchScheduleWithTemplate(ctx context.Context, template *BatchTaskTemplate, interval time.Duration, startAt time.Time, opts ...BatchOption) (*BatchTask, error) {
	tasks, err := template.GenerateTasks()
	if err != nil {
		return nil, fmt.Errorf("failed to generate tasks from template: %v", err)
	}

	return bc.BatchScheduleWithPreciseInterval(ctx, tasks, interval, startAt, opts...)
}

// BatchScheduleWithAdvancedOptions 使用高级选项批量调度任务
func (bc *BatchClient) BatchScheduleWithAdvancedOptions(ctx context.Context, tasks []*Task, scheduleOption *BatchScheduleOption, opts ...BatchOption) (*BatchTask, error) {
	if scheduleOption == nil {
		scheduleOption = NewBatchScheduleOption()
	}

	// 使用基础间隔
	interval := scheduleOption.intervalOption.CalculateInterval(0, nil)

	return bc.BatchScheduleWithPreciseInterval(ctx, tasks, interval, scheduleOption.startTime, opts...)
}

// GetBatchTask 获取批量任务信息
func (bc *BatchClient) GetBatchTask(batchID string) (*BatchTaskInfo, error) {
	if bc.batchManager == nil {
		return nil, fmt.Errorf("batch manager not initialized")
	}

	return bc.batchManager.GetBatchInfo(batchID)
}

// CancelBatchTask 取消批量任务
func (bc *BatchClient) CancelBatchTask(batchID string) error {
	if bc.batchManager == nil {
		return fmt.Errorf("batch manager not initialized")
	}

	return bc.batchManager.CancelBatch(batchID)
}

// ListBatchTasks 列出批量任务
func (bc *BatchClient) ListBatchTasks(status string, limit, offset int) ([]*BatchTaskInfo, error) {
	if bc.batchManager == nil {
		return nil, fmt.Errorf("batch manager not initialized")
	}

	return bc.batchManager.ListBatchesByStatus(status, limit, offset)
}

// GetBatchStats 获取批量任务统计信息
func (bc *BatchClient) GetBatchStats() (map[string]int64, error) {
	if bc.batchManager == nil {
		return nil, fmt.Errorf("batch manager not initialized")
	}

	ctx := context.Background()
	return bc.batchManager.GetBatchStats(ctx)
}

// GetBatchMetrics 获取批量任务指标
func (bc *BatchClient) GetBatchMetrics() *BatchMetrics {
	if bc.batchManager == nil {
		return nil
	}

	return bc.batchManager.GetMetrics()
}

// RecoverPendingBatches 恢复未完成的批量任务
func (bc *BatchClient) RecoverPendingBatches() error {
	if bc.batchManager == nil {
		return fmt.Errorf("batch manager not initialized")
	}

	return bc.batchManager.RecoverPendingBatches()
}

// Close 关闭批量任务客户端
func (bc *BatchClient) Close() error {
	if bc.batchManager != nil {
		bc.batchManager.Shutdown()
	}

	return bc.Client.Close()
}

// CreateBatchFromPayloads 从载荷列表创建批量任务
func CreateBatchFromPayloads(taskType string, payloads [][]byte, opts ...Option) ([]*Task, error) {
	if len(payloads) == 0 {
		return nil, fmt.Errorf("payloads cannot be empty")
	}

	tasks := make([]*Task, len(payloads))
	for i, payload := range payloads {
		tasks[i] = NewTask(taskType, payload, opts...)
	}

	return tasks, nil
}

// CreateBatchFromStrings 从字符串列表创建批量任务
func CreateBatchFromStrings(taskType string, data []string, opts ...Option) ([]*Task, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	tasks := make([]*Task, len(data))
	for i, str := range data {
		tasks[i] = NewTask(taskType, []byte(str), opts...)
	}

	return tasks, nil
}

// CreateBatchFromInterface 从接口列表创建批量任务
func CreateBatchFromInterface(taskType string, data []interface{}, opts ...Option) ([]*Task, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}

	tasks := make([]*Task, len(data))
	for i, item := range data {
		payload, err := encodePayload(item)
		if err != nil {
			return nil, fmt.Errorf("failed to encode item %d: %v", i, err)
		}
		tasks[i] = NewTask(taskType, payload, opts...)
	}

	return tasks, nil
}

// BatchTaskBuilder 批量任务构建器
type BatchTaskBuilder struct {
	taskType string
	tasks    []*Task
	options  []Option
}

// NewBatchTaskBuilder 创建批量任务构建器
func NewBatchTaskBuilder(taskType string) *BatchTaskBuilder {
	return &BatchTaskBuilder{
		taskType: taskType,
		tasks:    make([]*Task, 0),
		options:  make([]Option, 0),
	}
}

// AddTask 添加任务
func (btb *BatchTaskBuilder) AddTask(payload []byte, opts ...Option) *BatchTaskBuilder {
	allOpts := append(btb.options, opts...)
	btb.tasks = append(btb.tasks, NewTask(btb.taskType, payload, allOpts...))
	return btb
}

// AddTaskString 添加字符串任务
func (btb *BatchTaskBuilder) AddTaskString(data string, opts ...Option) *BatchTaskBuilder {
	return btb.AddTask([]byte(data), opts...)
}

// AddTaskInterface 添加接口任务
func (btb *BatchTaskBuilder) AddTaskInterface(data interface{}, opts ...Option) *BatchTaskBuilder {
	payload, err := encodePayload(data)
	if err != nil {
		// 这里可以考虑返回错误，但为了链式调用，暂时忽略
		return btb
	}
	return btb.AddTask(payload, opts...)
}

// WithOptions 设置全局选项
func (btb *BatchTaskBuilder) WithOptions(opts ...Option) *BatchTaskBuilder {
	btb.options = append(btb.options, opts...)
	return btb
}

// Build 构建任务列表
func (btb *BatchTaskBuilder) Build() []*Task {
	return btb.tasks
}

// BuildAndSchedule 构建并调度任务
func (btb *BatchTaskBuilder) BuildAndSchedule(bc *BatchClient, ctx context.Context, interval time.Duration, startAt time.Time, opts ...BatchOption) (*BatchTask, error) {
	if len(btb.tasks) == 0 {
		return nil, fmt.Errorf("no tasks to schedule")
	}

	return bc.BatchScheduleWithPreciseInterval(ctx, btb.tasks, interval, startAt, opts...)
}

// BatchTaskScheduler 批量任务调度器（高级接口）
type BatchTaskScheduler struct {
	client           *BatchClient
	defaultInterval  time.Duration
	defaultQueue     string
	defaultStartTime time.Time
	defaultOptions   []BatchOption
}

// NewBatchTaskScheduler 创建批量任务调度器
func NewBatchTaskScheduler(client *BatchClient) *BatchTaskScheduler {
	return &BatchTaskScheduler{
		client:           client,
		defaultInterval:  100 * time.Millisecond,
		defaultQueue:     "default",
		defaultStartTime: time.Now().Add(1 * time.Second),
		defaultOptions:   make([]BatchOption, 0),
	}
}

// SetDefaults 设置默认值
func (bts *BatchTaskScheduler) SetDefaults(interval time.Duration, queue string, startTime time.Time, opts ...BatchOption) *BatchTaskScheduler {
	bts.defaultInterval = interval
	bts.defaultQueue = queue
	bts.defaultStartTime = startTime
	bts.defaultOptions = opts
	return bts
}

// ScheduleEmails 调度邮件发送任务
func (bts *BatchTaskScheduler) ScheduleEmails(ctx context.Context, emails []string, opts ...BatchOption) (*BatchTask, error) {
	tasks, err := CreateBatchFromStrings("send_email", emails)
	if err != nil {
		return nil, err
	}

	allOpts := append(bts.defaultOptions, opts...)
	allOpts = append(allOpts, BatchQueue(bts.defaultQueue))

	return bts.client.BatchScheduleWithPreciseInterval(
		ctx, tasks, bts.defaultInterval, bts.defaultStartTime, allOpts...)
}

// ScheduleNotifications 调度通知任务
func (bts *BatchTaskScheduler) ScheduleNotifications(ctx context.Context, notifications []interface{}, opts ...BatchOption) (*BatchTask, error) {
	tasks, err := CreateBatchFromInterface("send_notification", notifications)
	if err != nil {
		return nil, err
	}

	allOpts := append(bts.defaultOptions, opts...)
	allOpts = append(allOpts, BatchQueue(bts.defaultQueue))

	return bts.client.BatchScheduleWithPreciseInterval(
		ctx, tasks, bts.defaultInterval, bts.defaultStartTime, allOpts...)
}

// ScheduleCustomTasks 调度自定义任务
func (bts *BatchTaskScheduler) ScheduleCustomTasks(ctx context.Context, taskType string, payloads [][]byte, opts ...BatchOption) (*BatchTask, error) {
	tasks, err := CreateBatchFromPayloads(taskType, payloads)
	if err != nil {
		return nil, err
	}

	allOpts := append(bts.defaultOptions, opts...)
	allOpts = append(allOpts, BatchQueue(bts.defaultQueue))

	return bts.client.BatchScheduleWithPreciseInterval(
		ctx, tasks, bts.defaultInterval, bts.defaultStartTime, allOpts...)
}

// ScheduleWithTemplate 使用模板调度任务
func (bts *BatchTaskScheduler) ScheduleWithTemplate(ctx context.Context, template *BatchTaskTemplate, opts ...BatchOption) (*BatchTask, error) {
	allOpts := append(bts.defaultOptions, opts...)

	return bts.client.BatchScheduleWithTemplate(
		ctx, template, bts.defaultInterval, bts.defaultStartTime, allOpts...)
}

// ScheduleStaggered 调度错开的任务（不同的间隔）
func (bts *BatchTaskScheduler) ScheduleStaggered(ctx context.Context, tasks []*Task, intervals []time.Duration, opts ...BatchOption) ([]*BatchTask, error) {
	if len(tasks) != len(intervals) {
		return nil, fmt.Errorf("tasks and intervals length mismatch: %d vs %d", len(tasks), len(intervals))
	}

	batches := make([]*BatchTask, len(tasks))
	startTime := bts.defaultStartTime

	for i, task := range tasks {
		singleTask := []*Task{task}
		allOpts := append(bts.defaultOptions, opts...)

		batch, err := bts.client.BatchScheduleWithPreciseInterval(
			ctx, singleTask, intervals[i], startTime, allOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to schedule task %d: %v", i, err)
		}

		batches[i] = batch

		// 下一个任务的开始时间 = 当前开始时间 + 当前间隔
		startTime = startTime.Add(intervals[i])
	}

	return batches, nil
}

// encodePayload 编码载荷
func encodePayload(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}
