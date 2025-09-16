// Copyright 2024. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/redis/go-redis/v9"
)

// BatchTaskConfig 批量任务配置
type BatchTaskConfig struct {
	MaxBatchSize    int           `json:"max_batch_size"`
	DefaultInterval time.Duration `json:"default_interval"`
	MaxTimeError    time.Duration `json:"max_time_error"`
	RetryOnFailure  bool          `json:"retry_on_failure"`
	MaxRetries      int           `json:"max_retries"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
	BatchTTL        time.Duration `json:"batch_ttl"`
}

// DefaultBatchTaskConfig 返回默认的批量任务配置
func DefaultBatchTaskConfig() *BatchTaskConfig {
	return &BatchTaskConfig{
		MaxBatchSize:    40,
		DefaultInterval: 100 * time.Millisecond,
		MaxTimeError:    5 * time.Millisecond,
		RetryOnFailure:  true,
		MaxRetries:      3,
		CleanupInterval: 1 * time.Hour,
		BatchTTL:        24 * time.Hour,
	}
}

// BatchTask 批量任务
type BatchTask struct {
	ID          string  `json:"id"`
	Tasks       []*Task `json:"tasks"`
	Interval    int64   `json:"interval"`   // 纳秒
	StartTime   int64   `json:"start_time"` // Unix纳秒时间戳
	Status      string  `json:"status"`     // pending, running, completed, failed, cancelled
	CreatedAt   int64   `json:"created_at"`
	UpdatedAt   int64   `json:"updated_at"`
	ErrorMsg    string  `json:"error_msg,omitempty"`
	RetryCount  int     `json:"retry_count"`
	CompletedAt int64   `json:"completed_at,omitempty"`
	Queue       string  `json:"queue"`
}

// BatchTaskInfo 批量任务信息
type BatchTaskInfo struct {
	BatchTask
	NextTaskIndex    int   `json:"next_task_index"`
	CompletedCount   int   `json:"completed_count"`
	FailedCount      int   `json:"failed_count"`
	TotalTasks       int   `json:"total_tasks"`
	EstimatedEndTime int64 `json:"estimated_end_time"`
}

// BatchTaskExecution 批量任务执行状态
type BatchTaskExecution struct {
	BatchID      string                `json:"batch_id"`
	CurrentIndex int                   `json:"current_index"`
	StartTime    time.Time             `json:"start_time"`
	LastTaskTime time.Time             `json:"last_task_time"`
	TimeErrors   []time.Duration       `json:"time_errors"`
	TaskResults  []TaskExecutionResult `json:"task_results"`
}

// TaskExecutionResult 任务执行结果
type TaskExecutionResult struct {
	Index       int           `json:"index"`
	TaskID      string        `json:"task_id"`
	Status      string        `json:"status"` // success, failed, skipped
	Error       string        `json:"error,omitempty"`
	ExecutedAt  int64         `json:"executed_at"`
	ScheduledAt int64         `json:"scheduled_at"`
	TimeError   time.Duration `json:"time_error"`
	Duration    time.Duration `json:"duration"`
}

// BatchTaskManager 批量任务管理器
type BatchTaskManager struct {
	broker  base.Broker
	logger  *log.Logger
	config  *BatchTaskConfig
	redis   redis.UniversalClient
	handler Handler // 新增：任务处理器

	// 运行时状态
	timers     map[string]*time.Timer
	executions map[string]*BatchTaskExecution
	mu         sync.RWMutex

	// 控制通道
	done chan struct{}
	wg   sync.WaitGroup

	// 监控和统计
	metrics *BatchMetrics
}

// BatchMetrics 批量任务指标
type BatchMetrics struct {
	TotalBatches     int64         `json:"total_batches"`
	CompletedBatches int64         `json:"completed_batches"`
	FailedBatches    int64         `json:"failed_batches"`
	AvgTimeError     time.Duration `json:"avg_time_error"`
	MaxTimeError     time.Duration `json:"max_time_error"`
	mu               sync.RWMutex
}

// NewBatchTaskManager 创建新的批量任务管理器
func NewBatchTaskManager(broker base.Broker, redis redis.UniversalClient, logger *log.Logger, config *BatchTaskConfig, handler Handler) *BatchTaskManager {
	if config == nil {
		config = DefaultBatchTaskConfig()
	}

	if logger == nil {
		logger = log.NewLogger(nil)
	}

	btm := &BatchTaskManager{
		broker:     broker,
		redis:      redis,
		logger:     logger,
		config:     config,
		handler:    handler,
		timers:     make(map[string]*time.Timer),
		executions: make(map[string]*BatchTaskExecution),
		done:       make(chan struct{}),
		metrics:    &BatchMetrics{},
	}

	// 启动清理协程
	go btm.cleanupRoutine()

	return btm
}

// ScheduleBatch 调度批量任务
func (btm *BatchTaskManager) ScheduleBatch(ctx context.Context, tasks []*Task, interval time.Duration, startAt time.Time, opts ...BatchOption) (*BatchTask, error) {
	// 验证输入参数
	if err := btm.validateBatchParams(tasks, interval); err != nil {
		return nil, err
	}

	// 处理选项
	options := &batchOptions{
		queue: base.DefaultQueueName,
	}
	for _, opt := range opts {
		opt.apply(options)
	}

	// 创建批量任务
	batchID := btm.generateBatchID()
	batch := &BatchTask{
		ID:        batchID,
		Tasks:     tasks,
		Interval:  int64(interval),
		StartTime: startAt.UnixNano(),
		Status:    "pending",
		CreatedAt: time.Now().UnixNano(),
		UpdatedAt: time.Now().UnixNano(),
		Queue:     options.queue,
	}

	// 存储到 Redis
	if err := btm.storeBatch(ctx, batch); err != nil {
		return nil, fmt.Errorf("failed to store batch: %v", err)
	}

	// 计算第一个任务的执行时间
	firstTaskTime := startAt
	now := time.Now()

	// 如果开始时间过早，需要自动调整
	// 确保有足够的时间来准备和调度所有任务
	minStartTime := now.Add(800 * time.Millisecond) // 至少200ms的准备时间
	if firstTaskTime.Before(minStartTime) {
		btm.logger.Warnf("Start time %v is too early, adjusting to %v",
			firstTaskTime.Format("15:04:05.000"), minStartTime.Format("15:04:05.000"))
		firstTaskTime = minStartTime
		batch.StartTime = firstTaskTime.UnixNano()
		btm.updateBatchStatus(ctx, batch)
	}

	// 直接启动批量任务执行，不等待startTime
	// 每个任务的goroutine会独立等待到自己的执行时间
	go btm.executeBatch(batch)

	// 注意：不再使用timer，因为每个任务都会独立等待

	// 更新指标
	btm.metrics.mu.Lock()
	btm.metrics.TotalBatches++
	btm.metrics.mu.Unlock()

	btm.logger.Infof("Scheduled batch %s with %d tasks, starting at %v, interval %v",
		batchID, len(tasks), firstTaskTime, interval)

	return batch, nil
}

// validateBatchParams 验证批量任务参数
func (btm *BatchTaskManager) validateBatchParams(tasks []*Task, interval time.Duration) error {
	if len(tasks) == 0 {
		return fmt.Errorf("tasks cannot be empty")
	}

	if len(tasks) > btm.config.MaxBatchSize {
		return fmt.Errorf("batch size %d exceeds maximum %d", len(tasks), btm.config.MaxBatchSize)
	}

	if interval < time.Millisecond {
		return fmt.Errorf("interval %v is too small, minimum is 1ms", interval)
	}

	// 验证任务
	for i, task := range tasks {
		if task == nil {
			return fmt.Errorf("task at index %d is nil", i)
		}
		if task.Type() == "" {
			return fmt.Errorf("task at index %d has empty type", i)
		}
	}

	return nil
}

// generateBatchID 生成批量任务ID
func (btm *BatchTaskManager) generateBatchID() string {
	return fmt.Sprintf("batch_%d_%d", time.Now().UnixNano(), time.Now().Nanosecond()%1000)
}

// executeBatch 执行批量任务（并发版本）
func (btm *BatchTaskManager) executeBatch(batch *BatchTask) {
	btm.wg.Add(1)
	defer btm.wg.Done()

	// 注意：不再需要清理定时器，因为我们不使用全局timer了

	// 创建执行状态
	execution := &BatchTaskExecution{
		BatchID:     batch.ID,
		StartTime:   time.Now(),
		TaskResults: make([]TaskExecutionResult, len(batch.Tasks)),
		TimeErrors:  make([]time.Duration, len(batch.Tasks)),
	}

	btm.mu.Lock()
	btm.executions[batch.ID] = execution
	btm.mu.Unlock()

	// 更新状态为运行中
	batch.Status = "running"
	batch.UpdatedAt = time.Now().UnixNano()
	btm.updateBatchStatus(context.Background(), batch)

	startTime := time.Unix(0, batch.StartTime)
	interval := time.Duration(batch.Interval)

	btm.logger.Infof("Starting concurrent execution of batch %s with %d tasks, startTime: %v, interval: %v",
		batch.ID, len(batch.Tasks), startTime.Format("15:04:05.000"), interval)

	// 创建结果收集通道和同步机制
	resultChan := make(chan TaskExecutionResult, len(batch.Tasks))
	var taskWg sync.WaitGroup
	taskWg.Add(len(batch.Tasks))

	// 为每个任务创建独立的goroutine和定时器
	for i, task := range batch.Tasks {
		go func(taskIndex int, taskToExecute *Task) {
			defer taskWg.Done()

			// 检查是否应该停止
			select {
			case <-btm.done:
				btm.logger.Infof("Task %d in batch %s cancelled due to shutdown", taskIndex, batch.ID)
				resultChan <- TaskExecutionResult{
					Index:  taskIndex,
					TaskID: fmt.Sprintf("%s_%d", batch.ID, taskIndex),
					Status: "cancelled",
					Error:  "shutdown requested",
				}
				return
			default:
			}

			// 计算该任务的精确执行时间
			scheduledTime := startTime.Add(time.Duration(taskIndex) * interval)

			// 精确等待到执行时间
			now := time.Now()
			if scheduledTime.After(now) {
				waitTime := scheduledTime.Sub(now)
				btm.logger.Debugf("Task %d in batch %s scheduled at %v, waiting %v",
					taskIndex, batch.ID, scheduledTime.Format("15:04:05.000"), waitTime)

				if waitTime > 0 {
					// 使用定时器而不是sleep，确保精确性
					timer := time.NewTimer(waitTime)
					select {
					case <-timer.C:
						// 到达执行时间
						btm.logger.Debugf("Task %d in batch %s timer triggered", taskIndex, batch.ID)
					case <-btm.done:
						timer.Stop()
						btm.logger.Infof("Task %d in batch %s cancelled during wait", taskIndex, batch.ID)
						resultChan <- TaskExecutionResult{
							Index:  taskIndex,
							TaskID: fmt.Sprintf("%s_%d", batch.ID, taskIndex),
							Status: "cancelled",
							Error:  "shutdown requested during wait",
						}
						return
					}
				}
			} else {
				// 预定时间已过，立即执行
				pastDue := now.Sub(scheduledTime)
				btm.logger.Warnf("Task %d in batch %s scheduled time %v has passed by %v, executing immediately",
					taskIndex, batch.ID, scheduledTime.Format("15:04:05.000"), pastDue)
			}

			// 记录实际执行时间，计算误差
			actualTime := time.Now()
			timeError := actualTime.Sub(scheduledTime)
			if timeError < 0 {
				timeError = -timeError
			}

			// 检查时间误差是否超过阈值
			if timeError > btm.config.MaxTimeError {
				btm.logger.Warnf("Task %d in batch %s executed with time error: %v (threshold: %v)",
					taskIndex, batch.ID, timeError, btm.config.MaxTimeError)
			}

			// 执行任务（在独立的goroutine中）
			result := btm.executeTask(taskToExecute, batch, taskIndex, scheduledTime, actualTime)
			result.TimeError = timeError

			btm.logger.Debugf("Completed task %d in batch %s (time error: %v, duration: %v)",
				taskIndex, batch.ID, timeError, result.Duration)

			// 发送结果到通道
			resultChan <- result

		}(i, task)
	}

	// 等待所有任务完成
	go func() {
		taskWg.Wait()
		close(resultChan)
	}()

	// 收集所有任务结果
	completedTasks := 0
	failedTasks := 0
	var totalTimeError time.Duration
	maxTimeError := time.Duration(0)

	for result := range resultChan {
		// 存储结果
		btm.mu.Lock()
		if execution, exists := btm.executions[batch.ID]; exists {
			execution.TaskResults[result.Index] = result
			execution.TimeErrors[result.Index] = result.TimeError
			execution.CurrentIndex = completedTasks + 1
		}
		btm.mu.Unlock()

		completedTasks++
		totalTimeError += result.TimeError
		if result.TimeError > maxTimeError {
			maxTimeError = result.TimeError
		}

		if result.Status == "failed" {
			failedTasks++
			btm.logger.Errorf("Task %d in batch %s failed: %v", result.Index, batch.ID, result.Error)
		}
	}

	// 检查批量任务是否应该标记为失败
	if failedTasks > 0 && !btm.config.RetryOnFailure {
		batch.Status = "failed"
		batch.ErrorMsg = fmt.Sprintf("%d tasks failed", failedTasks)
		batch.UpdatedAt = time.Now().UnixNano()
		btm.updateBatchStatus(context.Background(), batch)

		// 更新指标
		btm.metrics.mu.Lock()
		btm.metrics.FailedBatches++
		btm.metrics.mu.Unlock()

		// 清理执行状态
		btm.mu.Lock()
		delete(btm.executions, batch.ID)
		btm.mu.Unlock()

		btm.logger.Errorf("Batch %s failed with %d failed tasks", batch.ID, failedTasks)
		return
	}

	// 所有任务执行完成
	batch.Status = "completed"
	batch.UpdatedAt = time.Now().UnixNano()
	batch.CompletedAt = time.Now().UnixNano()
	btm.updateBatchStatus(context.Background(), batch)

	// 更新指标
	avgTimeError := time.Duration(0)
	if completedTasks > 0 {
		avgTimeError = totalTimeError / time.Duration(completedTasks)
	}

	btm.metrics.mu.Lock()
	btm.metrics.CompletedBatches++
	btm.metrics.AvgTimeError = avgTimeError
	if maxTimeError > btm.metrics.MaxTimeError {
		btm.metrics.MaxTimeError = maxTimeError
	}
	btm.metrics.mu.Unlock()

	// 清理执行状态
	btm.mu.Lock()
	delete(btm.executions, batch.ID)
	btm.mu.Unlock()

	btm.logger.Infof("Completed batch %s with %d tasks (%d succeeded, %d failed, avg time error: %v, max time error: %v)",
		batch.ID, len(batch.Tasks), completedTasks-failedTasks, failedTasks, avgTimeError, maxTimeError)
}

// executeTask 执行单个任务
func (btm *BatchTaskManager) executeTask(task *Task, batch *BatchTask, taskIndex int, scheduledTime, actualTime time.Time) TaskExecutionResult {
	result := TaskExecutionResult{
		Index:       taskIndex,
		TaskID:      fmt.Sprintf("%s_%d", batch.ID, taskIndex),
		Status:      "success",
		ExecutedAt:  actualTime.UnixNano(),
		ScheduledAt: scheduledTime.UnixNano(),
		TimeError:   actualTime.Sub(scheduledTime),
	}

	startTime := time.Now()

	// 直接调用handler执行任务，而不是放入队列
	if btm.handler != nil {
		ctx := context.Background()

		// 设置任务超时
		if task.opts != nil {
			for _, opt := range task.opts {
				if timeoutOpt, ok := opt.(timeoutOption); ok {
					timeout := time.Duration(timeoutOpt)
					if timeout > 0 {
						var cancel context.CancelFunc
						ctx, cancel = context.WithTimeout(ctx, timeout)
						defer cancel()
					}
				} else if deadlineOpt, ok := opt.(deadlineOption); ok {
					deadline := time.Time(deadlineOpt)
					if !deadline.IsZero() {
						var cancel context.CancelFunc
						ctx, cancel = context.WithDeadline(ctx, deadline)
						defer cancel()
					}
				}
			}
		}

		// 创建Asynq任务对象
		asynqTask := NewTask(task.Type(), task.Payload())

		// 直接调用handler处理任务
		err := btm.handler.ProcessTask(ctx, asynqTask)
		if err != nil {
			result.Status = "failed"
			result.Error = err.Error()
			btm.logger.Errorf("Batch task %s failed: %v", result.TaskID, err)
		} else {
			btm.logger.Debugf("Batch task %s completed successfully", result.TaskID)
		}
	} else {
		result.Status = "failed"
		result.Error = "no handler registered for batch task execution"
		btm.logger.Errorf("No handler registered for batch task %s", result.TaskID)
	}

	result.Duration = time.Since(startTime)
	return result
}

// CancelBatch 取消批量任务
func (btm *BatchTaskManager) CancelBatch(batchID string) error {
	btm.mu.Lock()
	// 不再需要处理timer，直接删除执行状态
	delete(btm.executions, batchID)
	btm.mu.Unlock()

	// 更新 Redis 中的状态
	ctx := context.Background()
	batch, err := btm.getBatch(ctx, batchID)
	if err != nil {
		return err
	}

	if batch.Status == "pending" || batch.Status == "running" {
		batch.Status = "cancelled"
		batch.UpdatedAt = time.Now().UnixNano()
		return btm.updateBatchStatus(ctx, batch)
	}

	return nil
}

// GetBatchInfo 获取批量任务信息
func (btm *BatchTaskManager) GetBatchInfo(batchID string) (*BatchTaskInfo, error) {
	ctx := context.Background()
	batch, err := btm.getBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}

	info := &BatchTaskInfo{
		BatchTask:  *batch,
		TotalTasks: len(batch.Tasks),
	}

	// 如果任务正在执行，获取实时状态
	btm.mu.RLock()
	execution, exists := btm.executions[batchID]
	if exists {
		info.NextTaskIndex = execution.CurrentIndex

		// 计算已完成和失败的任务数
		for _, result := range execution.TaskResults[:execution.CurrentIndex] {
			if result.Status == "success" {
				info.CompletedCount++
			} else if result.Status == "failed" {
				info.FailedCount++
			}
		}

		// 估算结束时间
		if execution.CurrentIndex > 0 && execution.CurrentIndex < len(batch.Tasks) {
			remainingTasks := len(batch.Tasks) - execution.CurrentIndex
			interval := time.Duration(batch.Interval)
			estimatedRemaining := time.Duration(remainingTasks) * interval
			info.EstimatedEndTime = time.Now().Add(estimatedRemaining).UnixNano()
		}
	} else if batch.Status == "completed" {
		info.CompletedCount = len(batch.Tasks)
		info.NextTaskIndex = len(batch.Tasks)
	}
	btm.mu.RUnlock()

	return info, nil
}

// GetMetrics 获取批量任务指标
func (btm *BatchTaskManager) GetMetrics() *BatchMetrics {
	btm.metrics.mu.RLock()
	defer btm.metrics.mu.RUnlock()

	return &BatchMetrics{
		TotalBatches:     btm.metrics.TotalBatches,
		CompletedBatches: btm.metrics.CompletedBatches,
		FailedBatches:    btm.metrics.FailedBatches,
		AvgTimeError:     btm.metrics.AvgTimeError,
		MaxTimeError:     btm.metrics.MaxTimeError,
	}
}

// Shutdown 优雅关闭批量任务管理器
func (btm *BatchTaskManager) Shutdown() {
	btm.logger.Info("Shutting down batch task manager...")

	// 停止接受新任务
	close(btm.done)

	// 不再需要取消定时器，因为每个任务都会通过btm.done通道接收关闭信号

	// 等待所有正在执行的任务完成（最多等待30秒）
	done := make(chan struct{})
	go func() {
		btm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		btm.logger.Info("All batch tasks completed")
	case <-time.After(30 * time.Second):
		btm.logger.Warn("Timeout waiting for batch tasks to complete")
	}

	btm.logger.Info("Batch task manager shutdown complete")
}
