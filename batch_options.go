// Copyright 2024. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"time"

	"github.com/hibiken/asynq/internal/base"
)

// BatchOption 批量任务选项接口
type BatchOption interface {
	apply(*batchOptions)
}

// batchOptions 批量任务选项
type batchOptions struct {
	queue          string
	maxRetries     int
	retryOnFailure bool
	timeout        time.Duration
	deadline       time.Time
	priority       int
	tags           []string
	metadata       map[string]string
}

// batchOptionFunc 批量任务选项函数类型
type batchOptionFunc func(*batchOptions)

func (f batchOptionFunc) apply(opts *batchOptions) {
	f(opts)
}

// BatchQueue 设置批量任务队列
func BatchQueue(name string) BatchOption {
	return batchOptionFunc(func(opts *batchOptions) {
		opts.queue = name
	})
}

// BatchMaxRetries 设置批量任务最大重试次数
func BatchMaxRetries(n int) BatchOption {
	return batchOptionFunc(func(opts *batchOptions) {
		opts.maxRetries = n
	})
}

// BatchRetryOnFailure 设置是否在任务失败时重试
func BatchRetryOnFailure(retry bool) BatchOption {
	return batchOptionFunc(func(opts *batchOptions) {
		opts.retryOnFailure = retry
	})
}

// BatchTimeout 设置批量任务超时时间
func BatchTimeout(d time.Duration) BatchOption {
	return batchOptionFunc(func(opts *batchOptions) {
		opts.timeout = d
	})
}

// BatchDeadline 设置批量任务截止时间
func BatchDeadline(t time.Time) BatchOption {
	return batchOptionFunc(func(opts *batchOptions) {
		opts.deadline = t
	})
}

// BatchPriority 设置批量任务优先级
func BatchPriority(p int) BatchOption {
	return batchOptionFunc(func(opts *batchOptions) {
		opts.priority = p
	})
}

// BatchTags 设置批量任务标签
func BatchTags(tags ...string) BatchOption {
	return batchOptionFunc(func(opts *batchOptions) {
		opts.tags = tags
	})
}

// BatchMetadata 设置批量任务元数据
func BatchMetadata(metadata map[string]string) BatchOption {
	return batchOptionFunc(func(opts *batchOptions) {
		opts.metadata = metadata
	})
}

// BatchIntervalOption 批量任务间隔选项
type BatchIntervalOption struct {
	interval    time.Duration
	jitter      time.Duration
	adaptive    bool
	minInterval time.Duration
	maxInterval time.Duration
}

// NewBatchIntervalOption 创建批量任务间隔选项
func NewBatchIntervalOption(interval time.Duration) *BatchIntervalOption {
	return &BatchIntervalOption{
		interval:    interval,
		jitter:      0,
		adaptive:    false,
		minInterval: interval,
		maxInterval: interval,
	}
}

// WithJitter 添加抖动
func (bio *BatchIntervalOption) WithJitter(jitter time.Duration) *BatchIntervalOption {
	bio.jitter = jitter
	return bio
}

// WithAdaptive 启用自适应间隔
func (bio *BatchIntervalOption) WithAdaptive(min, max time.Duration) *BatchIntervalOption {
	bio.adaptive = true
	bio.minInterval = min
	bio.maxInterval = max
	return bio
}

// CalculateInterval 计算实际间隔
func (bio *BatchIntervalOption) CalculateInterval(taskIndex int, lastError error) time.Duration {
	baseInterval := bio.interval

	// 如果启用了自适应间隔
	if bio.adaptive {
		if lastError != nil {
			// 有错误时增加间隔
			baseInterval = bio.maxInterval
		} else {
			// 无错误时使用最小间隔
			baseInterval = bio.minInterval
		}
	}

	// 添加抖动
	if bio.jitter > 0 {
		// 简单的伪随机抖动
		jitterNs := int64(bio.jitter)
		adjustment := (int64(taskIndex)*1234567)%(jitterNs*2) - jitterNs
		baseInterval += time.Duration(adjustment)

		// 确保间隔不为负数
		if baseInterval < 0 {
			baseInterval = bio.minInterval
		}
	}

	return baseInterval
}

// BatchScheduleOption 批量调度选项
type BatchScheduleOption struct {
	startTime        time.Time
	intervalOption   *BatchIntervalOption
	maxConcurrency   int
	failFast         bool
	continueOnError  bool
	progressCallback func(batchID string, completedCount, totalCount int)
	taskCallback     func(batchID string, taskIndex int, result *TaskExecutionResult)
}

// NewBatchScheduleOption 创建批量调度选项
func NewBatchScheduleOption() *BatchScheduleOption {
	return &BatchScheduleOption{
		startTime:       time.Now(),
		intervalOption:  NewBatchIntervalOption(100 * time.Millisecond),
		maxConcurrency:  1, // 默认顺序执行
		failFast:        false,
		continueOnError: true,
	}
}

// WithStartTime 设置开始时间
func (bso *BatchScheduleOption) WithStartTime(t time.Time) *BatchScheduleOption {
	bso.startTime = t
	return bso
}

// WithInterval 设置间隔选项
func (bso *BatchScheduleOption) WithInterval(intervalOption *BatchIntervalOption) *BatchScheduleOption {
	bso.intervalOption = intervalOption
	return bso
}

// WithMaxConcurrency 设置最大并发数
func (bso *BatchScheduleOption) WithMaxConcurrency(n int) *BatchScheduleOption {
	bso.maxConcurrency = n
	return bso
}

// WithFailFast 设置快速失败
func (bso *BatchScheduleOption) WithFailFast() *BatchScheduleOption {
	bso.failFast = true
	bso.continueOnError = false
	return bso
}

// WithContinueOnError 设置遇到错误时继续执行
func (bso *BatchScheduleOption) WithContinueOnError() *BatchScheduleOption {
	bso.continueOnError = true
	bso.failFast = false
	return bso
}

// WithProgressCallback 设置进度回调
func (bso *BatchScheduleOption) WithProgressCallback(callback func(batchID string, completedCount, totalCount int)) *BatchScheduleOption {
	bso.progressCallback = callback
	return bso
}

// WithTaskCallback 设置任务回调
func (bso *BatchScheduleOption) WithTaskCallback(callback func(batchID string, taskIndex int, result *TaskExecutionResult)) *BatchScheduleOption {
	bso.taskCallback = callback
	return bso
}

// BatchTaskTemplate 批量任务模板
type BatchTaskTemplate struct {
	taskType   string
	payloads   [][]byte
	options    []Option
	queue      string
	maxRetries int
	timeout    time.Duration
	deadline   time.Time
}

// NewBatchTaskTemplate 创建批量任务模板
func NewBatchTaskTemplate(taskType string) *BatchTaskTemplate {
	return &BatchTaskTemplate{
		taskType:   taskType,
		payloads:   make([][]byte, 0),
		options:    make([]Option, 0),
		queue:      base.DefaultQueueName,
		maxRetries: 25,
	}
}

// AddPayload 添加任务载荷
func (btt *BatchTaskTemplate) AddPayload(payload []byte) *BatchTaskTemplate {
	btt.payloads = append(btt.payloads, payload)
	return btt
}

// AddPayloads 批量添加任务载荷
func (btt *BatchTaskTemplate) AddPayloads(payloads ...[]byte) *BatchTaskTemplate {
	btt.payloads = append(btt.payloads, payloads...)
	return btt
}

// WithQueue 设置队列
func (btt *BatchTaskTemplate) WithQueue(queue string) *BatchTaskTemplate {
	btt.queue = queue
	return btt
}

// WithMaxRetries 设置最大重试次数
func (btt *BatchTaskTemplate) WithMaxRetries(n int) *BatchTaskTemplate {
	btt.maxRetries = n
	return btt
}

// WithTimeout 设置超时时间
func (btt *BatchTaskTemplate) WithTimeout(d time.Duration) *BatchTaskTemplate {
	btt.timeout = d
	return btt
}

// WithDeadline 设置截止时间
func (btt *BatchTaskTemplate) WithDeadline(t time.Time) *BatchTaskTemplate {
	btt.deadline = t
	return btt
}

// WithOptions 添加任务选项
func (btt *BatchTaskTemplate) WithOptions(opts ...Option) *BatchTaskTemplate {
	btt.options = append(btt.options, opts...)
	return btt
}

// GenerateTasks 生成任务列表
func (btt *BatchTaskTemplate) GenerateTasks() ([]*Task, error) {
	if len(btt.payloads) == 0 {
		return nil, fmt.Errorf("no payloads provided")
	}

	tasks := make([]*Task, len(btt.payloads))
	baseOptions := make([]Option, 0)

	// 添加基础选项
	if btt.queue != base.DefaultQueueName {
		baseOptions = append(baseOptions, Queue(btt.queue))
	}
	if btt.maxRetries != 25 {
		baseOptions = append(baseOptions, MaxRetry(btt.maxRetries))
	}
	if btt.timeout > 0 {
		baseOptions = append(baseOptions, Timeout(btt.timeout))
	}
	if !btt.deadline.IsZero() {
		baseOptions = append(baseOptions, Deadline(btt.deadline))
	}

	// 合并所有选项
	allOptions := append(baseOptions, btt.options...)

	for i, payload := range btt.payloads {
		tasks[i] = NewTask(btt.taskType, payload, allOptions...)
	}

	return tasks, nil
}

// BatchExecutionOptions 批量执行选项
type BatchExecutionOptions struct {
	enableMetrics       bool
	metricsInterval     time.Duration
	enableProgressLog   bool
	progressLogInterval time.Duration
	enableTimeTracking  bool
	enableErrorDetail   bool
	maxTimeError        time.Duration
	timeErrorCallback   func(batchID string, taskIndex int, timeError time.Duration)
}

// DefaultBatchExecutionOptions 默认批量执行选项
func DefaultBatchExecutionOptions() *BatchExecutionOptions {
	return &BatchExecutionOptions{
		enableMetrics:       true,
		metricsInterval:     10 * time.Second,
		enableProgressLog:   true,
		progressLogInterval: 5 * time.Second,
		enableTimeTracking:  true,
		enableErrorDetail:   true,
		maxTimeError:        5 * time.Millisecond,
	}
}

// WithMetrics 启用指标收集
func (beo *BatchExecutionOptions) WithMetrics(interval time.Duration) *BatchExecutionOptions {
	beo.enableMetrics = true
	beo.metricsInterval = interval
	return beo
}

// WithProgressLog 启用进度日志
func (beo *BatchExecutionOptions) WithProgressLog(interval time.Duration) *BatchExecutionOptions {
	beo.enableProgressLog = true
	beo.progressLogInterval = interval
	return beo
}

// WithTimeTracking 启用时间跟踪
func (beo *BatchExecutionOptions) WithTimeTracking(maxTimeError time.Duration) *BatchExecutionOptions {
	beo.enableTimeTracking = true
	beo.maxTimeError = maxTimeError
	return beo
}

// WithTimeErrorCallback 设置时间误差回调
func (beo *BatchExecutionOptions) WithTimeErrorCallback(callback func(batchID string, taskIndex int, timeError time.Duration)) *BatchExecutionOptions {
	beo.timeErrorCallback = callback
	return beo
}

// WithErrorDetail 启用错误详情
func (beo *BatchExecutionOptions) WithErrorDetail() *BatchExecutionOptions {
	beo.enableErrorDetail = true
	return beo
}

// DisableMetrics 禁用指标收集
func (beo *BatchExecutionOptions) DisableMetrics() *BatchExecutionOptions {
	beo.enableMetrics = false
	return beo
}

// DisableProgressLog 禁用进度日志
func (beo *BatchExecutionOptions) DisableProgressLog() *BatchExecutionOptions {
	beo.enableProgressLog = false
	return beo
}

// DisableTimeTracking 禁用时间跟踪
func (beo *BatchExecutionOptions) DisableTimeTracking() *BatchExecutionOptions {
	beo.enableTimeTracking = false
	return beo
}
