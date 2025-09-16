// Copyright 2024. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/log"
)

// BatchErrorType 批量任务错误类型
type BatchErrorType int

const (
	BatchErrorValidation BatchErrorType = iota + 1
	BatchErrorStorage
	BatchErrorExecution
	BatchErrorTimeout
	BatchErrorCancelled
	BatchErrorRecovery
	BatchErrorTiming
)

func (bet BatchErrorType) String() string {
	switch bet {
	case BatchErrorValidation:
		return "validation"
	case BatchErrorStorage:
		return "storage"
	case BatchErrorExecution:
		return "execution"
	case BatchErrorTimeout:
		return "timeout"
	case BatchErrorCancelled:
		return "cancelled"
	case BatchErrorRecovery:
		return "recovery"
	case BatchErrorTiming:
		return "timing"
	default:
		return "unknown"
	}
}

// BatchError 批量任务错误
type BatchError struct {
	Type      BatchErrorType         `json:"type"`
	BatchID   string                 `json:"batch_id"`
	TaskIndex int                    `json:"task_index"`
	Message   string                 `json:"message"`
	Cause     error                  `json:"cause,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Retryable bool                   `json:"retryable"`
	Context   map[string]interface{} `json:"context,omitempty"`
}

func (be *BatchError) Error() string {
	if be.TaskIndex >= 0 {
		return fmt.Sprintf("batch %s task %d %s error: %s",
			be.BatchID, be.TaskIndex, be.Type.String(), be.Message)
	}
	return fmt.Sprintf("batch %s %s error: %s",
		be.BatchID, be.Type.String(), be.Message)
}

func (be *BatchError) Unwrap() error {
	return be.Cause
}

// NewBatchError 创建批量任务错误
func NewBatchError(errorType BatchErrorType, batchID string, taskIndex int, message string, cause error) *BatchError {
	return &BatchError{
		Type:      errorType,
		BatchID:   batchID,
		TaskIndex: taskIndex,
		Message:   message,
		Cause:     cause,
		Timestamp: time.Now(),
		Retryable: isRetryableError(errorType, cause),
		Context:   make(map[string]interface{}),
	}
}

// isRetryableError 判断错误是否可重试
func isRetryableError(errorType BatchErrorType, cause error) bool {
	switch errorType {
	case BatchErrorValidation:
		return false // 验证错误通常不可重试
	case BatchErrorStorage:
		return true // 存储错误可重试
	case BatchErrorExecution:
		return true // 执行错误可重试
	case BatchErrorTimeout:
		return true // 超时错误可重试
	case BatchErrorCancelled:
		return false // 取消操作不可重试
	case BatchErrorRecovery:
		return true // 恢复错误可重试
	case BatchErrorTiming:
		return false // 时间错误通常不可重试
	default:
		return false
	}
}

// BatchErrorHandler 批量任务错误处理器
type BatchErrorHandler interface {
	HandleError(ctx context.Context, err *BatchError) error
	CanRetry(err *BatchError) bool
	GetRetryDelay(err *BatchError, attempt int) time.Duration
}

// DefaultBatchErrorHandler 默认批量任务错误处理器
type DefaultBatchErrorHandler struct {
	logger       *log.Logger
	maxRetries   int
	baseDelay    time.Duration
	maxDelay     time.Duration
	backoffFunc  func(attempt int, baseDelay time.Duration) time.Duration
	retryFilters []RetryFilter
}

// RetryFilter 重试过滤器
type RetryFilter func(err *BatchError) bool

// NewDefaultBatchErrorHandler 创建默认批量任务错误处理器
func NewDefaultBatchErrorHandler(logger *log.Logger) *DefaultBatchErrorHandler {
	return &DefaultBatchErrorHandler{
		logger:      logger,
		maxRetries:  3,
		baseDelay:   1 * time.Second,
		maxDelay:    30 * time.Second,
		backoffFunc: exponentialBackoff,
		retryFilters: []RetryFilter{
			func(err *BatchError) bool { return err.Retryable },
		},
	}
}

// exponentialBackoff 指数退避算法
func exponentialBackoff(attempt int, baseDelay time.Duration) time.Duration {
	delay := baseDelay
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	return delay
}

// HandleError 处理错误
func (h *DefaultBatchErrorHandler) HandleError(ctx context.Context, err *BatchError) error {
	h.logger.Errorf("Batch error occurred: %v", err)

	// 记录错误详情
	if err.Context == nil {
		err.Context = make(map[string]interface{})
	}
	err.Context["handled_at"] = time.Now()
	err.Context["handler"] = "DefaultBatchErrorHandler"

	return nil
}

// CanRetry 判断是否可以重试
func (h *DefaultBatchErrorHandler) CanRetry(err *BatchError) bool {
	for _, filter := range h.retryFilters {
		if !filter(err) {
			return false
		}
	}
	return true
}

// GetRetryDelay 获取重试延迟
func (h *DefaultBatchErrorHandler) GetRetryDelay(err *BatchError, attempt int) time.Duration {
	if attempt > h.maxRetries {
		return 0 // 超过最大重试次数
	}

	delay := h.backoffFunc(attempt-1, h.baseDelay)
	if delay > h.maxDelay {
		delay = h.maxDelay
	}

	return delay
}

// SetMaxRetries 设置最大重试次数
func (h *DefaultBatchErrorHandler) SetMaxRetries(maxRetries int) {
	h.maxRetries = maxRetries
}

// SetBackoffParameters 设置退避参数
func (h *DefaultBatchErrorHandler) SetBackoffParameters(baseDelay, maxDelay time.Duration) {
	h.baseDelay = baseDelay
	h.maxDelay = maxDelay
}

// AddRetryFilter 添加重试过滤器
func (h *DefaultBatchErrorHandler) AddRetryFilter(filter RetryFilter) {
	h.retryFilters = append(h.retryFilters, filter)
}

// BatchRecoveryManager 批量任务恢复管理器
type BatchRecoveryManager struct {
	batchManager *BatchTaskManager
	errorHandler BatchErrorHandler
	logger       *log.Logger

	// 恢复状态
	recoveryInProgress map[string]bool
	mu                 sync.RWMutex

	// 恢复配置
	recoveryTimeout     time.Duration
	recoveryInterval    time.Duration
	maxRecoveryAttempts int
}

// NewBatchRecoveryManager 创建批量任务恢复管理器
func NewBatchRecoveryManager(batchManager *BatchTaskManager, errorHandler BatchErrorHandler, logger *log.Logger) *BatchRecoveryManager {
	return &BatchRecoveryManager{
		batchManager:        batchManager,
		errorHandler:        errorHandler,
		logger:              logger,
		recoveryInProgress:  make(map[string]bool),
		recoveryTimeout:     5 * time.Minute,
		recoveryInterval:    30 * time.Second,
		maxRecoveryAttempts: 5,
	}
}

// RecoverFailedBatch 恢复失败的批量任务
func (brm *BatchRecoveryManager) RecoverFailedBatch(ctx context.Context, batchID string) error {
	brm.mu.Lock()
	if brm.recoveryInProgress[batchID] {
		brm.mu.Unlock()
		return fmt.Errorf("recovery already in progress for batch %s", batchID)
	}
	brm.recoveryInProgress[batchID] = true
	brm.mu.Unlock()

	defer func() {
		brm.mu.Lock()
		delete(brm.recoveryInProgress, batchID)
		brm.mu.Unlock()
	}()

	brm.logger.Infof("Starting recovery for batch %s", batchID)

	// 获取批量任务信息
	batch, err := brm.batchManager.getBatch(ctx, batchID)
	if err != nil {
		batchErr := NewBatchError(BatchErrorRecovery, batchID, -1,
			"failed to get batch for recovery", err)
		return brm.errorHandler.HandleError(ctx, batchErr)
	}

	// 检查批量任务状态
	if batch.Status != "failed" && batch.Status != "cancelled" {
		return fmt.Errorf("batch %s is not in a recoverable state: %s", batchID, batch.Status)
	}

	// 根据失败原因决定恢复策略
	return brm.executeRecoveryStrategy(ctx, batch)
}

// executeRecoveryStrategy 执行恢复策略
func (brm *BatchRecoveryManager) executeRecoveryStrategy(ctx context.Context, batch *BatchTask) error {
	// 分析失败原因
	failureReason := brm.analyzeFailureReason(batch)

	switch failureReason {
	case "timeout":
		return brm.recoverFromTimeout(ctx, batch)
	case "storage_error":
		return brm.recoverFromStorageError(ctx, batch)
	case "execution_error":
		return brm.recoverFromExecutionError(ctx, batch)
	case "timing_error":
		return brm.recoverFromTimingError(ctx, batch)
	default:
		return brm.performGenericRecovery(ctx, batch)
	}
}

// analyzeFailureReason 分析失败原因
func (brm *BatchRecoveryManager) analyzeFailureReason(batch *BatchTask) string {
	if batch.ErrorMsg == "" {
		return "unknown"
	}

	errorMsg := batch.ErrorMsg

	if contains(errorMsg, "timeout") || contains(errorMsg, "context deadline") {
		return "timeout"
	}
	if contains(errorMsg, "redis") || contains(errorMsg, "storage") || contains(errorMsg, "connection") {
		return "storage_error"
	}
	if contains(errorMsg, "execution") || contains(errorMsg, "enqueue") {
		return "execution_error"
	}
	if contains(errorMsg, "time") || contains(errorMsg, "timing") {
		return "timing_error"
	}

	return "unknown"
}

// contains 检查字符串包含
func contains(str, substr string) bool {
	return len(str) >= len(substr) &&
		(str == substr || (len(str) > len(substr) &&
			(str[:len(substr)] == substr || str[len(str)-len(substr):] == substr ||
				findSubstring(str, substr))))
}

func findSubstring(str, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// recoverFromTimeout 从超时恢复
func (brm *BatchRecoveryManager) recoverFromTimeout(ctx context.Context, batch *BatchTask) error {
	brm.logger.Infof("Recovering batch %s from timeout", batch.ID)

	// 重新设置开始时间，延长超时时间
	newStartTime := time.Now().Add(1 * time.Minute)
	batch.StartTime = newStartTime.UnixNano()
	batch.Status = "pending"
	batch.ErrorMsg = ""
	batch.UpdatedAt = time.Now().UnixNano()

	// 更新到Redis
	err := brm.batchManager.updateBatchStatus(ctx, batch)
	if err != nil {
		batchErr := NewBatchError(BatchErrorRecovery, batch.ID, -1,
			"failed to update batch status during recovery", err)
		return brm.errorHandler.HandleError(ctx, batchErr)
	}

	// 重新调度
	timer := time.AfterFunc(time.Until(newStartTime), func() {
		brm.batchManager.executeBatch(batch)
	})

	brm.batchManager.mu.Lock()
	brm.batchManager.timers[batch.ID] = timer
	brm.batchManager.mu.Unlock()

	brm.logger.Infof("Successfully recovered batch %s from timeout, rescheduled for %v",
		batch.ID, newStartTime)

	return nil
}

// recoverFromStorageError 从存储错误恢复
func (brm *BatchRecoveryManager) recoverFromStorageError(ctx context.Context, batch *BatchTask) error {
	brm.logger.Infof("Recovering batch %s from storage error", batch.ID)

	// 尝试重新存储批量任务
	err := brm.batchManager.storeBatch(ctx, batch)
	if err != nil {
		batchErr := NewBatchError(BatchErrorRecovery, batch.ID, -1,
			"failed to restore batch during recovery", err)
		return brm.errorHandler.HandleError(ctx, batchErr)
	}

	return brm.recoverFromTimeout(ctx, batch) // 然后按超时恢复流程处理
}

// recoverFromExecutionError 从执行错误恢复
func (brm *BatchRecoveryManager) recoverFromExecutionError(ctx context.Context, batch *BatchTask) error {
	brm.logger.Infof("Recovering batch %s from execution error", batch.ID)

	// 检查是否可以重试
	batchErr := NewBatchError(BatchErrorExecution, batch.ID, -1, batch.ErrorMsg, nil)
	if !brm.errorHandler.CanRetry(batchErr) {
		brm.logger.Warnf("Batch %s cannot be retried due to error handler policy", batch.ID)
		return fmt.Errorf("batch %s is not retryable", batch.ID)
	}

	// 增加重试计数
	batch.RetryCount++

	// 计算重试延迟
	retryDelay := brm.errorHandler.GetRetryDelay(batchErr, batch.RetryCount)
	if retryDelay == 0 {
		brm.logger.Warnf("Batch %s exceeded maximum retry attempts", batch.ID)
		return fmt.Errorf("batch %s exceeded maximum retry attempts", batch.ID)
	}

	// 重新调度
	newStartTime := time.Now().Add(retryDelay)
	batch.StartTime = newStartTime.UnixNano()
	batch.Status = "pending"
	batch.ErrorMsg = ""
	batch.UpdatedAt = time.Now().UnixNano()

	err := brm.batchManager.updateBatchStatus(ctx, batch)
	if err != nil {
		batchErr := NewBatchError(BatchErrorRecovery, batch.ID, -1,
			"failed to update batch status during execution recovery", err)
		return brm.errorHandler.HandleError(ctx, batchErr)
	}

	timer := time.AfterFunc(retryDelay, func() {
		brm.batchManager.executeBatch(batch)
	})

	brm.batchManager.mu.Lock()
	brm.batchManager.timers[batch.ID] = timer
	brm.batchManager.mu.Unlock()

	brm.logger.Infof("Successfully recovered batch %s from execution error, retry %d scheduled for %v",
		batch.ID, batch.RetryCount, newStartTime)

	return nil
}

// recoverFromTimingError 从时间错误恢复
func (brm *BatchRecoveryManager) recoverFromTimingError(ctx context.Context, batch *BatchTask) error {
	brm.logger.Infof("Recovering batch %s from timing error", batch.ID)

	// 时间错误通常不需要重新调度，只需要调整配置
	// 这里可以调整时间控制器的参数

	// 标记为已恢复
	batch.Status = "completed"
	batch.ErrorMsg = "recovered from timing error"
	batch.UpdatedAt = time.Now().UnixNano()
	batch.CompletedAt = time.Now().UnixNano()

	err := brm.batchManager.updateBatchStatus(ctx, batch)
	if err != nil {
		batchErr := NewBatchError(BatchErrorRecovery, batch.ID, -1,
			"failed to update batch status during timing recovery", err)
		return brm.errorHandler.HandleError(ctx, batchErr)
	}

	brm.logger.Infof("Successfully recovered batch %s from timing error", batch.ID)
	return nil
}

// performGenericRecovery 执行通用恢复
func (brm *BatchRecoveryManager) performGenericRecovery(ctx context.Context, batch *BatchTask) error {
	brm.logger.Infof("Performing generic recovery for batch %s", batch.ID)

	// 通用恢复策略：重新调度到未来某个时间
	return brm.recoverFromTimeout(ctx, batch)
}

// RecoverAllFailedBatches 恢复所有失败的批量任务
func (brm *BatchRecoveryManager) RecoverAllFailedBatches(ctx context.Context) error {
	brm.logger.Info("Starting recovery of all failed batches")

	// 获取所有失败的批量任务
	failedBatches, err := brm.batchManager.ListBatchesByStatus("failed", 1000, 0)
	if err != nil {
		return fmt.Errorf("failed to list failed batches: %v", err)
	}

	recoveredCount := 0
	errorCount := 0

	for _, batchInfo := range failedBatches {
		err := brm.RecoverFailedBatch(ctx, batchInfo.ID)
		if err != nil {
			brm.logger.Errorf("Failed to recover batch %s: %v", batchInfo.ID, err)
			errorCount++
		} else {
			recoveredCount++
		}
	}

	brm.logger.Infof("Recovery completed: %d batches recovered, %d errors",
		recoveredCount, errorCount)

	return nil
}

// SetRecoveryParameters 设置恢复参数
func (brm *BatchRecoveryManager) SetRecoveryParameters(timeout, interval time.Duration, maxAttempts int) {
	brm.recoveryTimeout = timeout
	brm.recoveryInterval = interval
	brm.maxRecoveryAttempts = maxAttempts
}

// CircuitBreaker 熔断器
type CircuitBreaker struct {
	mu               sync.RWMutex
	failureThreshold int
	resetTimeout     time.Duration
	maxRetries       int

	failures        int
	lastFailureTime time.Time
	state           CircuitState
}

// CircuitState 熔断器状态
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

func (cs CircuitState) String() string {
	switch cs {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// NewCircuitBreaker 创建熔断器
func NewCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		maxRetries:       3,
		state:            CircuitClosed,
	}
}

// Call 调用操作
func (cb *CircuitBreaker) Call(operation func() error) error {
	if !cb.allowRequest() {
		return fmt.Errorf("circuit breaker is open")
	}

	err := operation()
	cb.recordResult(err)

	return err
}

// allowRequest 是否允许请求
func (cb *CircuitBreaker) allowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailureTime) > cb.resetTimeout {
			cb.state = CircuitHalfOpen
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	default:
		return false
	}
}

// recordResult 记录结果
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		cb.lastFailureTime = time.Now()

		if cb.failures >= cb.failureThreshold {
			cb.state = CircuitOpen
		}
	} else {
		cb.failures = 0
		if cb.state == CircuitHalfOpen {
			cb.state = CircuitClosed
		}
	}
}

// GetState 获取状态
func (cb *CircuitBreaker) GetState() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return cb.state
}

// Reset 重置熔断器
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.state = CircuitClosed
}
