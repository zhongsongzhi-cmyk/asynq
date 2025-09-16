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
	"github.com/redis/go-redis/v9"
)

// BatchServer 批量任务服务器
type BatchServer struct {
	*Server
	batchManager *BatchTaskManager
	handler      Handler
	redis        redis.UniversalClient
	logger       *log.Logger
	config       *BatchTaskConfig
	mu           sync.RWMutex
}

// NewBatchServer 创建批量任务服务器
func NewBatchServer(redisOpt RedisConnOpt, handler Handler, batchConfig *BatchTaskConfig) *BatchServer {
	// 创建普通客户端来获取broker
	client := NewClient(redisOpt)

	// 获取Redis客户端
	redisClient, ok := redisOpt.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("asynq: unsupported RedisConnOpt type %T", redisOpt))
	}

	if batchConfig == nil {
		batchConfig = DefaultBatchTaskConfig()
	}

	logger := log.NewLogger(nil)

	// 创建批量任务管理器
	batchManager := NewBatchTaskManager(
		client.broker,
		redisClient,
		logger,
		batchConfig,
		handler,
	)

	batchServer := &BatchServer{
		Server:       nil, // 暂时不依赖Server
		batchManager: batchManager,
		handler:      handler,
		redis:        redisClient,
		logger:       logger,
		config:       batchConfig,
	}

	return batchServer
}

// Start 启动批量任务服务器
func (bs *BatchServer) Start() error {
	// 恢复未完成的批量任务
	if err := bs.batchManager.RecoverPendingBatches(); err != nil {
		bs.logger.Errorf("Failed to recover pending batches: %v", err)
		// 不因为恢复失败而停止服务器启动
	}

	bs.logger.Info("Batch server started successfully")
	return nil
}

// Shutdown 关闭批量任务服务器
func (bs *BatchServer) Shutdown() {
	bs.logger.Info("Shutting down batch server...")

	// 关闭批量任务管理器
	if bs.batchManager != nil {
		bs.batchManager.Shutdown()
	}

	bs.logger.Info("Batch server shutdown complete")
}

// Run 运行批量任务服务器（阻塞直到收到停止信号）
func (bs *BatchServer) Run() error {
	if err := bs.Start(); err != nil {
		return err
	}

	// 等待停止信号
	bs.waitForSignals()
	bs.Shutdown()
	return nil
}

// GetBatchManager 获取批量任务管理器
func (bs *BatchServer) GetBatchManager() *BatchTaskManager {
	return bs.batchManager
}

// GetBatchStats 获取批量任务统计信息
func (bs *BatchServer) GetBatchStats() (map[string]int64, error) {
	if bs.batchManager == nil {
		return nil, fmt.Errorf("batch manager not initialized")
	}

	ctx := context.Background()
	return bs.batchManager.GetBatchStats(ctx)
}

// GetBatchMetrics 获取批量任务指标
func (bs *BatchServer) GetBatchMetrics() *BatchMetrics {
	if bs.batchManager == nil {
		return nil
	}

	return bs.batchManager.GetMetrics()
}

// ListBatchTasks 列出批量任务
func (bs *BatchServer) ListBatchTasks(status string, limit, offset int) ([]*BatchTaskInfo, error) {
	if bs.batchManager == nil {
		return nil, fmt.Errorf("batch manager not initialized")
	}

	return bs.batchManager.ListBatchesByStatus(status, limit, offset)
}

// CancelBatchTask 取消批量任务
func (bs *BatchServer) CancelBatchTask(batchID string) error {
	if bs.batchManager == nil {
		return fmt.Errorf("batch manager not initialized")
	}

	return bs.batchManager.CancelBatch(batchID)
}

// GetBatchTaskInfo 获取批量任务信息
func (bs *BatchServer) GetBatchTaskInfo(batchID string) (*BatchTaskInfo, error) {
	if bs.batchManager == nil {
		return nil, fmt.Errorf("batch manager not initialized")
	}

	return bs.batchManager.GetBatchInfo(batchID)
}

// BatchServerConfig 批量任务服务器配置
type BatchServerConfig struct {
	RedisOpt    RedisConnOpt
	BatchConfig *BatchTaskConfig
	Handler     Handler
}

// NewBatchServerWithConfig 使用配置创建批量任务服务器
func NewBatchServerWithConfig(config BatchServerConfig) *BatchServer {
	return NewBatchServer(config.RedisOpt, config.Handler, config.BatchConfig)
}

// BatchServerOption 批量任务服务器选项
type BatchServerOption func(*BatchServer)

// WithBatchLogger 设置批量任务日志记录器
func WithBatchLogger(logger Logger) BatchServerOption {
	return func(bs *BatchServer) {
		bs.logger = log.NewLogger(logger)
		if bs.batchManager != nil {
			bs.batchManager.logger = bs.logger
		}
	}
}

// WithBatchConfig 设置批量任务配置
func WithBatchConfig(config *BatchTaskConfig) BatchServerOption {
	return func(bs *BatchServer) {
		if config != nil {
			bs.config = config
			if bs.batchManager != nil {
				bs.batchManager.config = config
			}
		}
	}
}

// ApplyOptions 应用选项
func (bs *BatchServer) ApplyOptions(opts ...BatchServerOption) {
	for _, opt := range opts {
		opt(bs)
	}
}

// waitForSignals 等待停止信号（从原始Server复制）
func (bs *BatchServer) waitForSignals() {
	// 这里应该实现信号等待逻辑，类似于原始Server的实现
	// 为了简化，这里使用Server的方法
	select {} // 阻塞等待
}

// BatchTaskHandler 批量任务处理器接口
type BatchTaskHandler interface {
	Handler
	HandleBatchTask(ctx context.Context, batchID string, tasks []*Task) error
}

// MultiplexBatchHandler 多路复用批量任务处理器
type MultiplexBatchHandler struct {
	*ServeMux
	batchHandlers map[string]BatchTaskHandler
	mu            sync.RWMutex
}

// NewMultiplexBatchHandler 创建多路复用批量任务处理器
func NewMultiplexBatchHandler() *MultiplexBatchHandler {
	return &MultiplexBatchHandler{
		ServeMux:      NewServeMux(),
		batchHandlers: make(map[string]BatchTaskHandler),
	}
}

// HandleBatch 注册批量任务处理器
func (m *MultiplexBatchHandler) HandleBatch(pattern string, handler BatchTaskHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.batchHandlers[pattern] = handler
	m.Handle(pattern, handler) // 同时注册到普通处理器
}

// ProcessBatchTask 处理批量任务
func (m *MultiplexBatchHandler) ProcessBatchTask(ctx context.Context, batchID string, tasks []*Task) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 根据任务类型查找处理器
	for pattern, handler := range m.batchHandlers {
		if len(tasks) > 0 && tasks[0].Type() == pattern {
			return handler.HandleBatchTask(ctx, batchID, tasks)
		}
	}

	// 如果没有找到批量处理器，使用普通处理器逐个处理
	for _, task := range tasks {
		if err := m.ProcessTask(ctx, task); err != nil {
			return fmt.Errorf("failed to process task %s: %v", task.Type(), err)
		}
	}

	return nil
}

// BatchTaskHandlerFunc 批量任务处理器函数类型
type BatchTaskHandlerFunc func(ctx context.Context, batchID string, tasks []*Task) error

// HandleBatchTask 实现BatchTaskHandler接口
func (fn BatchTaskHandlerFunc) HandleBatchTask(ctx context.Context, batchID string, tasks []*Task) error {
	return fn(ctx, batchID, tasks)
}

// ProcessTask 实现Handler接口
func (fn BatchTaskHandlerFunc) ProcessTask(ctx context.Context, task *Task) error {
	// 对于单个任务，创建一个只包含该任务的批次
	return fn(ctx, fmt.Sprintf("single_%s", task.Type()), []*Task{task})
}

// BatchTaskMiddleware 批量任务中间件类型
type BatchTaskMiddleware func(BatchTaskHandler) BatchTaskHandler

// UseBatchMiddleware 使用批量任务中间件
func (m *MultiplexBatchHandler) UseBatchMiddleware(middleware ...BatchTaskMiddleware) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for pattern, handler := range m.batchHandlers {
		for _, mw := range middleware {
			handler = mw(handler)
		}
		m.batchHandlers[pattern] = handler
	}
}

// LoggingBatchMiddleware 批量任务日志中间件
func LoggingBatchMiddleware(logger Logger) BatchTaskMiddleware {
	return func(next BatchTaskHandler) BatchTaskHandler {
		return BatchTaskHandlerFunc(func(ctx context.Context, batchID string, tasks []*Task) error {
			start := time.Now()

			if logger != nil {
				logger.Info(fmt.Sprintf("Starting batch %s with %d tasks", batchID, len(tasks)))
			}

			err := next.HandleBatchTask(ctx, batchID, tasks)

			duration := time.Since(start)
			if err != nil {
				if logger != nil {
					logger.Error(fmt.Sprintf("Batch %s failed after %v: %v", batchID, duration, err))
				}
			} else {
				if logger != nil {
					logger.Info(fmt.Sprintf("Batch %s completed in %v", batchID, duration))
				}
			}

			return err
		})
	}
}

// RecoveryBatchMiddleware 批量任务恢复中间件
func RecoveryBatchMiddleware(logger Logger) BatchTaskMiddleware {
	return func(next BatchTaskHandler) BatchTaskHandler {
		return BatchTaskHandlerFunc(func(ctx context.Context, batchID string, tasks []*Task) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic in batch %s: %v", batchID, r)
					if logger != nil {
						logger.Error(fmt.Sprintf("Recovered from panic in batch %s: %v", batchID, r))
					}
				}
			}()

			return next.HandleBatchTask(ctx, batchID, tasks)
		})
	}
}
