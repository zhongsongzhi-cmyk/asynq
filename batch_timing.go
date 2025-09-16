// Copyright 2024. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/log"
)

// PreciseTimer 精确定时器
type PreciseTimer struct {
	targetTime time.Time
	precision  time.Duration
	callback   func()
	done       chan struct{}
	started    bool
	mu         sync.Mutex
}

// NewPreciseTimer 创建精确定时器
func NewPreciseTimer(targetTime time.Time, callback func()) *PreciseTimer {
	return &PreciseTimer{
		targetTime: targetTime,
		precision:  time.Microsecond,
		callback:   callback,
		done:       make(chan struct{}),
	}
}

// Start 启动定时器
func (pt *PreciseTimer) Start() {
	pt.mu.Lock()
	if pt.started {
		pt.mu.Unlock()
		return
	}
	pt.started = true
	pt.mu.Unlock()

	go pt.run()
}

// Stop 停止定时器
func (pt *PreciseTimer) Stop() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if !pt.started {
		return
	}

	close(pt.done)
	pt.started = false
}

// run 运行定时器
func (pt *PreciseTimer) run() {
	now := time.Now()
	if pt.targetTime.Before(now) {
		// 目标时间已过，立即执行
		pt.callback()
		return
	}

	duration := pt.targetTime.Sub(now)

	// 如果等待时间很短，使用精确等待
	if duration <= 100*time.Millisecond {
		pt.preciseWait(duration)
		return
	}

	// 如果等待时间较长，先粗略等待，然后精确等待
	roughWait := duration - 50*time.Millisecond
	timer := time.NewTimer(roughWait)

	select {
	case <-pt.done:
		timer.Stop()
		return
	case <-timer.C:
		// 继续精确等待
		remaining := pt.targetTime.Sub(time.Now())
		if remaining > 0 {
			pt.preciseWait(remaining)
		} else {
			pt.callback()
		}
	}
}

// preciseWait 精确等待
func (pt *PreciseTimer) preciseWait(duration time.Duration) {
	if duration <= 0 {
		pt.callback()
		return
	}

	// 使用忙等待进行精确控制
	startTime := time.Now()
	for {
		select {
		case <-pt.done:
			return
		default:
			elapsed := time.Since(startTime)
			if elapsed >= duration {
				pt.callback()
				return
			}

			// 如果剩余时间很短，使用忙等待
			remaining := duration - elapsed
			if remaining <= time.Millisecond {
				// 忙等待
				for time.Since(startTime) < duration {
					// 检查是否被取消
					select {
					case <-pt.done:
						return
					default:
					}
				}
				pt.callback()
				return
			} else {
				// 短暂休眠
				time.Sleep(remaining / 10)
			}
		}
	}
}

// TimingController 时间控制器
type TimingController struct {
	logger         *log.Logger
	maxTimeError   time.Duration
	enableTracking bool
	statistics     *TimingStatistics
	mu             sync.RWMutex
}

// TimingStatistics 时间统计
type TimingStatistics struct {
	TotalTasks     int64         `json:"total_tasks"`
	TotalTimeError time.Duration `json:"total_time_error"`
	MaxTimeError   time.Duration `json:"max_time_error"`
	MinTimeError   time.Duration `json:"min_time_error"`
	AvgTimeError   time.Duration `json:"avg_time_error"`
	ErrorCount     int64         `json:"error_count"`
	AccuracyRate   float64       `json:"accuracy_rate"`
	LastUpdated    time.Time     `json:"last_updated"`
}

// NewTimingController 创建时间控制器
func NewTimingController(logger *log.Logger, maxTimeError time.Duration) *TimingController {
	return &TimingController{
		logger:         logger,
		maxTimeError:   maxTimeError,
		enableTracking: true,
		statistics: &TimingStatistics{
			MinTimeError: time.Hour, // 初始化为较大值
			LastUpdated:  time.Now(),
		},
	}
}

// WaitUntil 等待到指定时间
func (tc *TimingController) WaitUntil(targetTime time.Time, taskInfo string) time.Duration {
	startWait := time.Now()

	if targetTime.Before(startWait) {
		actualTime := startWait
		timeError := actualTime.Sub(targetTime)
		tc.recordTiming(targetTime, actualTime, timeError, taskInfo)
		return timeError
	}

	duration := targetTime.Sub(startWait)

	// 使用精确等待
	tc.preciseWait(duration)

	actualTime := time.Now()
	timeError := actualTime.Sub(targetTime)
	if timeError < 0 {
		timeError = -timeError
	}

	tc.recordTiming(targetTime, actualTime, timeError, taskInfo)

	return timeError
}

// preciseWait 精确等待实现
func (tc *TimingController) preciseWait(duration time.Duration) {
	if duration <= 0 {
		return
	}

	// 如果等待时间很短（小于10ms），使用高精度等待
	if duration <= 10*time.Millisecond {
		tc.highPrecisionWait(duration)
		return
	}

	// 如果等待时间较长，先使用Timer粗略等待，然后精确等待
	roughWait := duration - 5*time.Millisecond
	timer := time.NewTimer(roughWait)
	<-timer.C

	// 精确等待剩余时间
	remaining := duration - roughWait
	tc.highPrecisionWait(remaining)
}

// highPrecisionWait 高精度等待
func (tc *TimingController) highPrecisionWait(duration time.Duration) {
	if duration <= 0 {
		return
	}

	startTime := time.Now()

	// 使用hybrid方式：先sleep大部分时间，最后一点时间用忙等待
	if duration > time.Millisecond {
		sleepDuration := duration - 500*time.Microsecond
		time.Sleep(sleepDuration)
	}

	// 忙等待最后的时间
	for time.Since(startTime) < duration {
		// 空循环，精确控制时间
	}
}

// recordTiming 记录时间信息
func (tc *TimingController) recordTiming(targetTime, actualTime time.Time, timeError time.Duration, taskInfo string) {
	if !tc.enableTracking {
		return
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.statistics.TotalTasks++
	tc.statistics.TotalTimeError += timeError
	tc.statistics.LastUpdated = time.Now()

	// 更新最大和最小时间误差
	if timeError > tc.statistics.MaxTimeError {
		tc.statistics.MaxTimeError = timeError
	}
	if timeError < tc.statistics.MinTimeError {
		tc.statistics.MinTimeError = timeError
	}

	// 计算平均时间误差
	tc.statistics.AvgTimeError = tc.statistics.TotalTimeError / time.Duration(tc.statistics.TotalTasks)

	// 计算准确率（时间误差在阈值内的比例）
	if timeError <= tc.maxTimeError {
		tc.statistics.AccuracyRate = float64(tc.statistics.TotalTasks-tc.statistics.ErrorCount) / float64(tc.statistics.TotalTasks)
	} else {
		tc.statistics.ErrorCount++
		tc.statistics.AccuracyRate = float64(tc.statistics.TotalTasks-tc.statistics.ErrorCount) / float64(tc.statistics.TotalTasks)

		// 记录超出阈值的时间误差
		if tc.logger != nil {
			tc.logger.Warnf("Time error exceeded threshold for %s: %v (threshold: %v, target: %v, actual: %v)",
				taskInfo, timeError, tc.maxTimeError, targetTime, actualTime)
		}
	}
}

// GetStatistics 获取时间统计信息
func (tc *TimingController) GetStatistics() *TimingStatistics {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	// 返回副本以避免并发访问问题
	return &TimingStatistics{
		TotalTasks:     tc.statistics.TotalTasks,
		TotalTimeError: tc.statistics.TotalTimeError,
		MaxTimeError:   tc.statistics.MaxTimeError,
		MinTimeError:   tc.statistics.MinTimeError,
		AvgTimeError:   tc.statistics.AvgTimeError,
		ErrorCount:     tc.statistics.ErrorCount,
		AccuracyRate:   tc.statistics.AccuracyRate,
		LastUpdated:    tc.statistics.LastUpdated,
	}
}

// ResetStatistics 重置统计信息
func (tc *TimingController) ResetStatistics() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.statistics = &TimingStatistics{
		MinTimeError: time.Hour,
		LastUpdated:  time.Now(),
	}
}

// SetMaxTimeError 设置最大时间误差阈值
func (tc *TimingController) SetMaxTimeError(maxError time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.maxTimeError = maxError
}

// EnableTracking 启用时间跟踪
func (tc *TimingController) EnableTracking(enable bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.enableTracking = enable
}

// BatchTimingManager 批量时间管理器
type BatchTimingManager struct {
	controller     *TimingController
	batchSchedules map[string]*BatchTimingSchedule
	mu             sync.RWMutex
}

// BatchTimingSchedule 批量时间调度
type BatchTimingSchedule struct {
	BatchID     string          `json:"batch_id"`
	StartTime   time.Time       `json:"start_time"`
	Interval    time.Duration   `json:"interval"`
	TaskCount   int             `json:"task_count"`
	TaskTimes   []time.Time     `json:"task_times"`
	ActualTimes []time.Time     `json:"actual_times"`
	TimeErrors  []time.Duration `json:"time_errors"`
}

// NewBatchTimingManager 创建批量时间管理器
func NewBatchTimingManager(controller *TimingController) *BatchTimingManager {
	return &BatchTimingManager{
		controller:     controller,
		batchSchedules: make(map[string]*BatchTimingSchedule),
	}
}

// CreateSchedule 创建批量时间调度
func (btm *BatchTimingManager) CreateSchedule(batchID string, startTime time.Time, interval time.Duration, taskCount int) *BatchTimingSchedule {
	schedule := &BatchTimingSchedule{
		BatchID:     batchID,
		StartTime:   startTime,
		Interval:    interval,
		TaskCount:   taskCount,
		TaskTimes:   make([]time.Time, taskCount),
		ActualTimes: make([]time.Time, taskCount),
		TimeErrors:  make([]time.Duration, taskCount),
	}

	// 预计算所有任务的目标执行时间
	for i := 0; i < taskCount; i++ {
		schedule.TaskTimes[i] = startTime.Add(time.Duration(i) * interval)
	}

	btm.mu.Lock()
	btm.batchSchedules[batchID] = schedule
	btm.mu.Unlock()

	return schedule
}

// WaitForTask 等待指定任务的执行时间
func (btm *BatchTimingManager) WaitForTask(batchID string, taskIndex int) time.Duration {
	btm.mu.RLock()
	schedule, exists := btm.batchSchedules[batchID]
	btm.mu.RUnlock()

	if !exists || taskIndex >= len(schedule.TaskTimes) {
		return 0
	}

	targetTime := schedule.TaskTimes[taskIndex]
	taskInfo := fmt.Sprintf("batch:%s task:%d", batchID, taskIndex)

	timeError := btm.controller.WaitUntil(targetTime, taskInfo)

	// 记录实际执行时间
	btm.mu.Lock()
	schedule.ActualTimes[taskIndex] = time.Now()
	schedule.TimeErrors[taskIndex] = timeError
	btm.mu.Unlock()

	return timeError
}

// GetSchedule 获取批量时间调度
func (btm *BatchTimingManager) GetSchedule(batchID string) (*BatchTimingSchedule, bool) {
	btm.mu.RLock()
	defer btm.mu.RUnlock()

	schedule, exists := btm.batchSchedules[batchID]
	return schedule, exists
}

// RemoveSchedule 移除批量时间调度
func (btm *BatchTimingManager) RemoveSchedule(batchID string) {
	btm.mu.Lock()
	defer btm.mu.Unlock()

	delete(btm.batchSchedules, batchID)
}

// GetAllSchedules 获取所有批量时间调度
func (btm *BatchTimingManager) GetAllSchedules() map[string]*BatchTimingSchedule {
	btm.mu.RLock()
	defer btm.mu.RUnlock()

	// 返回副本
	result := make(map[string]*BatchTimingSchedule)
	for k, v := range btm.batchSchedules {
		result[k] = v
	}
	return result
}

// CalculateScheduleAccuracy 计算调度准确率
func (btm *BatchTimingManager) CalculateScheduleAccuracy(batchID string) (float64, error) {
	btm.mu.RLock()
	schedule, exists := btm.batchSchedules[batchID]
	btm.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("schedule for batch %s not found", batchID)
	}

	accurateCount := 0
	totalCount := 0

	for i, timeError := range schedule.TimeErrors {
		if schedule.ActualTimes[i].IsZero() {
			continue // 任务尚未执行
		}

		totalCount++
		if timeError <= btm.controller.maxTimeError {
			accurateCount++
		}
	}

	if totalCount == 0 {
		return 0, nil
	}

	return float64(accurateCount) / float64(totalCount), nil
}

// AdaptiveTimingController 自适应时间控制器
type AdaptiveTimingController struct {
	*TimingController
	adaptiveEnabled  bool
	adjustmentFactor float64
	minAdjustment    time.Duration
	maxAdjustment    time.Duration
}

// NewAdaptiveTimingController 创建自适应时间控制器
func NewAdaptiveTimingController(logger *log.Logger, maxTimeError time.Duration) *AdaptiveTimingController {
	return &AdaptiveTimingController{
		TimingController: NewTimingController(logger, maxTimeError),
		adaptiveEnabled:  true,
		adjustmentFactor: 0.1,
		minAdjustment:    -time.Millisecond,
		maxAdjustment:    time.Millisecond,
	}
}

// WaitUntilAdaptive 自适应等待到指定时间
func (atc *AdaptiveTimingController) WaitUntilAdaptive(targetTime time.Time, taskInfo string) time.Duration {
	if !atc.adaptiveEnabled {
		return atc.WaitUntil(targetTime, taskInfo)
	}

	// 根据历史统计调整目标时间
	adjustment := atc.calculateAdjustment()
	adjustedTargetTime := targetTime.Add(adjustment)

	return atc.WaitUntil(adjustedTargetTime, taskInfo)
}

// calculateAdjustment 计算时间调整量
func (atc *AdaptiveTimingController) calculateAdjustment() time.Duration {
	stats := atc.GetStatistics()

	if stats.TotalTasks < 10 {
		return 0 // 样本不足，不进行调整
	}

	// 基于平均时间误差计算调整量
	adjustment := time.Duration(float64(stats.AvgTimeError) * atc.adjustmentFactor)

	// 限制调整范围
	if adjustment < atc.minAdjustment {
		adjustment = atc.minAdjustment
	} else if adjustment > atc.maxAdjustment {
		adjustment = atc.maxAdjustment
	}

	return -adjustment // 负调整，提前执行以补偿延迟
}

// EnableAdaptive 启用自适应模式
func (atc *AdaptiveTimingController) EnableAdaptive(enable bool) {
	atc.adaptiveEnabled = enable
}

// SetAdjustmentParameters 设置调整参数
func (atc *AdaptiveTimingController) SetAdjustmentParameters(factor float64, min, max time.Duration) {
	atc.adjustmentFactor = factor
	atc.minAdjustment = min
	atc.maxAdjustment = max
}
