// Copyright 2024. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis 键前缀
const (
	batchKeyPrefix  = "asynq:batch:"
	batchIndexKey   = "asynq:batch:index"
	batchMetricsKey = "asynq:batch:metrics"
	batchCleanupKey = "asynq:batch:cleanup"
)

// storeBatch 存储批量任务到 Redis
func (btm *BatchTaskManager) storeBatch(ctx context.Context, batch *BatchTask) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %v", err)
	}

	key := batchKeyPrefix + batch.ID

	// 使用事务确保原子性
	pipe := btm.redis.TxPipeline()

	// 存储批量任务数据
	pipe.Set(ctx, key, data, btm.config.BatchTTL)

	// 添加到索引（按状态和创建时间）
	pipe.ZAdd(ctx, batchIndexKey, redis.Z{
		Score:  float64(batch.CreatedAt),
		Member: batch.ID,
	})

	// 添加到状态索引
	statusKey := fmt.Sprintf("%sstatus:%s", batchKeyPrefix, batch.Status)
	pipe.ZAdd(ctx, statusKey, redis.Z{
		Score:  float64(batch.CreatedAt),
		Member: batch.ID,
	})

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to store batch in redis: %v", err)
	}

	btm.logger.Debugf("Stored batch %s to Redis", batch.ID)
	return nil
}

// updateBatchStatus 更新批量任务状态
func (btm *BatchTaskManager) updateBatchStatus(ctx context.Context, batch *BatchTask) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %v", err)
	}

	key := batchKeyPrefix + batch.ID

	// 获取旧状态
	oldBatch, err := btm.getBatch(ctx, batch.ID)
	if err != nil {
		// 如果获取失败，直接存储新状态
		return btm.redis.Set(ctx, key, data, btm.config.BatchTTL).Err()
	}

	// 使用事务更新
	pipe := btm.redis.TxPipeline()

	// 更新批量任务数据
	pipe.Set(ctx, key, data, btm.config.BatchTTL)

	// 如果状态发生变化，更新状态索引
	if oldBatch.Status != batch.Status {
		// 从旧状态索引中移除
		oldStatusKey := fmt.Sprintf("%sstatus:%s", batchKeyPrefix, oldBatch.Status)
		pipe.ZRem(ctx, oldStatusKey, batch.ID)

		// 添加到新状态索引
		newStatusKey := fmt.Sprintf("%sstatus:%s", batchKeyPrefix, batch.Status)
		pipe.ZAdd(ctx, newStatusKey, redis.Z{
			Score:  float64(batch.UpdatedAt),
			Member: batch.ID,
		})
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to update batch status in redis: %v", err)
	}

	btm.logger.Debugf("Updated batch %s status to %s", batch.ID, batch.Status)
	return nil
}

// getBatch 从 Redis 获取批量任务
func (btm *BatchTaskManager) getBatch(ctx context.Context, batchID string) (*BatchTask, error) {
	key := batchKeyPrefix + batchID

	data, err := btm.redis.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("batch %s not found", batchID)
		}
		return nil, fmt.Errorf("failed to get batch from redis: %v", err)
	}

	var batch BatchTask
	err = json.Unmarshal([]byte(data), &batch)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal batch: %v", err)
	}

	return &batch, nil
}

// listBatches 列出批量任务
func (btm *BatchTaskManager) listBatches(ctx context.Context, status string, limit int, offset int) ([]*BatchTask, error) {
	var key string
	if status == "" {
		key = batchIndexKey
	} else {
		key = fmt.Sprintf("%sstatus:%s", batchKeyPrefix, status)
	}

	// 按时间倒序获取批量任务ID
	batchIDs, err := btm.redis.ZRevRange(ctx, key, int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list batch IDs: %v", err)
	}

	if len(batchIDs) == 0 {
		return []*BatchTask{}, nil
	}

	// 批量获取批量任务数据
	keys := make([]string, len(batchIDs))
	for i, batchID := range batchIDs {
		keys[i] = batchKeyPrefix + batchID
	}

	results, err := btm.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get batch data: %v", err)
	}

	batches := make([]*BatchTask, 0, len(results))
	for i, result := range results {
		if result == nil {
			btm.logger.Warnf("Batch %s data not found, removing from index", batchIDs[i])
			// 从索引中移除不存在的批量任务
			btm.redis.ZRem(ctx, key, batchIDs[i])
			continue
		}

		var batch BatchTask
		err = json.Unmarshal([]byte(result.(string)), &batch)
		if err != nil {
			btm.logger.Errorf("Failed to unmarshal batch %s: %v", batchIDs[i], err)
			continue
		}

		batches = append(batches, &batch)
	}

	return batches, nil
}

// deleteBatch 删除批量任务
func (btm *BatchTaskManager) deleteBatch(ctx context.Context, batchID string) error {
	// 先获取批量任务信息
	batch, err := btm.getBatch(ctx, batchID)
	if err != nil {
		return err
	}

	pipe := btm.redis.TxPipeline()

	// 删除批量任务数据
	key := batchKeyPrefix + batchID
	pipe.Del(ctx, key)

	// 从索引中移除
	pipe.ZRem(ctx, batchIndexKey, batchID)

	// 从状态索引中移除
	statusKey := fmt.Sprintf("%sstatus:%s", batchKeyPrefix, batch.Status)
	pipe.ZRem(ctx, statusKey, batchID)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete batch from redis: %v", err)
	}

	btm.logger.Infof("Deleted batch %s from Redis", batchID)
	return nil
}

// RecoverPendingBatches 恢复未完成的批量任务
func (btm *BatchTaskManager) RecoverPendingBatches() error {
	ctx := context.Background()
	btm.logger.Info("Starting recovery of pending batch tasks...")

	// 获取所有 pending 和 running 状态的批量任务
	pendingBatches, err := btm.listBatches(ctx, "pending", 1000, 0)
	if err != nil {
		return fmt.Errorf("failed to get pending batches: %v", err)
	}

	runningBatches, err := btm.listBatches(ctx, "running", 1000, 0)
	if err != nil {
		return fmt.Errorf("failed to get running batches: %v", err)
	}

	allBatches := append(pendingBatches, runningBatches...)
	recoveredCount := 0

	for _, batch := range allBatches {
		// 检查批量任务是否过期
		createdAt := time.Unix(0, batch.CreatedAt)
		if time.Since(createdAt) > btm.config.BatchTTL {
			btm.logger.Warnf("Batch %s has expired, marking as failed", batch.ID)
			batch.Status = "failed"
			batch.ErrorMsg = "batch expired during recovery"
			batch.UpdatedAt = time.Now().UnixNano()
			btm.updateBatchStatus(ctx, batch)
			continue
		}

		startTime := time.Unix(0, batch.StartTime)
		now := time.Now()

		btm.logger.Infof("Recovering batch %s (status: %s, start time: %v)",
			batch.ID, batch.Status, startTime)

		if batch.Status == "running" {
			// 正在运行的任务标记为失败，需要手动重新调度
			batch.Status = "failed"
			batch.ErrorMsg = "batch was running during application restart"
			batch.UpdatedAt = time.Now().UnixNano()
			btm.updateBatchStatus(ctx, batch)
			btm.logger.Warnf("Marked running batch %s as failed due to restart", batch.ID)
		} else if batch.Status == "pending" {
			if startTime.Before(now) {
				// 开始时间已过，立即开始执行
				btm.logger.Infof("Starting overdue batch %s immediately", batch.ID)
				go btm.executeBatch(batch)
			} else {
				// 重新创建定时器
				delay := startTime.Sub(now)
				btm.logger.Infof("Rescheduling batch %s to start in %v", batch.ID, delay)

				timer := time.AfterFunc(delay, func() {
					btm.executeBatch(batch)
				})

				btm.mu.Lock()
				btm.timers[batch.ID] = timer
				btm.mu.Unlock()
			}
			recoveredCount++
		}
	}

	btm.logger.Infof("Recovery completed: %d batches recovered, %d total batches processed",
		recoveredCount, len(allBatches))

	return nil
}

// cleanupRoutine 清理过期的批量任务
func (btm *BatchTaskManager) cleanupRoutine() {
	ticker := time.NewTicker(btm.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-btm.done:
			return
		case <-ticker.C:
			btm.performCleanup()
		}
	}
}

// performCleanup 执行清理操作
func (btm *BatchTaskManager) performCleanup() {
	ctx := context.Background()
	btm.logger.Debug("Starting batch cleanup...")

	// 清理已完成和失败的批量任务（超过TTL的）
	cutoffTime := time.Now().Add(-btm.config.BatchTTL).UnixNano()

	statuses := []string{"completed", "failed", "cancelled"}
	totalCleaned := 0

	for _, status := range statuses {
		cleaned, err := btm.cleanupByStatus(ctx, status, cutoffTime)
		if err != nil {
			btm.logger.Errorf("Failed to cleanup %s batches: %v", status, err)
			continue
		}
		totalCleaned += cleaned
	}

	if totalCleaned > 0 {
		btm.logger.Infof("Cleanup completed: %d batches cleaned", totalCleaned)
	}

	// 清理孤立的索引项
	btm.cleanupOrphanedIndexes(ctx)
}

// cleanupByStatus 按状态清理批量任务
func (btm *BatchTaskManager) cleanupByStatus(ctx context.Context, status string, cutoffTime int64) (int, error) {
	statusKey := fmt.Sprintf("%sstatus:%s", batchKeyPrefix, status)

	// 获取过期的批量任务ID
	batchIDs, err := btm.redis.ZRangeByScore(ctx, statusKey, &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%d", cutoffTime),
	}).Result()

	if err != nil {
		return 0, err
	}

	if len(batchIDs) == 0 {
		return 0, nil
	}

	// 批量删除
	pipe := btm.redis.TxPipeline()

	for _, batchID := range batchIDs {
		// 删除批量任务数据
		pipe.Del(ctx, batchKeyPrefix+batchID)

		// 从索引中移除
		pipe.ZRem(ctx, batchIndexKey, batchID)
		pipe.ZRem(ctx, statusKey, batchID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup batches: %v", err)
	}

	btm.logger.Debugf("Cleaned up %d %s batches", len(batchIDs), status)
	return len(batchIDs), nil
}

// cleanupOrphanedIndexes 清理孤立的索引项
func (btm *BatchTaskManager) cleanupOrphanedIndexes(ctx context.Context) {
	// 获取所有批量任务索引
	batchIDs, err := btm.redis.ZRange(ctx, batchIndexKey, 0, -1).Result()
	if err != nil {
		btm.logger.Errorf("Failed to get batch index: %v", err)
		return
	}

	if len(batchIDs) == 0 {
		return
	}

	// 检查哪些批量任务不存在
	keys := make([]string, len(batchIDs))
	for i, batchID := range batchIDs {
		keys[i] = batchKeyPrefix + batchID
	}

	results, err := btm.redis.MGet(ctx, keys...).Result()
	if err != nil {
		btm.logger.Errorf("Failed to check batch existence: %v", err)
		return
	}

	orphanedIDs := make([]string, 0)
	for i, result := range results {
		if result == nil {
			orphanedIDs = append(orphanedIDs, batchIDs[i])
		}
	}

	if len(orphanedIDs) == 0 {
		return
	}

	// 清理孤立的索引项
	pipe := btm.redis.TxPipeline()
	for _, batchID := range orphanedIDs {
		pipe.ZRem(ctx, batchIndexKey, batchID)

		// 也尝试从状态索引中移除
		for _, status := range []string{"pending", "running", "completed", "failed", "cancelled"} {
			statusKey := fmt.Sprintf("%sstatus:%s", batchKeyPrefix, status)
			pipe.ZRem(ctx, statusKey, batchID)
		}
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		btm.logger.Errorf("Failed to cleanup orphaned indexes: %v", err)
		return
	}

	btm.logger.Debugf("Cleaned up %d orphaned index entries", len(orphanedIDs))
}

// GetBatchStats 获取批量任务统计信息
func (btm *BatchTaskManager) GetBatchStats(ctx context.Context) (map[string]int64, error) {
	stats := make(map[string]int64)

	statuses := []string{"pending", "running", "completed", "failed", "cancelled"}

	for _, status := range statuses {
		statusKey := fmt.Sprintf("%sstatus:%s", batchKeyPrefix, status)
		count, err := btm.redis.ZCard(ctx, statusKey).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get %s batch count: %v", status, err)
		}
		stats[status] = count
	}

	// 总数
	total, err := btm.redis.ZCard(ctx, batchIndexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get total batch count: %v", err)
	}
	stats["total"] = total

	return stats, nil
}

// ListBatchesByStatus 根据状态列出批量任务
func (btm *BatchTaskManager) ListBatchesByStatus(status string, limit, offset int) ([]*BatchTaskInfo, error) {
	ctx := context.Background()

	batches, err := btm.listBatches(ctx, status, limit, offset)
	if err != nil {
		return nil, err
	}

	infos := make([]*BatchTaskInfo, len(batches))
	for i, batch := range batches {
		infos[i] = &BatchTaskInfo{
			BatchTask:  *batch,
			TotalTasks: len(batch.Tasks),
		}

		// 如果是已完成的任务，设置完成数量
		if batch.Status == "completed" {
			infos[i].CompletedCount = len(batch.Tasks)
		}
	}

	return infos, nil
}
