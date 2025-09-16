// Copyright 2024. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/log"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupBatchTest(t *testing.T) (*BatchClient, redis.UniversalClient) {
	redisOpt := &redis.Options{
		Addr: "localhost:6379",
		DB:   14, // 使用独立的测试数据库
	}

	redisClient := redis.NewClient(redisOpt)

	// 清理测试数据库
	err := redisClient.FlushDB(context.Background()).Err()
	require.NoError(t, err)

	config := DefaultBatchTaskConfig()
	config.BatchTTL = 1 * time.Hour // 测试时使用较短的TTL

	client := NewBatchClientFromRedisClient(redisClient, config)

	return client, redisClient
}

func teardownBatchTest(t *testing.T, client *BatchClient, redisClient redis.UniversalClient) {
	err := client.Close()
	assert.NoError(t, err)

	err = redisClient.FlushDB(context.Background()).Err()
	assert.NoError(t, err)

	err = redisClient.Close()
	assert.NoError(t, err)
}

func TestBatchScheduleWithPreciseInterval(t *testing.T) {
	client, redisClient := setupBatchTest(t)
	defer teardownBatchTest(t, client, redisClient)

	// 创建测试任务
	tasks := []*Task{
		NewTask("test_task", []byte("task1")),
		NewTask("test_task", []byte("task2")),
		NewTask("test_task", []byte("task3")),
	}

	interval := 100 * time.Millisecond
	startAt := time.Now().Add(1 * time.Second)

	// 调度批量任务
	batch, err := client.BatchScheduleWithPreciseInterval(
		context.Background(),
		tasks,
		interval,
		startAt,
		BatchQueue("test_queue"),
	)

	require.NoError(t, err)
	assert.NotEmpty(t, batch.ID)
	assert.Equal(t, "pending", batch.Status)
	assert.Equal(t, len(tasks), len(batch.Tasks))
	assert.Equal(t, int64(interval), batch.Interval)
	assert.Equal(t, "test_queue", batch.Queue)

	// 验证任务已保存到Redis
	batchInfo, err := client.GetBatchTask(batch.ID)
	require.NoError(t, err)
	assert.Equal(t, batch.ID, batchInfo.ID)
	assert.Equal(t, len(tasks), batchInfo.TotalTasks)
}

func TestBatchTaskValidation(t *testing.T) {
	client, redisClient := setupBatchTest(t)
	defer teardownBatchTest(t, client, redisClient)

	// 测试空任务列表
	_, err := client.BatchScheduleWithPreciseInterval(
		context.Background(),
		[]*Task{},
		100*time.Millisecond,
		time.Now().Add(1*time.Second),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tasks cannot be empty")

	// 测试任务数量超限
	tasks := make([]*Task, 50) // 超过默认的40个限制
	for i := range tasks {
		tasks[i] = NewTask("test_task", []byte("task"))
	}

	_, err = client.BatchScheduleWithPreciseInterval(
		context.Background(),
		tasks,
		100*time.Millisecond,
		time.Now().Add(1*time.Second),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "batch size")

	// 测试间隔过小
	tasks = []*Task{NewTask("test_task", []byte("task"))}
	_, err = client.BatchScheduleWithPreciseInterval(
		context.Background(),
		tasks,
		500*time.Microsecond, // 小于1ms
		time.Now().Add(1*time.Second),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "interval")
}

func TestBatchTaskTemplate(t *testing.T) {
	client, redisClient := setupBatchTest(t)
	defer teardownBatchTest(t, client, redisClient)

	// 创建批量任务模板
	template := NewBatchTaskTemplate("email_task").
		AddPayload([]byte("user1@example.com")).
		AddPayload([]byte("user2@example.com")).
		AddPayload([]byte("user3@example.com")).
		WithQueue("email_queue").
		WithMaxRetries(5).
		WithTimeout(30 * time.Second)

	// 使用模板调度任务
	batch, err := client.BatchScheduleWithTemplate(
		context.Background(),
		template,
		150*time.Millisecond,
		time.Now().Add(2*time.Second),
	)

	require.NoError(t, err)
	assert.Equal(t, 3, len(batch.Tasks))
	assert.Equal(t, "email_queue", batch.Queue)

	// 验证任务内容
	for i, task := range batch.Tasks {
		assert.Equal(t, "email_task", task.Type())
		expectedPayload := []byte(fmt.Sprintf("user%d@example.com", i+1))
		assert.Equal(t, expectedPayload, task.Payload())
	}
}

func TestBatchTaskBuilder(t *testing.T) {
	// 创建批量任务构建器
	builder := NewBatchTaskBuilder("notification").
		AddTaskString("Hello User 1").
		AddTaskString("Hello User 2").
		AddTaskInterface(map[string]string{"message": "Hello User 3"}).
		WithOptions(Queue("notification_queue"), MaxRetry(3))

	tasks := builder.Build()
	assert.Equal(t, 3, len(tasks))

	for _, task := range tasks {
		assert.Equal(t, "notification", task.Type())
	}
}

func TestTimingController(t *testing.T) {
	logger := log.NewLogger(nil)
	controller := NewTimingController(logger, 5*time.Millisecond)

	// 测试精确等待
	targetTime := time.Now().Add(50 * time.Millisecond)
	startTime := time.Now()

	timeError := controller.WaitUntil(targetTime, "test_task")
	actualDuration := time.Since(startTime)

	// 验证时间精度
	expectedDuration := 50 * time.Millisecond
	assert.True(t, timeError <= 10*time.Millisecond,
		"Time error %v should be <= 10ms", timeError)
	assert.True(t, actualDuration >= expectedDuration-5*time.Millisecond &&
		actualDuration <= expectedDuration+15*time.Millisecond,
		"Actual duration %v should be close to expected %v", actualDuration, expectedDuration)

	// 获取统计信息
	stats := controller.GetStatistics()
	assert.Equal(t, int64(1), stats.TotalTasks)
	assert.True(t, stats.AvgTimeError <= 10*time.Millisecond)
}

func TestBatchTaskExecution(t *testing.T) {
	client, redisClient := setupBatchTest(t)
	defer teardownBatchTest(t, client, redisClient)

	// 创建短间隔的测试任务
	tasks := []*Task{
		NewTask("quick_task", []byte("task1")),
		NewTask("quick_task", []byte("task2")),
	}

	interval := 50 * time.Millisecond
	startAt := time.Now().Add(100 * time.Millisecond)

	// 调度批量任务
	batch, err := client.BatchScheduleWithPreciseInterval(
		context.Background(),
		tasks,
		interval,
		startAt,
	)

	require.NoError(t, err)

	// 等待任务开始执行
	time.Sleep(150 * time.Millisecond)

	// 检查批量任务状态
	batchInfo, err := client.GetBatchTask(batch.ID)
	require.NoError(t, err)

	// 任务应该正在执行或已完成
	assert.True(t, batchInfo.Status == "running" || batchInfo.Status == "completed",
		"Batch status should be running or completed, got: %s", batchInfo.Status)

	// 等待任务完成
	time.Sleep(200 * time.Millisecond)

	batchInfo, err = client.GetBatchTask(batch.ID)
	require.NoError(t, err)
	assert.Equal(t, "completed", batchInfo.Status)
}

func TestBatchTaskCancellation(t *testing.T) {
	client, redisClient := setupBatchTest(t)
	defer teardownBatchTest(t, client, redisClient)

	// 创建长延迟的测试任务
	tasks := []*Task{
		NewTask("long_task", []byte("task1")),
	}

	startAt := time.Now().Add(5 * time.Second) // 5秒后开始

	batch, err := client.BatchScheduleWithPreciseInterval(
		context.Background(),
		tasks,
		100*time.Millisecond,
		startAt,
	)

	require.NoError(t, err)
	assert.Equal(t, "pending", batch.Status)

	// 取消批量任务
	err = client.CancelBatchTask(batch.ID)
	require.NoError(t, err)

	// 验证任务已被取消
	batchInfo, err := client.GetBatchTask(batch.ID)
	require.NoError(t, err)
	assert.Equal(t, "cancelled", batchInfo.Status)
}

func TestBatchTaskRecovery(t *testing.T) {
	client, redisClient := setupBatchTest(t)
	defer teardownBatchTest(t, client, redisClient)

	// 创建测试任务
	tasks := []*Task{
		NewTask("recovery_task", []byte("task1")),
	}

	startAt := time.Now().Add(100 * time.Millisecond)

	batch, err := client.BatchScheduleWithPreciseInterval(
		context.Background(),
		tasks,
		100*time.Millisecond,
		startAt,
	)

	require.NoError(t, err)

	// 模拟失败状态
	batch.Status = "failed"
	batch.ErrorMsg = "simulated failure"
	err = client.batchManager.updateBatchStatus(context.Background(), batch)
	require.NoError(t, err)

	// 测试恢复功能
	err = client.RecoverPendingBatches()
	assert.NoError(t, err)
}

func TestBatchTaskMetrics(t *testing.T) {
	client, redisClient := setupBatchTest(t)
	defer teardownBatchTest(t, client, redisClient)

	// 创建多个批量任务
	for i := 0; i < 3; i++ {
		tasks := []*Task{
			NewTask("metrics_task", []byte(fmt.Sprintf("task_%d", i))),
		}

		_, err := client.BatchScheduleWithPreciseInterval(
			context.Background(),
			tasks,
			100*time.Millisecond,
			time.Now().Add(time.Duration(i+1)*100*time.Millisecond),
		)
		require.NoError(t, err)
	}

	// 获取统计信息
	stats, err := client.GetBatchStats()
	require.NoError(t, err)

	assert.Equal(t, int64(3), stats["pending"])
	assert.Equal(t, int64(3), stats["total"])

	// 获取指标
	metrics := client.GetBatchMetrics()
	require.NotNil(t, metrics)
	assert.Equal(t, int64(3), metrics.TotalBatches)
}

func TestCreateBatchFromVariousTypes(t *testing.T) {
	// 测试从字符串创建批量任务
	strings := []string{"task1", "task2", "task3"}
	tasks, err := CreateBatchFromStrings("string_task", strings)
	require.NoError(t, err)
	assert.Equal(t, 3, len(tasks))

	for i, task := range tasks {
		assert.Equal(t, "string_task", task.Type())
		assert.Equal(t, []byte(strings[i]), task.Payload())
	}

	// 测试从接口创建批量任务
	interfaces := []interface{}{
		map[string]string{"name": "John"},
		map[string]string{"name": "Jane"},
	}
	tasks, err = CreateBatchFromInterface("interface_task", interfaces)
	require.NoError(t, err)
	assert.Equal(t, 2, len(tasks))

	for _, task := range tasks {
		assert.Equal(t, "interface_task", task.Type())
		assert.NotEmpty(t, task.Payload())
	}
}

func TestBatchErrorHandling(t *testing.T) {
	logger := log.NewLogger(nil)
	handler := NewDefaultBatchErrorHandler(logger)

	// 测试可重试错误
	retryableErr := NewBatchError(BatchErrorExecution, "batch_1", 0, "execution failed", nil)
	assert.True(t, handler.CanRetry(retryableErr))

	delay := handler.GetRetryDelay(retryableErr, 1)
	assert.Greater(t, delay, time.Duration(0))

	// 测试不可重试错误
	nonRetryableErr := NewBatchError(BatchErrorValidation, "batch_1", 0, "validation failed", nil)
	assert.False(t, handler.CanRetry(nonRetryableErr))

	// 测试错误处理
	err := handler.HandleError(context.Background(), retryableErr)
	assert.NoError(t, err)
	assert.NotEmpty(t, retryableErr.Context)
}

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	// 初始状态应该是关闭的
	assert.Equal(t, CircuitClosed, cb.GetState())

	// 模拟连续失败
	for i := 0; i < 3; i++ {
		err := cb.Call(func() error {
			return fmt.Errorf("simulated error")
		})
		assert.Error(t, err)
	}

	// 熔断器应该打开
	assert.Equal(t, CircuitOpen, cb.GetState())

	// 此时调用应该被拒绝
	err := cb.Call(func() error {
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "circuit breaker is open")

	// 等待重置时间后应该变为半开状态
	time.Sleep(1100 * time.Millisecond)

	// 成功调用应该关闭熔断器
	err = cb.Call(func() error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, CircuitClosed, cb.GetState())
}

func TestBatchTaskScheduler(t *testing.T) {
	client, redisClient := setupBatchTest(t)
	defer teardownBatchTest(t, client, redisClient)

	scheduler := NewBatchTaskScheduler(client)
	scheduler.SetDefaults(
		100*time.Millisecond,
		"scheduler_queue",
		time.Now().Add(1*time.Second),
	)

	// 测试邮件调度
	emails := []string{"user1@test.com", "user2@test.com"}
	batch, err := scheduler.ScheduleEmails(context.Background(), emails)
	require.NoError(t, err)
	assert.Equal(t, 2, len(batch.Tasks))
	assert.Equal(t, "scheduler_queue", batch.Queue)

	// 测试自定义任务调度
	payloads := [][]byte{[]byte("data1"), []byte("data2")}
	batch, err = scheduler.ScheduleCustomTasks(context.Background(), "custom_task", payloads)
	require.NoError(t, err)
	assert.Equal(t, 2, len(batch.Tasks))
}

// 基准测试
func BenchmarkBatchScheduling(b *testing.B) {
	client, redisClient := setupBatchTest(&testing.T{})
	defer teardownBatchTest(&testing.T{}, client, redisClient)

	// 准备测试数据
	tasks := make([]*Task, 10)
	for i := range tasks {
		tasks[i] = NewTask("bench_task", []byte(fmt.Sprintf("task_%d", i)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := client.BatchScheduleWithPreciseInterval(
			context.Background(),
			tasks,
			10*time.Millisecond,
			time.Now().Add(time.Duration(i)*time.Second),
		)
		if err != nil {
			b.Fatalf("Failed to schedule batch: %v", err)
		}
	}
}

func BenchmarkTimingController(b *testing.B) {
	logger := log.NewLogger(nil)
	controller := NewTimingController(logger, 5*time.Millisecond)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		targetTime := time.Now().Add(1 * time.Millisecond)
		controller.WaitUntil(targetTime, fmt.Sprintf("bench_task_%d", i))
	}
}

// 集成测试
func TestBatchTaskIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client, redisClient := setupBatchTest(t)
	defer teardownBatchTest(t, client, redisClient)

	// 创建一个完整的批量任务流程
	tasks := make([]*Task, 5)
	for i := range tasks {
		tasks[i] = NewTask("integration_task", []byte(fmt.Sprintf("task_%d", i)))
	}

	interval := 200 * time.Millisecond
	startAt := time.Now().Add(500 * time.Millisecond)

	// 1. 调度批量任务
	batch, err := client.BatchScheduleWithPreciseInterval(
		context.Background(),
		tasks,
		interval,
		startAt,
		BatchQueue("integration_queue"),
	)
	require.NoError(t, err)

	// 2. 验证初始状态
	batchInfo, err := client.GetBatchTask(batch.ID)
	require.NoError(t, err)
	assert.Equal(t, "pending", batchInfo.Status)

	// 3. 等待任务开始执行
	time.Sleep(700 * time.Millisecond)

	batchInfo, err = client.GetBatchTask(batch.ID)
	require.NoError(t, err)
	assert.True(t, batchInfo.Status == "running" || batchInfo.Status == "completed")

	// 4. 等待所有任务完成
	totalTime := time.Duration(len(tasks)) * interval
	time.Sleep(totalTime + 500*time.Millisecond)

	batchInfo, err = client.GetBatchTask(batch.ID)
	require.NoError(t, err)
	assert.Equal(t, "completed", batchInfo.Status)
	assert.Equal(t, len(tasks), batchInfo.CompletedCount)

	// 5. 验证指标
	metrics := client.GetBatchMetrics()
	assert.Greater(t, metrics.TotalBatches, int64(0))
	assert.Greater(t, metrics.CompletedBatches, int64(0))
}
