// Copyright 2024. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Example usage of the batch task functionality
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

func main() {
	// 创建批量任务客户端
	client := asynq.NewBatchClient(
		asynq.RedisClientOpt{Addr: "localhost:6379"},
		&asynq.BatchTaskConfig{
			MaxBatchSize:    40,
			DefaultInterval: 100 * time.Millisecond,
			MaxTimeError:    5 * time.Millisecond,
			RetryOnFailure:  true,
			MaxRetries:      3,
		},
	)
	defer client.Close()

	// 示例1: 基本批量调度
	basicBatchExample(client)

	// 示例2: 使用模板创建批量任务
	templateExample(client)

	// 示例3: 使用构建器创建批量任务
	builderExample(client)

	// 示例4: 高级调度选项
	advancedSchedulingExample(client)

	// 示例5: 批量任务监控
	monitoringExample(client)

	// 示例6: 错误处理和恢复
	errorHandlingExample(client)
}

// 基本批量调度示例
func basicBatchExample(client *asynq.BatchClient) {
	fmt.Println("=== 基本批量调度示例 ===")

	// 创建任务列表
	tasks := []*asynq.Task{
		asynq.NewTask("send_email", []byte("user1@example.com")),
		asynq.NewTask("send_email", []byte("user2@example.com")),
		asynq.NewTask("send_email", []byte("user3@example.com")),
		asynq.NewTask("send_email", []byte("user4@example.com")),
		asynq.NewTask("send_email", []byte("user5@example.com")),
	}

	// 批量调度，每个任务间隔100ms
	batch, err := client.BatchScheduleWithPreciseInterval(
		context.Background(),
		tasks,
		100*time.Millisecond,
		time.Now().Add(1*time.Second), // 1秒后开始
		asynq.BatchQueue("email_queue"),
	)

	if err != nil {
		log.Fatalf("批量调度失败: %v", err)
	}

	fmt.Printf("成功调度批量任务: ID=%s, 任务数量=%d, 开始时间=%v\n",
		batch.ID, len(batch.Tasks), time.Unix(0, batch.StartTime))

	// 监控批量任务状态
	monitorBatch(client, batch.ID)
}

// 使用模板创建批量任务示例
func templateExample(client *asynq.BatchClient) {
	fmt.Println("\n=== 模板批量任务示例 ===")

	// 创建批量任务模板
	template := asynq.NewBatchTaskTemplate("process_order").
		AddPayload([]byte(`{"order_id": "001", "amount": 100.00}`)).
		AddPayload([]byte(`{"order_id": "002", "amount": 250.50}`)).
		AddPayload([]byte(`{"order_id": "003", "amount": 75.25}`)).
		WithQueue("order_queue").
		WithMaxRetries(5).
		WithTimeout(30 * time.Second)

	// 使用模板调度任务
	batch, err := client.BatchScheduleWithTemplate(
		context.Background(),
		template,
		200*time.Millisecond,
		time.Now().Add(2*time.Second),
		asynq.BatchRetryOnFailure(true),
	)

	if err != nil {
		log.Fatalf("模板批量调度失败: %v", err)
	}

	fmt.Printf("使用模板成功调度批量任务: ID=%s\n", batch.ID)
}

// 使用构建器创建批量任务示例
func builderExample(client *asynq.BatchClient) {
	fmt.Println("\n=== 构建器批量任务示例 ===")

	// 使用构建器创建批量任务
	builder := asynq.NewBatchTaskBuilder("notification").
		AddTaskString("Welcome message for user 1").
		AddTaskString("Welcome message for user 2").
		AddTaskInterface(map[string]interface{}{
			"type":    "welcome",
			"user_id": 123,
			"message": "Hello World",
		}).
		WithOptions(asynq.Queue("notification_queue"), asynq.MaxRetry(3))

	// 构建并调度任务
	batch, err := builder.BuildAndSchedule(
		client,
		context.Background(),
		150*time.Millisecond,
		time.Now().Add(3*time.Second),
		asynq.BatchPriority(5),
	)

	if err != nil {
		log.Fatalf("构建器批量调度失败: %v", err)
	}

	fmt.Printf("使用构建器成功调度批量任务: ID=%s\n", batch.ID)
}

// 高级调度选项示例
func advancedSchedulingExample(client *asynq.BatchClient) {
	fmt.Println("\n=== 高级调度选项示例 ===")

	// 创建高级批量调度器
	scheduler := asynq.NewBatchTaskScheduler(client)
	scheduler.SetDefaults(
		80*time.Millisecond,           // 默认间隔
		"priority_queue",              // 默认队列
		time.Now().Add(4*time.Second), // 默认开始时间
		asynq.BatchPriority(10),       // 默认优先级
	)

	// 调度邮件任务
	emails := []string{
		"admin@example.com",
		"support@example.com",
		"sales@example.com",
	}

	batch, err := scheduler.ScheduleEmails(
		context.Background(),
		emails,
		asynq.BatchMaxRetries(5),
		asynq.BatchTimeout(60*time.Second),
	)

	if err != nil {
		log.Fatalf("高级调度失败: %v", err)
	}

	fmt.Printf("高级调度成功: ID=%s\n", batch.ID)

	// 调度自定义任务
	payloads := [][]byte{
		[]byte(`{"action": "backup", "database": "users"}`),
		[]byte(`{"action": "backup", "database": "orders"}`),
		[]byte(`{"action": "backup", "database": "products"}`),
	}

	batch2, err := scheduler.ScheduleCustomTasks(
		context.Background(),
		"database_backup",
		payloads,
		asynq.BatchRetryOnFailure(true),
	)

	if err != nil {
		log.Fatalf("自定义任务调度失败: %v", err)
	}

	fmt.Printf("自定义任务调度成功: ID=%s\n", batch2.ID)
}

// 批量任务监控示例
func monitoringExample(client *asynq.BatchClient) {
	fmt.Println("\n=== 批量任务监控示例 ===")

	// 获取批量任务统计信息
	stats, err := client.GetBatchStats()
	if err != nil {
		log.Printf("获取统计信息失败: %v", err)
		return
	}

	fmt.Printf("批量任务统计:\n")
	fmt.Printf("  总数: %d\n", stats["total"])
	fmt.Printf("  待执行: %d\n", stats["pending"])
	fmt.Printf("  执行中: %d\n", stats["running"])
	fmt.Printf("  已完成: %d\n", stats["completed"])
	fmt.Printf("  失败: %d\n", stats["failed"])

	// 获取批量任务指标
	metrics := client.GetBatchMetrics()
	if metrics != nil {
		fmt.Printf("批量任务指标:\n")
		fmt.Printf("  总批次: %d\n", metrics.TotalBatches)
		fmt.Printf("  完成批次: %d\n", metrics.CompletedBatches)
		fmt.Printf("  失败批次: %d\n", metrics.FailedBatches)
		fmt.Printf("  平均时间误差: %v\n", metrics.AvgTimeError)
		fmt.Printf("  最大时间误差: %v\n", metrics.MaxTimeError)
	}

	// 列出待执行的批量任务
	pendingBatches, err := client.ListBatchTasks("pending", 10, 0)
	if err != nil {
		log.Printf("获取待执行任务失败: %v", err)
		return
	}

	fmt.Printf("待执行的批量任务 (%d个):\n", len(pendingBatches))
	for _, batch := range pendingBatches {
		fmt.Printf("  ID: %s, 任务数: %d, 队列: %s, 创建时间: %v\n",
			batch.ID, batch.TotalTasks, batch.Queue,
			time.Unix(0, batch.CreatedAt))
	}
}

// 错误处理和恢复示例
func errorHandlingExample(client *asynq.BatchClient) {
	fmt.Println("\n=== 错误处理和恢复示例 ===")

	// 恢复未完成的批量任务
	err := client.RecoverPendingBatches()
	if err != nil {
		log.Printf("恢复失败: %v", err)
	} else {
		fmt.Println("成功恢复未完成的批量任务")
	}

	// 列出失败的批量任务
	failedBatches, err := client.ListBatchTasks("failed", 5, 0)
	if err != nil {
		log.Printf("获取失败任务失败: %v", err)
		return
	}

	fmt.Printf("失败的批量任务 (%d个):\n", len(failedBatches))
	for _, batch := range failedBatches {
		fmt.Printf("  ID: %s, 错误: %s, 重试次数: %d\n",
			batch.ID, batch.ErrorMsg, batch.RetryCount)
	}
}

// 监控批量任务状态
func monitorBatch(client *asynq.BatchClient, batchID string) {
	fmt.Printf("监控批量任务 %s...\n", batchID)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(10 * time.Second)
	lastStatus := ""

	for {
		select {
		case <-timeout:
			fmt.Println("监控超时")
			return
		case <-ticker.C:
			batchInfo, err := client.GetBatchTask(batchID)
			if err != nil {
				log.Printf("获取批量任务信息失败: %v", err)
				return
			}

			if batchInfo.Status != lastStatus {
				fmt.Printf("  状态变更: %s -> %s\n", lastStatus, batchInfo.Status)
				lastStatus = batchInfo.Status

				if batchInfo.Status == "running" {
					fmt.Printf("  进度: %d/%d 已完成\n",
						batchInfo.CompletedCount, batchInfo.TotalTasks)
				}
			}

			if batchInfo.Status == "completed" {
				fmt.Printf("  批量任务完成! 总计: %d, 完成: %d, 失败: %d\n",
					batchInfo.TotalTasks, batchInfo.CompletedCount, batchInfo.FailedCount)
				return
			}

			if batchInfo.Status == "failed" || batchInfo.Status == "cancelled" {
				fmt.Printf("  批量任务结束: %s\n", batchInfo.Status)
				if batchInfo.ErrorMsg != "" {
					fmt.Printf("  错误信息: %s\n", batchInfo.ErrorMsg)
				}
				return
			}
		}
	}
}

// 演示时间精度测试
func timingAccuracyDemo() {
	fmt.Println("\n=== 时间精度演示 ===")

	client := asynq.NewBatchClient(
		asynq.RedisClientOpt{Addr: "localhost:6379"},
		&asynq.BatchTaskConfig{
			MaxBatchSize:    10,
			DefaultInterval: 50 * time.Millisecond, // 50ms间隔
			MaxTimeError:    2 * time.Millisecond,  // 2ms误差阈值
		},
	)
	defer client.Close()

	// 创建高精度测试任务
	tasks := make([]*asynq.Task, 10)
	for i := range tasks {
		tasks[i] = asynq.NewTask("timing_test", []byte(fmt.Sprintf("task_%d", i)))
	}

	startTime := time.Now()
	batch, err := client.BatchScheduleWithPreciseInterval(
		context.Background(),
		tasks,
		50*time.Millisecond,
		startTime.Add(1*time.Second),
	)

	if err != nil {
		log.Fatalf("时间精度测试调度失败: %v", err)
	}

	fmt.Printf("时间精度测试批量任务: ID=%s\n", batch.ID)
	fmt.Printf("预期执行时间间隔: 50ms\n")
	fmt.Printf("时间误差阈值: 2ms\n")

	// 等待执行完成
	time.Sleep(2 * time.Second)

	// 获取执行指标
	metrics := client.GetBatchMetrics()
	if metrics != nil {
		fmt.Printf("实际平均时间误差: %v\n", metrics.AvgTimeError)
		fmt.Printf("最大时间误差: %v\n", metrics.MaxTimeError)
		fmt.Printf("准确率: %.2f%%\n", metrics.AccuracyRate*100)
	}
}

// 并发批量任务演示
func concurrentBatchDemo() {
	fmt.Println("\n=== 并发批量任务演示 ===")

	client := asynq.NewBatchClient(
		asynq.RedisClientOpt{Addr: "localhost:6379"},
		nil, // 使用默认配置
	)
	defer client.Close()

	// 并发创建多个批量任务
	batchCount := 5
	resultChan := make(chan string, batchCount)

	for i := 0; i < batchCount; i++ {
		go func(index int) {
			tasks := []*asynq.Task{
				asynq.NewTask("concurrent_task", []byte(fmt.Sprintf("batch_%d_task_1", index))),
				asynq.NewTask("concurrent_task", []byte(fmt.Sprintf("batch_%d_task_2", index))),
				asynq.NewTask("concurrent_task", []byte(fmt.Sprintf("batch_%d_task_3", index))),
			}

			batch, err := client.BatchScheduleWithPreciseInterval(
				context.Background(),
				tasks,
				100*time.Millisecond,
				time.Now().Add(time.Duration(index+1)*500*time.Millisecond),
				asynq.BatchQueue(fmt.Sprintf("concurrent_queue_%d", index)),
			)

			if err != nil {
				resultChan <- fmt.Sprintf("批次 %d 失败: %v", index, err)
			} else {
				resultChan <- fmt.Sprintf("批次 %d 成功: %s", index, batch.ID)
			}
		}(i)
	}

	// 收集结果
	for i := 0; i < batchCount; i++ {
		result := <-resultChan
		fmt.Printf("  %s\n", result)
	}

	fmt.Printf("并发创建了 %d 个批量任务\n", batchCount)
}
