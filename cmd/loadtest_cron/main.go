package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"awesomeProject/internal/core"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis not running: %v", err)
	}

	repo := core.NewRedisRepository(rdb)
	core.SetDefaultRepo(repo)

	// 环境清理
	rdb.FlushDB(ctx)

	log.Println("=== 自动化内部定时任务压测 (Automated Cron Scheduler Load Test) ===")

	lbID := "cron_lb_100k"
	schema := map[string]interface{}{"score_base": 0.0}

	// 设置为每 5 秒自动重算一次 (将落入 Tier5s 分级队列)
	cronSpec := "*/5 * * * * *"
	expr := "score_base * 5.5 + 10"

	lb, err := core.CreateLeaderboard(ctx, lbID, expr, schema, core.RefreshPolicyScheduled, cronSpec, repo)
	if err != nil {
		log.Fatalf("Failed to create leaderboard: %v", err)
	}

	totalItems := 100000
	workers := 100
	itemsPerWorker := totalItems / workers

	log.Printf("1. 正在大规模生成 %d 条设为 Schedule 延迟执行的排名项...", totalItems)
	var wg sync.WaitGroup
	startW := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				itemID := fmt.Sprintf("u_%d_%d", workerID, j)
				_, _ = lb.UpsertItem(ctx, itemID, map[string]interface{}{"score_base": rand.Float64() * 1000})
			}
		}(i)
	}
	wg.Wait()
	log.Printf(">> 写入完毕. 耗时: %v", time.Since(startW))

	dirtyIDs, _ := repo.GetDirtyItemIDs(ctx, lbID)
	log.Printf("2. 验证写入后积压的脏数据数量: %d 条", len(dirtyIDs))

	if len(dirtyIDs) == 0 {
		log.Fatalf("数据并未正确进入 Dirty 队列，请检查逻辑！")
	}

	log.Println("3. 启动后台分布式定时调度器 (StartCronScheduler) 并进行观测...")

	cronCtx, cronCancel := context.WithCancel(context.Background())
	defer cronCancel()

	// 在后台拉起我们的 4 级队列心跳系统
	go core.StartCronScheduler(cronCtx)

	// 轮询观测 Redis 脏数据队列的情况
	timeout := time.After(20 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	processStart := time.Time{}

	for {
		select {
		case <-timeout:
			log.Fatalf("超时啦！20秒内调度器未能消化完 %d 条脏任务。", totalItems)
		case <-ticker.C:
			currentDirty, _ := repo.GetDirtyItemIDs(ctx, lbID)
			count := len(currentDirty)

			if count < totalItems && processStart.IsZero() {
				processStart = time.Now()
				log.Printf(">> [观测点] 调度器 Cron 滴答已触发，开始吞吐消化批处理...")
			}

			if count == 0 {
				dur := time.Since(processStart)
				if dur.Seconds() == 0 {
					dur = 500 * time.Millisecond // 避免除 0，至少代表是半秒轮询的极速
				}
				rps := float64(totalItems) / dur.Seconds()
				log.Printf(">> [观测点] 调度器清空队列完毕！数据全部重算结账。")
				log.Printf("=============================================")
				log.Printf("清算耗时: %v, Cron 批处理估算速度: > %.2f 次/秒", dur, rps)

				top := lb.GetTopN(ctx, 10)
				if len(top) > 0 {
					log.Printf("最高排位分数示例: %.2f", top[0].Score)
				}
				return
			}
		}
	}
}
