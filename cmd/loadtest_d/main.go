package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"awesomeProject/internal/core"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()

	// 检查 Redis 连通性
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis not running: %v", err)
	}

	repo := core.NewRedisRepository(rdb)
	core.SetDefaultRepo(repo)

	rdb.FlushDB(ctx)

	// D: 多榜冷起击穿测试
	log.Println("=== Scenario D: Stampede Protection ===")
	lbID := "stampede_lb"
	schema := map[string]interface{}{"score_base": 0.0}
	expr := "score_base * 1.5"

	// 1. 写 metadata 到 redis
	_, err := core.CreateLeaderboard(ctx, lbID, expr, schema, core.RefreshPolicyRealtime, "", repo)
	if err != nil {
		log.Fatalf("Failed to create leaderboard: %v", err)
	}

	// 2. 清理本地内存，模拟冷起
	core.Leaderboards = make(map[string]*core.Leaderboard)

	// 3. 并发 1000 获取
	workers := 1000
	var wg sync.WaitGroup
	var successCount int32
	var failCount int32

	startD := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := core.GetLeaderboard(ctx, lbID)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else {
				atomic.AddInt32(&failCount, 1)
			}
		}()
	}
	wg.Wait()
	durD := time.Since(startD)

	log.Printf("Scenario D finished. Workers: %d, Success: %d, Fail: %d, Duration: %v, RPS: %.2f",
		workers, successCount, failCount, durD, float64(workers)/durD.Seconds())
	fmt.Printf("RESULTS_D|%.2f|%v\n", float64(workers)/durD.Seconds(), failCount == 0)
}
