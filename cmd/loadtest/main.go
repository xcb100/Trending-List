package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"trendingList/internal/core"

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

	// 环境清理
	rdb.FlushDB(ctx)

	// 创建榜单
	lbID := "benchmark_lb_100k"
	schema := map[string]interface{}{"score_base": 0.0}
	expr := "score_base * 10.0"

	lb, err := core.CreateLeaderboard(ctx, lbID, expr, schema, core.RefreshPolicyRealtime, "", repo)
	if err != nil {
		log.Fatalf("Failed to create leaderboard: %v", err)
	}

	totalItems := 1000000
	workers := 100
	itemsPerWorker := totalItems / workers

	log.Printf("Starting write test for %d items using %d workers...", totalItems, workers)

	startWrite := time.Now()
	var wg sync.WaitGroup
	var errorCount int32

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				itemID := fmt.Sprintf("user_%d_%d", workerID, j)
				scoreBase := rand.Float64() * 1000

				_, err := lb.UpsertItem(ctx, itemID, map[string]interface{}{"score_base": scoreBase})
				if err != nil {
					atomic.AddInt32(&errorCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	writeDuration := time.Since(startWrite)
	writeRPS := float64(totalItems) / writeDuration.Seconds()

	log.Printf("Write test finished. Duration: %v, RPS: %.2f, Errors: %d", writeDuration, writeRPS, errorCount)

	log.Printf("Starting read test for topN (100 times)...")
	readStart := time.Now()
	var topN []*core.Item
	for i := 0; i < 100; i++ {
		topN, err = lb.GetTopN(ctx, 100)
		if err != nil {
			log.Fatalf("topN read failed: %v", err)
		}
	}
	readDuration := time.Since(readStart)
	readRPS := float64(100) / readDuration.Seconds()

	log.Printf("Read test finished. Duration: %v, RPS: %.2f (for 100 repeated topN queries)", readDuration, readRPS)

	if len(topN) > 0 {
		log.Printf("Top item score: %.2f", topN[0].Score)
	}

	// 输出汇总信息，便于外部采集结果
	fmt.Printf("RESULTS|%d|%.2f|%.2f\n", totalItems, writeRPS, readRPS)
}
