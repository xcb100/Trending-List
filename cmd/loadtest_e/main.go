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
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis not running: %v", err)
	}

	repo := core.NewRedisRepository(rdb)
	core.SetDefaultRepo(repo)
	_ = rdb.FlushDB(ctx).Err()

	log.Println("=== Scenario E: Complex Recompute (Scheduled) ===")

	lb, err := core.CreateLeaderboard(
		ctx,
		"recompute_lb",
		"score_base * 5.5 + 10",
		map[string]interface{}{"score_base": 0.0},
		core.RefreshPolicyScheduled,
		"@every 1m",
		repo,
	)
	if err != nil {
		log.Fatalf("create leaderboard: %v", err)
	}

	totalItems := 100000
	workers := 100
	itemsPerWorker := totalItems / workers

	log.Printf("writing %d dirty items...", totalItems)
	var wg sync.WaitGroup
	startWrite := time.Now()
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
	log.Printf("write finished in %v", time.Since(startWrite))

	log.Println("triggering recompute...")
	startRecompute := time.Now()
	if err := lb.Recompute(ctx); err != nil {
		log.Fatalf("recompute failed: %v", err)
	}
	duration := time.Since(startRecompute)
	rps := float64(totalItems) / duration.Seconds()
	log.Printf("recompute finished in %v, throughput %.2f items/s", duration, rps)

	top, err := lb.GetTopN(ctx, 100)
	if err != nil {
		log.Fatalf("read topN failed: %v", err)
	}
	topScore := 0.0
	if len(top) > 0 {
		topScore = top[0].Score
	}

	fmt.Printf("RESULTS_E|%d|%.2f|%.2f\n", totalItems, rps, topScore)
}
