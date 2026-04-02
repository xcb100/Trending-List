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

	rdb.FlushDB(ctx)

	log.Println("=== Scenario E: Complex Recompute (Scheduled) ===")
	lbID := "recompute_lb"
	schema := map[string]interface{}{"score_base": 0.0}
	expr := "score_base * 5.5 + 10"

	lb, err := core.CreateLeaderboard(ctx, lbID, expr, schema, core.RefreshPolicyScheduled, "@every 1m", repo)
	if err != nil {
		log.Fatalf("Failed to create leaderboard: %v", err)
	}

	totalItems := 100000
	workers := 100
	itemsPerWorker := totalItems / workers

	log.Printf("Writing %d dirty items...", totalItems)
	var wg sync.WaitGroup
	startW := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				itemID := fmt.Sprintf("u_%d_%d", workerID, j)
				lb.UpsertItem(ctx, itemID, map[string]interface{}{"score_base": rand.Float64() * 1000})
			}
		}(i)
	}
	wg.Wait()
	log.Printf("Written %d dirty items in %v.", totalItems, time.Since(startW))

	log.Println("Triggering recompute...")
	startR := time.Now()
	if err := lb.Recompute(ctx); err != nil {
		log.Fatalf("Recompute failed: %v", err)
	}
	durR := time.Since(startR)
	rpsR := float64(totalItems) / durR.Seconds()
	log.Printf("Recompute finished. Duration: %v, Objects/sec: %.2f", durR, rpsR)

	top := lb.GetTopN(ctx, 100)
	topScore := 0.0
	if len(top) > 0 {
		topScore = top[0].Score
	}

	fmt.Printf("RESULTS_E|%d|%.2f|%.2f\n", totalItems, rpsR, topScore)
}
