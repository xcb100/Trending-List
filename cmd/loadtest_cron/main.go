package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"trendingList/internal/core"

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

	log.Println("=== Automated Cron Scheduler Load Test ===")

	lb, err := core.CreateLeaderboard(
		ctx,
		"cron_lb_100k",
		"score_base * 5.5 + 10",
		map[string]interface{}{"score_base": 0.0},
		core.RefreshPolicyScheduled,
		"*/5 * * * * *",
		repo,
	)
	if err != nil {
		log.Fatalf("create leaderboard: %v", err)
	}

	totalItems := 100000
	workers := 100
	itemsPerWorker := totalItems / workers

	log.Printf("writing %d scheduled items...", totalItems)
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

	dirtyIDs, _ := repo.GetDirtyItemIDs(ctx, lb.ID)
	log.Printf("dirty item count after writes: %d", len(dirtyIDs))
	if len(dirtyIDs) == 0 {
		log.Fatal("expected dirty items after scheduled writes")
	}

	cronCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go core.StartCronScheduler(cronCtx)

	timeout := time.After(20 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var processStart time.Time
	for {
		select {
		case <-timeout:
			log.Fatalf("scheduler did not finish in time for %d items", totalItems)
		case <-ticker.C:
			currentDirty, _ := repo.GetDirtyItemIDs(ctx, lb.ID)
			if len(currentDirty) < totalItems && processStart.IsZero() {
				processStart = time.Now()
				log.Println("scheduler started consuming dirty items")
			}
			if len(currentDirty) != 0 {
				continue
			}

			duration := time.Since(processStart)
			if duration <= 0 {
				duration = 500 * time.Millisecond
			}
			rps := float64(totalItems) / duration.Seconds()
			log.Printf("scheduler finished in %v, estimated throughput %.2f items/s", duration, rps)

			top, err := lb.GetTopN(ctx, 10)
			if err != nil {
				log.Fatalf("read topN failed: %v", err)
			}
			if len(top) > 0 {
				log.Printf("top item score: %.2f", top[0].Score)
			}
			return
		}
	}
}
