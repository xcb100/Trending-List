package core_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"awesomeProject/internal/core"

	"github.com/redis/go-redis/v9"
)

type rankedItem struct {
	ID    string
	Score float64
}

func TestUltimateRedisFlow(t *testing.T) {
	ctx := context.Background()
	client := newTestRedisClient()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis is unavailable: %v", err)
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis before test: %v", err)
	}
	t.Cleanup(func() {
		_ = client.FlushDB(context.Background()).Err()
		_ = client.Close()
		core.SetDefaultRepo(nil)
	})

	repo := core.NewRedisRepository(client)
	core.SetDefaultRepo(repo)

	suffix := time.Now().UnixNano()
	realtimeID := fmt.Sprintf("ultimate_realtime_%d", suffix)
	scheduledID := fmt.Sprintf("ultimate_scheduled_%d", suffix)
	switchID := fmt.Sprintf("ultimate_switch_%d", suffix)
	badID := fmt.Sprintf("ultimate_bad_%d", suffix)

	if _, err := core.CreateLeaderboard(
		ctx,
		badID,
		"views + likes",
		map[string]interface{}{"views": 0.0, "likes": 0.0},
		core.RefreshPolicyScheduled,
		"not-a-cron",
		repo,
	); err == nil {
		t.Fatal("expected invalid cron spec to be rejected")
	}

	realtimeLB, err := core.CreateLeaderboard(
		ctx,
		realtimeID,
		"views*2 + likes*7 + bonus",
		map[string]interface{}{"views": 0.0, "likes": 0.0, "bonus": 0.0},
		core.RefreshPolicyRealtime,
		"",
		repo,
	)
	if err != nil {
		t.Fatalf("create realtime leaderboard: %v", err)
	}
	if _, err := core.CreateLeaderboard(
		ctx,
		realtimeID,
		"views+1",
		map[string]interface{}{"views": 0.0},
		core.RefreshPolicyRealtime,
		"",
		repo,
	); !errors.Is(err, core.ErrLeaderboardExists) {
		t.Fatalf("expected duplicate create to fail with ErrLeaderboardExists, got %v", err)
	}

	realtimeExpected := make(map[string]float64, 1500)
	var realtimeMu sync.Mutex
	if err := runParallel(12, 1500, func(i int) error {
		data := map[string]interface{}{
			"views": float64(i * 3),
			"likes": float64((i * 7) % 17),
			"bonus": float64(i % 5),
		}
		item, err := realtimeLB.UpsertItem(ctx, fmt.Sprintf("rt_%04d", i), data)
		if err != nil {
			return err
		}
		realtimeMu.Lock()
		realtimeExpected[item.ID] = item.Score
		realtimeMu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("populate realtime leaderboard: %v", err)
	}
	topRealtime, err := realtimeLB.GetTopN(ctx, 10)
	if err != nil {
		t.Fatalf("read realtime topN: %v", err)
	}
	assertTopNMatches(t, topRealtime, topNExpected(rankedItemsFromMap(realtimeExpected), 10))

	if err := core.UpdateLeaderboardExpression(ctx, realtimeLB, "views*3 + likes*2 + bonus", nil); err != nil {
		t.Fatalf("update realtime expression: %v", err)
	}
	for id, score := range realtimeExpected {
		index := parseIndex(id)
		realtimeExpected[id] = float64(index*3)*3 + float64((index*7)%17)*2 + float64(index%5)
		_ = score
	}
	topRealtime, err = realtimeLB.GetTopN(ctx, 10)
	if err != nil {
		t.Fatalf("read realtime topN after expression update: %v", err)
	}
	assertTopNMatches(t, topRealtime, topNExpected(rankedItemsFromMap(realtimeExpected), 10))

	scheduledLB, err := core.CreateLeaderboard(
		ctx,
		scheduledID,
		"views*0.25 + likes*5 + shares*9",
		map[string]interface{}{"views": 0.0, "likes": 0.0, "shares": 0.0},
		core.RefreshPolicyScheduled,
		"@every 1s",
		repo,
	)
	if err != nil {
		t.Fatalf("create scheduled leaderboard: %v", err)
	}

	scheduledExpected := make(map[string]float64, 2200)
	var scheduledMu sync.Mutex
	if err := runParallel(16, 2200, func(i int) error {
		data := map[string]interface{}{
			"views":  float64(i * 4),
			"likes":  float64((i * 11) % 23),
			"shares": float64((i * 13) % 29),
		}
		itemID := fmt.Sprintf("sch_%04d", i)
		if _, err := scheduledLB.UpsertItem(ctx, itemID, data); err != nil {
			return err
		}
		score := data["views"].(float64)*0.25 + data["likes"].(float64)*5 + data["shares"].(float64)*9
		scheduledMu.Lock()
		scheduledExpected[itemID] = score
		scheduledMu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("populate scheduled leaderboard: %v", err)
	}

	dirtyKey := fmt.Sprintf("lb:%s:dirty_items", scheduledID)
	itemsKey := fmt.Sprintf("lb:%s:items", scheduledID)
	scoresKey := fmt.Sprintf("lb:%s:scores", scheduledID)

	if err := client.SAdd(ctx, dirtyKey, "missing_item", "corrupt_item").Err(); err != nil {
		t.Fatalf("inject dirty markers: %v", err)
	}
	if err := client.HSet(ctx, itemsKey, "corrupt_item", "{broken-json").Err(); err != nil {
		t.Fatalf("inject corrupt payload: %v", err)
	}
	if err := client.ZAdd(ctx, scoresKey,
		redis.Z{Member: "missing_item", Score: 999999},
		redis.Z{Member: "corrupt_item", Score: 888888},
	).Err(); err != nil {
		t.Fatalf("inject stale scores: %v", err)
	}

	dirtyBefore, err := repo.GetDirtyItemIDs(ctx, scheduledID)
	if err != nil {
		t.Fatalf("get dirty items before scheduler tick: %v", err)
	}
	if len(dirtyBefore) != 2202 {
		t.Fatalf("expected 2202 dirty items before scheduler tick, got %d", len(dirtyBefore))
	}

	if err := core.ProcessCronTick(ctx, core.DetermineTier("@every 1s")); err != nil {
		t.Fatalf("process cron tick: %v", err)
	}

	dirtyAfterTick, err := repo.GetDirtyItemIDs(ctx, scheduledID)
	if err != nil {
		t.Fatalf("get dirty items after scheduler tick: %v", err)
	}
	if len(dirtyAfterTick) != 0 {
		t.Fatalf("expected scheduler tick to clear all dirty items, got %d left", len(dirtyAfterTick))
	}

	if _, err := client.ZScore(ctx, scoresKey, "missing_item").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("expected missing_item score to be pruned, got err=%v", err)
	}
	if _, err := client.ZScore(ctx, scoresKey, "corrupt_item").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("expected corrupt_item score to be pruned, got err=%v", err)
	}

	topScheduled, err := scheduledLB.GetTopN(ctx, 10)
	if err != nil {
		t.Fatalf("read scheduled topN after tick: %v", err)
	}
	assertTopNMatches(t, topScheduled, topNExpected(rankedItemsFromMap(scheduledExpected), 10))
	if scheduledLB.State().LastRecomputedAt.IsZero() {
		t.Fatal("expected scheduled leaderboard to record recompute time")
	}

	for i := 0; i < 200; i++ {
		newData := map[string]interface{}{
			"views":  float64(10000 + i*10),
			"likes":  float64(50 + (i % 10)),
			"shares": float64(80 + (i % 7)),
		}
		itemID := fmt.Sprintf("sch_%04d", i)
		if _, err := scheduledLB.UpsertItem(ctx, itemID, newData); err != nil {
			t.Fatalf("update scheduled item %s: %v", itemID, err)
		}
		score := newData["views"].(float64)*0.25 + newData["likes"].(float64)*5 + newData["shares"].(float64)*9
		scheduledExpected[itemID] = score
	}

	if err := runParallel(6, 6, func(_ int) error {
		err := scheduledLB.Recompute(ctx)
		if errors.Is(err, core.ErrRecomputeInProgress) {
			return nil
		}
		return err
	}); err != nil {
		t.Fatalf("concurrent recompute: %v", err)
	}

	dirtyAfterRecompute, err := repo.GetDirtyItemIDs(ctx, scheduledID)
	if err != nil {
		t.Fatalf("get dirty items after concurrent recompute: %v", err)
	}
	if len(dirtyAfterRecompute) != 0 {
		t.Fatalf("expected no dirty items after concurrent recompute, got %d", len(dirtyAfterRecompute))
	}
	topScheduled, err = scheduledLB.GetTopN(ctx, 10)
	if err != nil {
		t.Fatalf("read scheduled topN after recompute: %v", err)
	}
	assertTopNMatches(t, topScheduled, topNExpected(rankedItemsFromMap(scheduledExpected), 10))

	switchLB, err := core.CreateLeaderboard(
		ctx,
		switchID,
		"views + likes*2",
		map[string]interface{}{"views": 0.0, "likes": 0.0},
		core.RefreshPolicyRealtime,
		"",
		repo,
	)
	if err != nil {
		t.Fatalf("create switch leaderboard: %v", err)
	}

	if err := core.UpdateLeaderboardSchedule(ctx, switchLB, "bad cron"); err == nil {
		t.Fatal("expected invalid schedule update to fail")
	}
	if switchLB.State().RefreshPolicy != core.RefreshPolicyRealtime {
		t.Fatalf("expected failed schedule update to keep realtime policy, got %s", switchLB.State().RefreshPolicy)
	}

	if err := core.UpdateLeaderboardSchedule(ctx, switchLB, "@every 2s"); err != nil {
		t.Fatalf("update schedule successfully: %v", err)
	}
	meta, err := repo.GetMetadata(ctx, switchID)
	if err != nil {
		t.Fatalf("get switch leaderboard metadata: %v", err)
	}
	if meta["schema"] == "" || meta["refresh_policy"] != core.RefreshPolicyScheduled || meta["cron_spec"] != "@every 2s" {
		t.Fatalf("unexpected saved metadata after schedule update: %+v", meta)
	}

	if err := scheduledLB.DeleteItem(ctx, "sch_0000"); err != nil {
		t.Fatalf("delete scheduled item: %v", err)
	}
	delete(scheduledExpected, "sch_0000")
	if item, err := repo.GetItem(ctx, scheduledID, "sch_0000"); err != nil || item != nil {
		t.Fatalf("expected deleted item to be absent, item=%v err=%v", item, err)
	}

	if err := core.DeleteLeaderboard(ctx, scheduledID); err != nil {
		t.Fatalf("delete scheduled leaderboard: %v", err)
	}
	if _, err := core.GetLeaderboard(ctx, scheduledID); !errors.Is(err, core.ErrLeaderboardNotFound) {
		t.Fatalf("expected deleted leaderboard lookup to fail, got %v", err)
	}
}

func newTestRedisClient() *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	password := os.Getenv("REDIS_PASSWORD")
	db := 14
	if os.Getenv("REDIS_TEST_DB") == "15" {
		db = 15
	}
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
}

func runParallel(workers int, count int, fn func(i int) error) error {
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for i := range jobs {
			if err := fn(i); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func topNExpected(all []rankedItem, n int) []rankedItem {
	copied := append([]rankedItem(nil), all...)
	sort.Slice(copied, func(i, j int) bool {
		if copied[i].Score == copied[j].Score {
			return copied[i].ID < copied[j].ID
		}
		return copied[i].Score > copied[j].Score
	})
	if len(copied) > n {
		copied = copied[:n]
	}
	return copied
}

func rankedItemsFromMap(scores map[string]float64) []rankedItem {
	items := make([]rankedItem, 0, len(scores))
	for id, score := range scores {
		items = append(items, rankedItem{ID: id, Score: score})
	}
	return items
}

func parseIndex(id string) int {
	var index int
	_, _ = fmt.Sscanf(id[len(id)-4:], "%d", &index)
	return index
}

func assertTopNMatches(t *testing.T, actual []*core.Item, expected []rankedItem) {
	t.Helper()

	if len(actual) != len(expected) {
		t.Fatalf("expected %d top items, got %d", len(expected), len(actual))
	}

	for i := range expected {
		if actual[i].ID != expected[i].ID {
			t.Fatalf("rank %d: expected id %s, got %s", i, expected[i].ID, actual[i].ID)
		}
		if math.Abs(actual[i].Score-expected[i].Score) > 1e-9 {
			t.Fatalf("rank %d: expected score %.6f, got %.6f", i, expected[i].Score, actual[i].Score)
		}
	}
}
