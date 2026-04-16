package core_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"trendingList/internal/core"
)

// mockRepo 是一个内存版仓储，用于核心逻辑单元测试。
type mockRepo struct {
	mu       sync.RWMutex
	metadata map[string]map[string]string
	items    map[string]map[string]*core.Item
	dirty    map[string]map[string]bool
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		metadata: make(map[string]map[string]string),
		items:    make(map[string]map[string]*core.Item),
		dirty:    make(map[string]map[string]bool),
	}
}

func (m *mockRepo) GetMetadata(ctx context.Context, lbID string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if meta, ok := m.metadata[lbID]; ok {
		// 返回副本，避免测试里共享底层 map。
		res := make(map[string]string, len(meta))
		for k, v := range meta {
			res[k] = v
		}
		return res, nil
	}
	return nil, nil
}

func (m *mockRepo) SaveMetadata(ctx context.Context, lbID string, metadata map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metadata[lbID] = metadata
	return nil
}

func (m *mockRepo) DeleteLeaderboard(ctx context.Context, lbID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.metadata, lbID)
	delete(m.items, lbID)
	delete(m.dirty, lbID)
	return nil
}

func (m *mockRepo) SaveItemData(ctx context.Context, lbID string, itemID string, data map[string]interface{}, updatedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.items[lbID] == nil {
		m.items[lbID] = make(map[string]*core.Item)
	}
	if item, ok := m.items[lbID][itemID]; ok {
		item.Data = data
		item.UpdatedAt = updatedAt
	} else {
		m.items[lbID][itemID] = &core.Item{
			ID:        itemID,
			Data:      data,
			Score:     0,
			UpdatedAt: updatedAt,
		}
	}
	return nil
}

func (m *mockRepo) UpdateItemScore(ctx context.Context, lbID string, itemID string, score float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.items[lbID] != nil && m.items[lbID][itemID] != nil {
		m.items[lbID][itemID].Score = score
	}
	return nil
}

func (m *mockRepo) UpsertItem(ctx context.Context, lbID string, itemID string, score float64, data map[string]interface{}, updatedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.items[lbID] == nil {
		m.items[lbID] = make(map[string]*core.Item)
	}
	m.items[lbID][itemID] = &core.Item{
		ID:        itemID,
		Data:      data,
		Score:     score,
		UpdatedAt: updatedAt,
	}
	if m.dirty[lbID] != nil {
		delete(m.dirty[lbID], itemID)
	}
	return nil
}

func (m *mockRepo) DeleteItem(ctx context.Context, lbID string, itemID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.items[lbID] != nil {
		delete(m.items[lbID], itemID)
	}
	if m.dirty[lbID] != nil {
		delete(m.dirty[lbID], itemID)
	}
	return nil
}

func (m *mockRepo) MarkItemDirty(ctx context.Context, lbID string, itemID string, dirty bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dirty[lbID] == nil {
		m.dirty[lbID] = make(map[string]bool)
	}
	if dirty {
		m.dirty[lbID][itemID] = true
	} else {
		delete(m.dirty[lbID], itemID)
	}
	return nil
}

func (m *mockRepo) PruneItems(ctx context.Context, lbID string, itemIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dirty[lbID] != nil {
		for _, id := range itemIDs {
			delete(m.dirty[lbID], id)
		}
	}
	if m.items[lbID] != nil {
		for _, id := range itemIDs {
			if item := m.items[lbID][id]; item != nil {
				item.Score = 0
			}
		}
	}
	return nil
}

func (m *mockRepo) GetDirtyItemIDs(ctx context.Context, lbID string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ids []string
	for id := range m.dirty[lbID] {
		ids = append(ids, id)
	}
	return ids, nil
}

// 确保 mockRepo 完整实现更新后的 Repository 接口。
func (m *mockRepo) AddScheduledLeaderboard(ctx context.Context, lbID string, tier string) error {
	return nil
}
func (m *mockRepo) RemoveScheduledLeaderboard(ctx context.Context, lbID string) error { return nil }
func (m *mockRepo) GetScheduledLeaderboardIDs(ctx context.Context, tier string) ([]string, error) {
	return nil, nil
}
func (m *mockRepo) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return true, nil
}
func (m *mockRepo) ReleaseLock(ctx context.Context, key string) error          { return nil }
func (m *mockRepo) GetAllLeaderboardIDs(ctx context.Context) ([]string, error) { return nil, nil }

func (m *mockRepo) ScanDirtyItemIDs(ctx context.Context, lbID string, cursor uint64, count int64) ([]string, uint64, error) {
	if cursor != 0 {
		return nil, 0, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ids []string
	for id := range m.dirty[lbID] {
		ids = append(ids, id)
	}
	return ids, 0, nil
}

func (m *mockRepo) CommitRecomputedScores(ctx context.Context, lbID string, scores map[string]float64, updatedAts map[string]time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, sc := range scores {
		if m.items[lbID] != nil && m.items[lbID][id] != nil {
			if m.items[lbID][id].UpdatedAt.Equal(updatedAts[id]) {
				m.items[lbID][id].Score = sc
				if m.dirty[lbID] != nil {
					delete(m.dirty[lbID], id)
				}
			}
		} else {
			if m.dirty[lbID] != nil {
				delete(m.dirty[lbID], id)
			}
		}
	}
	return nil
}

func (m *mockRepo) GetItems(ctx context.Context, lbID string, itemIDs []string) ([]*core.Item, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []*core.Item
	for _, id := range itemIDs {
		if item, ok := m.items[lbID][id]; ok {
			res = append(res, &core.Item{
				ID:        item.ID,
				Data:      item.Data, // 测试场景下这里直接复用即可。
				Score:     item.Score,
				UpdatedAt: item.UpdatedAt,
			})
		}
	}
	return res, nil
}

func (m *mockRepo) GetTopN(ctx context.Context, lbID string, n int) ([]*core.Item, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var all []*core.Item
	for _, item := range m.items[lbID] {
		all = append(all, item)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Score > all[j].Score // 按分数降序排列。
	})

	if len(all) > n {
		return all[:n], nil
	}
	return all, nil
}

func (m *mockRepo) GetItem(ctx context.Context, lbID string, itemID string) (*core.Item, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if item, ok := m.items[lbID][itemID]; ok {
		return item, nil
	}
	return nil, nil
}

func TestLeaderboard_RealtimePolicy(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()

	schema := map[string]interface{}{"views": 0.0, "likes": 0.0}
	expr := "views * 0.5 + likes * 2.0"

	lb, err := core.CreateLeaderboard(ctx, "test_realtime", expr, schema, core.RefreshPolicyRealtime, "", repo)
	if err != nil {
		t.Fatalf("unexpected error creating leaderboard: %v", err)
	}

	// 写入条目
	item, err := lb.UpsertItem(ctx, "item1", map[string]interface{}{"views": 100.0, "likes": 10.0})
	if err != nil {
		t.Fatalf("unexpected error upserting item: %v", err)
	}

	expectedScore := 100.0*0.5 + 10.0*2.0
	if item.Score != expectedScore {
		t.Errorf("expected score %f, got %f", expectedScore, item.Score)
	}

	top, err := lb.GetTopN(ctx, 1)
	if err != nil {
		t.Fatalf("unexpected error reading topN: %v", err)
	}
	if len(top) != 1 || top[0].ID != "item1" || top[0].Score != expectedScore {
		t.Errorf("expected item1 with score %f, got %+v", expectedScore, top)
	}
}

func TestLeaderboard_ScheduledPolicy(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()

	schema := map[string]interface{}{"score_base": 0.0}
	expr := "score_base * 10.0"

	lb, err := core.CreateLeaderboard(ctx, "test_cron", expr, schema, core.RefreshPolicyScheduled, "@every 10s", repo)
	if err != nil {
		t.Fatalf("unexpected error creating leaderboard: %v", err)
	}

	// 写入条目
	item, err := lb.UpsertItem(ctx, "item1", map[string]interface{}{"score_base": 5.0})
	if err != nil {
		t.Fatalf("unexpected error upserting item: %v", err)
	}

	if item.Score != 0.0 {
		t.Errorf("expected immediate score 0 for scheduled lb, got %f", item.Score)
	}

	dirtyIDs, _ := repo.GetDirtyItemIDs(ctx, "test_cron")
	if len(dirtyIDs) != 1 || dirtyIDs[0] != "item1" {
		t.Errorf("expected item1 to be dirty, got %v", dirtyIDs)
	}

	// 执行重算
	if err := lb.Recompute(ctx); err != nil {
		t.Fatalf("unexpected error recomputing: %v", err)
	}

	top, err := lb.GetTopN(ctx, 1)
	if err != nil {
		t.Fatalf("unexpected error reading topN: %v", err)
	}
	if len(top) != 1 || top[0].ID != "item1" {
		t.Fatalf("expected item1 in top N after recompute")
	}

	if top[0].Score != 50.0 {
		t.Errorf("expected recomputed score 50, got %f", top[0].Score)
	}

	dirtyIDsAfter, _ := repo.GetDirtyItemIDs(ctx, "test_cron")
	if len(dirtyIDsAfter) != 0 {
		t.Errorf("expected dirty items to be empty, got %v", dirtyIDsAfter)
	}
}

func TestLeaderboard_GetLeaderboard_ErrorsAndSingleflight(t *testing.T) {
	repo := newMockRepo()
	core.SetDefaultRepo(repo)
	ctx := context.Background()

	// 1. 验证 ErrLeaderboardNotFound
	_, err := core.GetLeaderboard(ctx, "not_exist_lb")
	if !errors.Is(err, core.ErrLeaderboardNotFound) {
		t.Errorf("expected ErrLeaderboardNotFound, got: %v", err)
	}

	// 2. 验证 ErrRestoreFailed（坏表达式）
	badMeta := map[string]string{
		"expression": "invalid +++ syntax",
		"schema":     "{}",
	}
	repo.SaveMetadata(ctx, "bad_lb", badMeta)
	_, err = core.GetLeaderboard(ctx, "bad_lb")
	if !errors.Is(err, core.ErrRestoreFailed) {
		t.Errorf("expected ErrRestoreFailed for bad expression, got: %v", err)
	}

	// 3. 验证并发场景下的 singleflight 恢复
	goodMeta := map[string]string{
		"expression":     "score_base * 2.0",
		"schema":         `{"score_base": 0.0}`,
		"refresh_policy": "realtime",
	}
	repo.SaveMetadata(ctx, "concurrent_lb", goodMeta)

	var wg sync.WaitGroup
	var successCount int
	var mu sync.Mutex

	workers := 50
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			lb, err := core.GetLeaderboard(ctx, "concurrent_lb")
			if err == nil && lb != nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if successCount != workers {
		t.Errorf("expected all %d concurrent workers to get the restored leaderboard successfully, got %d", workers, successCount)
	}

	// 清理内存状态，避免影响后续测试
	core.SetDefaultRepo(nil)
}
