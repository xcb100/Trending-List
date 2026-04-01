package core_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"awesomeProject/internal/core"
)

// mockRepo is an in-memory repository for unit testing core logic.
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
		// return a copy
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

func (m *mockRepo) ClearDirtyItemIDs(ctx context.Context, lbID string, itemIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dirty[lbID] != nil {
		for _, id := range itemIDs {
			delete(m.dirty[lbID], id)
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

func (m *mockRepo) GetItems(ctx context.Context, lbID string, itemIDs []string) ([]*core.Item, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []*core.Item
	for _, id := range itemIDs {
		if item, ok := m.items[lbID][id]; ok {
			res = append(res, &core.Item{
				ID:        item.ID,
				Data:      item.Data, // simplified for test
				Score:     item.Score,
				UpdatedAt: item.UpdatedAt,
			})
		}
	}
	return res, nil
}

func (m *mockRepo) UpdateItemsScores(ctx context.Context, lbID string, scores map[string]float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, sc := range scores {
		if m.items[lbID] != nil && m.items[lbID][id] != nil {
			m.items[lbID][id].Score = sc
		}
	}
	return nil
}

func (m *mockRepo) GetTopN(ctx context.Context, lbID string, n int) ([]*core.Item, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var all []*core.Item
	for _, item := range m.items[lbID] {
		all = append(all, item)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Score > all[j].Score // descending
	})

	if len(all) > n {
		return all[:n], nil
	}
	return all, nil
}

func (m *mockRepo) IterateItems(ctx context.Context, lbID string, callback func(item *core.Item) bool) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, item := range m.items[lbID] {
		if !callback(item) {
			break
		}
	}
	return nil
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

	// Add item
	item, err := lb.UpsertItem(ctx, "item1", map[string]interface{}{"views": 100.0, "likes": 10.0})
	if err != nil {
		t.Fatalf("unexpected error upserting item: %v", err)
	}

	expectedScore := 100.0*0.5 + 10.0*2.0
	if item.Score != expectedScore {
		t.Errorf("expected score %f, got %f", expectedScore, item.Score)
	}

	top := lb.GetTopN(ctx, 1)
	if len(top) != 1 || top[0].ID != "item1" || top[0].Score != expectedScore {
		t.Errorf("expected item1 with score %f, got %+v", expectedScore, top)
	}
}

func TestLeaderboard_ScheduledPolicy(t *testing.T) {
	repo := newMockRepo()
	ctx := context.Background()

	schema := map[string]interface{}{"score_base": 0.0}
	expr := "score_base * 10.0"

	lb, err := core.CreateLeaderboard(ctx, "test_cron", expr, schema, core.RefreshPolicyScheduled, "cron", repo)
	if err != nil {
		t.Fatalf("unexpected error creating leaderboard: %v", err)
	}

	// Add item
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

	// Recompute
	if err := lb.Recompute(ctx); err != nil {
		t.Fatalf("unexpected error recomputing: %v", err)
	}

	top := lb.GetTopN(ctx, 1)
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
