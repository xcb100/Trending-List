package core_test

import (
	"context"
	"encoding/json"
	"testing"

	"trendingList/internal/core"
)

type recordingDurableStore struct {
	events []core.DurableEvent
}

func (r *recordingDurableStore) AppendEvent(ctx context.Context, event core.DurableEvent) error {
	r.events = append(r.events, event)
	return nil
}

func TestDurableEventsAreRecordedForMutations(t *testing.T) {
	repo := newMockRepo()
	store := &recordingDurableStore{}
	core.SetDurableStore(store)
	defer core.SetDurableStore(nil)

	ctx := context.Background()
	schema := map[string]interface{}{"score_base": 0.0}

	lb, err := core.CreateLeaderboard(ctx, "durable_lb", "score_base * 2", schema, core.RefreshPolicyRealtime, "", repo)
	if err != nil {
		t.Fatalf("unexpected error creating leaderboard: %v", err)
	}

	if _, err := lb.UpsertItem(ctx, "item-1", map[string]interface{}{"score_base": 5.0}); err != nil {
		t.Fatalf("unexpected error upserting realtime item: %v", err)
	}
	if err := core.UpdateLeaderboardSchedule(ctx, lb, "@every 15s"); err != nil {
		t.Fatalf("unexpected error updating schedule: %v", err)
	}
	if err := lb.DeleteItem(ctx, "item-1"); err != nil {
		t.Fatalf("unexpected error deleting item: %v", err)
	}
	if err := core.DeleteLeaderboard(ctx, "durable_lb"); err != nil {
		t.Fatalf("unexpected error deleting leaderboard: %v", err)
	}

	if len(store.events) != 5 {
		t.Fatalf("expected 5 durable events, got %d", len(store.events))
	}

	if store.events[0].Operation != core.DurableOpLeaderboardUpsert {
		t.Fatalf("expected first event to be leaderboard upsert, got %s", store.events[0].Operation)
	}
	if store.events[1].Operation != core.DurableOpItemUpsert {
		t.Fatalf("expected second event to be item upsert, got %s", store.events[1].Operation)
	}
	if store.events[2].Operation != core.DurableOpLeaderboardUpsert {
		t.Fatalf("expected third event to be leaderboard upsert, got %s", store.events[2].Operation)
	}
	if store.events[3].Operation != core.DurableOpItemDelete {
		t.Fatalf("expected fourth event to be item delete, got %s", store.events[3].Operation)
	}
	if store.events[4].Operation != core.DurableOpLeaderboardDelete {
		t.Fatalf("expected fifth event to be leaderboard delete, got %s", store.events[4].Operation)
	}

	var leaderboardPayload core.DurableLeaderboardPayload
	if err := json.Unmarshal(store.events[2].Payload, &leaderboardPayload); err != nil {
		t.Fatalf("unexpected error decoding schedule payload: %v", err)
	}
	if leaderboardPayload.CronSpec != "@every 15s" || leaderboardPayload.RefreshPolicy != core.RefreshPolicyScheduled {
		t.Fatalf("expected updated schedule payload, got %+v", leaderboardPayload)
	}

	var itemPayload core.DurableItemPayload
	if err := json.Unmarshal(store.events[1].Payload, &itemPayload); err != nil {
		t.Fatalf("unexpected error decoding item payload: %v", err)
	}
	if itemPayload.Score == nil || *itemPayload.Score != 10 {
		t.Fatalf("expected realtime durable item payload to include computed score, got %+v", itemPayload)
	}
}
