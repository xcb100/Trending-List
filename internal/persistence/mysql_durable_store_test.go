package persistence

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"trendingList/internal/core"
)

func TestAggregateItemMutationsKeepsLastEventPerItem(t *testing.T) {
	firstScore := 5.0
	secondScore := 9.0

	firstPayload, err := json.Marshal(core.DurableItemPayload{
		ItemID:    "item-1",
		Data:      map[string]interface{}{"score_base": 2.5},
		Score:     &firstScore,
		UpdatedAt: time.Unix(10, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("marshal first payload: %v", err)
	}

	secondPayload, err := json.Marshal(core.DurableItemPayload{
		ItemID:    "item-1",
		Data:      map[string]interface{}{"score_base": 4.5},
		Score:     &secondScore,
		UpdatedAt: time.Unix(20, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("marshal second payload: %v", err)
	}

	mutations, err := aggregateItemMutations([]durableEventRow{
		{
			ID:            1,
			Operation:     core.DurableOpItemUpsert,
			LeaderboardID: "lb-1",
			ItemID:        nullableString("item-1"),
			Payload:       firstPayload,
			CreatedAt:     time.Unix(10, 0).UTC(),
		},
		{
			ID:            2,
			Operation:     core.DurableOpItemUpsert,
			LeaderboardID: "lb-1",
			ItemID:        nullableString("item-1"),
			Payload:       secondPayload,
			CreatedAt:     time.Unix(20, 0).UTC(),
		},
		{
			ID:            3,
			Operation:     core.DurableOpItemDelete,
			LeaderboardID: "lb-1",
			ItemID:        nullableString("item-1"),
			Payload:       []byte(`{}`),
			CreatedAt:     time.Unix(30, 0).UTC(),
		},
	})
	if err != nil {
		t.Fatalf("aggregate mutations: %v", err)
	}

	mutation, ok := mutations[itemMutationKey{leaderboardID: "lb-1", itemID: "item-1"}]
	if !ok {
		t.Fatal("expected final mutation for item-1 to exist")
	}
	if !mutation.IsDeleted {
		t.Fatalf("expected last event to win and mark item deleted, got %+v", mutation)
	}
	if mutation.LastEventID != 3 {
		t.Fatalf("expected last event id 3, got %d", mutation.LastEventID)
	}
}

func nullableString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: true}
}
