package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const (
	DurableOpLeaderboardUpsert = "leaderboard_upsert"
	DurableOpLeaderboardDelete = "leaderboard_delete"
	DurableOpItemUpsert        = "item_upsert"
	DurableOpItemMutate        = "item_mutate"
	DurableOpItemDelete        = "item_delete"
)

type DurableEvent struct {
	Operation     string
	LeaderboardID string
	ItemID        string
	Payload       json.RawMessage
	OccurredAt    time.Time
}

type DurableLeaderboardPayload struct {
	ID                string                 `json:"id"`
	Expression        string                 `json:"expression"`
	Schema            map[string]interface{} `json:"schema"`
	RefreshPolicy     string                 `json:"refresh_policy"`
	CronSpec          string                 `json:"cron_spec"`
	DefinitionVersion int64                  `json:"definition_version"`
}

type DurableItemPayload struct {
	ItemID    string                 `json:"item_id"`
	Data      map[string]interface{} `json:"data"`
	Score     *float64               `json:"score,omitempty"`
	UpdatedAt time.Time              `json:"updated_at"`
	IsDeleted bool                   `json:"is_deleted"`
}

type DurableItemMutationOp struct {
	Field string  `json:"field"`
	Op    string  `json:"op"`
	Value float64 `json:"value"`
}

type DurableItemMutationPayload struct {
	ItemID            string                  `json:"item_id"`
	Ops               []DurableItemMutationOp `json:"ops"`
	ExpectedUpdatedAt time.Time               `json:"expected_updated_at"`
	Data              map[string]interface{}  `json:"data"`
	Score             *float64                `json:"score,omitempty"`
	UpdatedAt         time.Time               `json:"updated_at"`
}

type DurableStore interface {
	AppendEvent(ctx context.Context, event DurableEvent) error
}

type noopDurableStore struct{}

func (noopDurableStore) AppendEvent(context.Context, DurableEvent) error {
	return nil
}

var DefaultDurableStore DurableStore = noopDurableStore{}

func SetDurableStore(store DurableStore) {
	if store == nil {
		DefaultDurableStore = noopDurableStore{}
		return
	}
	DefaultDurableStore = store
}

func appendDurableEvent(ctx context.Context, event DurableEvent) error {
	if DefaultDurableStore == nil {
		return nil
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now()
	}
	return DefaultDurableStore.AppendEvent(ctx, event)
}

func appendLeaderboardUpsertEvent(ctx context.Context, snapshot leaderboardSnapshot) error {
	payload, err := json.Marshal(DurableLeaderboardPayload{
		ID:                snapshot.ID,
		Expression:        snapshot.Expression,
		Schema:            cloneMap(snapshot.Schema),
		RefreshPolicy:     snapshot.RefreshPolicy,
		CronSpec:          snapshot.CronSpec,
		DefinitionVersion: 1,
	})
	if err != nil {
		return fmt.Errorf("marshal leaderboard durable payload: %w", err)
	}

	return appendDurableEvent(ctx, DurableEvent{
		Operation:     DurableOpLeaderboardUpsert,
		LeaderboardID: snapshot.ID,
		Payload:       payload,
		OccurredAt:    time.Now(),
	})
}

func appendLeaderboardDeleteEvent(ctx context.Context, leaderboardID string) error {
	return appendDurableEvent(ctx, DurableEvent{
		Operation:     DurableOpLeaderboardDelete,
		LeaderboardID: leaderboardID,
		Payload:       json.RawMessage(`{}`),
		OccurredAt:    time.Now(),
	})
}

func appendItemUpsertEvent(ctx context.Context, leaderboardID, itemID string, data map[string]interface{}, updatedAt time.Time, score *float64) error {
	payload, err := json.Marshal(DurableItemPayload{
		ItemID:    itemID,
		Data:      data,
		Score:     score,
		UpdatedAt: updatedAt,
		IsDeleted: false,
	})
	if err != nil {
		return fmt.Errorf("marshal item durable payload: %w", err)
	}

	return appendDurableEvent(ctx, DurableEvent{
		Operation:     DurableOpItemUpsert,
		LeaderboardID: leaderboardID,
		ItemID:        itemID,
		Payload:       payload,
		OccurredAt:    updatedAt,
	})
}

func appendItemDeleteEvent(ctx context.Context, leaderboardID, itemID string) error {
	return appendDurableEvent(ctx, DurableEvent{
		Operation:     DurableOpItemDelete,
		LeaderboardID: leaderboardID,
		ItemID:        itemID,
		Payload:       json.RawMessage(`{}`),
		OccurredAt:    time.Now(),
	})
}

func appendItemMutateEvent(ctx context.Context, leaderboardID, itemID string, ops []FieldMutation, expectedUpdatedAt time.Time, data map[string]interface{}, updatedAt time.Time, score *float64) error {
	durableOps := make([]DurableItemMutationOp, len(ops))
	for i, op := range ops {
		durableOps[i] = DurableItemMutationOp{
			Field: op.Field,
			Op:    op.Op,
			Value: op.Value,
		}
	}

	payload, err := json.Marshal(DurableItemMutationPayload{
		ItemID:            itemID,
		Ops:               durableOps,
		ExpectedUpdatedAt: expectedUpdatedAt,
		Data:              cloneMap(data),
		Score:             score,
		UpdatedAt:         updatedAt,
	})
	if err != nil {
		return fmt.Errorf("marshal item mutate durable payload: %w", err)
	}

	return appendDurableEvent(ctx, DurableEvent{
		Operation:     DurableOpItemMutate,
		LeaderboardID: leaderboardID,
		ItemID:        itemID,
		Payload:       payload,
		OccurredAt:    updatedAt,
	})
}
