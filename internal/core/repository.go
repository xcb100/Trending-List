package core

import (
	"context"
	"time"
)

type Repository interface {
	GetMetadata(ctx context.Context, lbID string) (map[string]string, error)
	SaveMetadata(ctx context.Context, lbID string, metadata map[string]string) error
	DeleteLeaderboard(ctx context.Context, lbID string) error

	SaveItemData(ctx context.Context, lbID string, itemID string, data map[string]interface{}, updatedAt time.Time) error
	UpdateItemScore(ctx context.Context, lbID string, itemID string, score float64) error
	UpsertItem(ctx context.Context, lbID string, itemID string, score float64, data map[string]interface{}, updatedAt time.Time) error
	DeleteItem(ctx context.Context, lbID string, itemID string) error

	MarkItemDirty(ctx context.Context, lbID string, itemID string, dirty bool) error
	ScanDirtyItemIDs(ctx context.Context, lbID string, cursor uint64, count int64) ([]string, uint64, error)
	CommitRecomputedScores(ctx context.Context, lbID string, scores map[string]float64, updatedAts map[string]time.Time) error
	PruneItems(ctx context.Context, lbID string, itemIDs []string) error

	AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, key string) error
	GetAllLeaderboardIDs(ctx context.Context) ([]string, error)

	AddScheduledLeaderboard(ctx context.Context, lbID string, tier string) error
	RemoveScheduledLeaderboard(ctx context.Context, lbID string) error
	GetScheduledLeaderboardIDs(ctx context.Context, tier string) ([]string, error)

	GetDirtyItemIDs(ctx context.Context, lbID string) ([]string, error)
	GetItems(ctx context.Context, lbID string, itemIDs []string) ([]*Item, error)

	GetTopN(ctx context.Context, lbID string, n int) ([]*Item, error)
	GetItem(ctx context.Context, lbID string, itemID string) (*Item, error)
}
