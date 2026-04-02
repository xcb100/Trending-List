package core

import (
	"context"
	"time"
)

// Repository 定义排行榜数据存储接口
type Repository interface {
	// GetMetadata 获取排行榜元数据
	GetMetadata(ctx context.Context, lbID string) (map[string]string, error)
	// SaveMetadata 保存排行榜元数据
	SaveMetadata(ctx context.Context, lbID string, metadata map[string]string) error

	// SaveItemData 保存条目原始数据，不更新分数
	SaveItemData(ctx context.Context, lbID string, itemID string, data map[string]interface{}, updatedAt time.Time) error
	// UpdateItemScore 更新条目分数
	UpdateItemScore(ctx context.Context, lbID string, itemID string, score float64) error
	// UpsertItem 同时更新条目数据和分数
	UpsertItem(ctx context.Context, lbID string, itemID string, score float64, data map[string]interface{}, updatedAt time.Time) error

	// MarkItemDirty 标记条目是否需要重算
	MarkItemDirty(ctx context.Context, lbID string, itemID string, dirty bool) error

	// ScanDirtyItemIDs 分批获取待重算的条目 ID 列表，防止大 key 发生 OOM
	ScanDirtyItemIDs(ctx context.Context, lbID string, cursor uint64, count int64) ([]string, uint64, error)

	// CommitRecomputedScores 采用原子化提交分数并定点清除脏标记（通过 UpdatedAt 校验防止并发 ABA 覆盖）
	CommitRecomputedScores(ctx context.Context, lbID string, scores map[string]float64, updatedAts map[string]time.Time) error

	// ClearDirtyItemIDs 批量清除待重算条目
	ClearDirtyItemIDs(ctx context.Context, lbID string, itemIDs []string) error

	// AcquireLock 获取分布式锁（防止多个副本并发执行定时任务）
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// GetAllLeaderboardIDs 获取系统中注册的所有排行榜 ID
	GetAllLeaderboardIDs(ctx context.Context) ([]string, error)

	// AddScheduledLeaderboard 记录定时策略的排行榜 ID，附加分级 tier 属性
	AddScheduledLeaderboard(ctx context.Context, lbID string, tier string) error

	// RemoveScheduledLeaderboard 移除定时策略的排行榜 ID
	RemoveScheduledLeaderboard(ctx context.Context, lbID string) error

	// GetScheduledLeaderboardIDs 获取对应分级 tier 的定时策略排行榜 ID
	GetScheduledLeaderboardIDs(ctx context.Context, tier string) ([]string, error)

	// GetDirtyItemIDs 获取待重算的条目 ID 列表
	GetDirtyItemIDs(ctx context.Context, lbID string) ([]string, error)

	// GetItems 批量获取条目
	GetItems(ctx context.Context, lbID string, itemIDs []string) ([]*Item, error)
	// UpdateItemsScores 批量更新条目分数
	UpdateItemsScores(ctx context.Context, lbID string, scores map[string]float64) error

	// GetTopN 获取前N名
	GetTopN(ctx context.Context, lbID string, n int) ([]*Item, error)
	// IterateItems 遍历所有条目
	IterateItems(ctx context.Context, lbID string, callback func(item *Item) bool) error
	// GetItem 获取单个条目
	GetItem(ctx context.Context, lbID string, itemID string) (*Item, error)
}
