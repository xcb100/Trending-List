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

	// ClearDirtyItemIDs 批量清除待重算条目
	ClearDirtyItemIDs(ctx context.Context, lbID string, itemIDs []string) error

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
