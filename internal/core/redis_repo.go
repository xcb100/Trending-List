package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisRepository struct {
	client *redis.Client
}

func NewRedisRepository(client *redis.Client) *RedisRepository {
	return &RedisRepository{client: client}
}

func marshalPayload(data map[string]interface{}, updatedAt time.Time) ([]byte, error) {
	b, err := json.Marshal(DataPayload{Data: data, UpdatedAt: updatedAt})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal item data: %w", err)
	}
	return b, nil
}

func (r *RedisRepository) metadataKey(lbID string) string {
	return fmt.Sprintf("lb:%s:meta", lbID)
}

func (r *RedisRepository) itemsKey(lbID string) string {
	return fmt.Sprintf("lb:%s:items", lbID)
}

func (r *RedisRepository) scoresKey(lbID string) string {
	return fmt.Sprintf("lb:%s:scores", lbID)
}

func (r *RedisRepository) dirtyKey(lbID string) string {
	return fmt.Sprintf("lb:%s:dirty_items", lbID)
}

func (r *RedisRepository) GetMetadata(ctx context.Context, lbID string) (map[string]string, error) {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()
	return r.client.HGetAll(ctx, r.metadataKey(lbID)).Result()
}

func (r *RedisRepository) SaveMetadata(ctx context.Context, lbID string, metadata map[string]string) error {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()

	values := make([]interface{}, 0, len(metadata)*2)
	for k, v := range metadata {
		values = append(values, k, v)
	}
	return r.client.HSet(ctx, r.metadataKey(lbID), values...).Err()
}

func (r *RedisRepository) saveItemWithPayload(ctx context.Context, lbID string, itemID string, data map[string]interface{}, updatedAt time.Time) error {
	payloadBytes, err := marshalPayload(data, updatedAt)
	if err != nil {
		return err
	}
	return r.client.HSet(ctx, r.itemsKey(lbID), itemID, payloadBytes).Err()
}

func (r *RedisRepository) SaveItemData(ctx context.Context, lbID string, itemID string, data map[string]interface{}, updatedAt time.Time) error {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()
	return r.saveItemWithPayload(ctx, lbID, itemID, data, updatedAt)
}

func (r *RedisRepository) UpdateItemScore(ctx context.Context, lbID string, itemID string, score float64) error {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()
	return r.client.ZAdd(ctx, r.scoresKey(lbID), redis.Z{Score: score, Member: itemID}).Err()
}

func (r *RedisRepository) UpsertItem(ctx context.Context, lbID string, itemID string, score float64, data map[string]interface{}, updatedAt time.Time) error {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()

	payloadBytes, err := marshalPayload(data, updatedAt)
	if err != nil {
		return err
	}

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.itemsKey(lbID), itemID, payloadBytes)
	pipe.ZAdd(ctx, r.scoresKey(lbID), redis.Z{Score: score, Member: itemID})
	pipe.SRem(ctx, r.dirtyKey(lbID), itemID)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisRepository) MarkItemDirty(ctx context.Context, lbID string, itemID string, dirty bool) error {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()
	if dirty {
		return r.client.SAdd(ctx, r.dirtyKey(lbID), itemID).Err()
	}
	return r.client.SRem(ctx, r.dirtyKey(lbID), itemID).Err()
}

func (r *RedisRepository) ClearDirtyItemIDs(ctx context.Context, lbID string, itemIDs []string) error {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()
	if len(itemIDs) == 0 {
		return nil
	}
	args := make([]interface{}, len(itemIDs))
	for i, id := range itemIDs {
		args[i] = id
	}
	return r.client.SRem(ctx, r.dirtyKey(lbID), args...).Err()
}

func (r *RedisRepository) GetDirtyItemIDs(ctx context.Context, lbID string) ([]string, error) {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()
	return r.client.SMembers(ctx, r.dirtyKey(lbID)).Result()
}

func (r *RedisRepository) GetItem(ctx context.Context, lbID string, itemID string) (*Item, error) {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()

	itemsKey := r.itemsKey(lbID)
	scoresKey := r.scoresKey(lbID)

	payloadJSON, err := r.client.HGet(ctx, itemsKey, itemID).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var payload DataPayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return nil, err
	}

	score, err := r.client.ZScore(ctx, scoresKey, itemID).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err == redis.Nil {
		score = 0
	}

	return &Item{
		ID:        itemID,
		Data:      payload.Data,
		Score:     score,
		UpdatedAt: payload.UpdatedAt,
	}, nil
}

func (r *RedisRepository) GetItems(ctx context.Context, lbID string, itemIDs []string) ([]*Item, error) {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()

	if len(itemIDs) == 0 {
		return []*Item{}, nil
	}

	itemsKey := r.itemsKey(lbID)
	payloads, err := r.client.HMGet(ctx, itemsKey, itemIDs...).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	result := make([]*Item, 0, len(itemIDs))
	for i, p := range payloads {
		if p == nil {
			continue
		}
		jsonStr, ok := p.(string)
		if !ok || jsonStr == "" {
			continue
		}
		var payload DataPayload
		if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
			continue
		}
		result = append(result, &Item{
			ID:        itemIDs[i],
			Data:      payload.Data,
			Score:     0,
			UpdatedAt: payload.UpdatedAt,
		})
	}

	return result, nil
}

func (r *RedisRepository) UpdateItemsScores(ctx context.Context, lbID string, scores map[string]float64) error {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()

	if len(scores) == 0 {
		return nil
	}

	zArgs := make([]redis.Z, 0, len(scores))
	for id, score := range scores {
		zArgs = append(zArgs, redis.Z{Score: score, Member: id})
	}
	return r.client.ZAdd(ctx, r.scoresKey(lbID), zArgs...).Err()
}

func (r *RedisRepository) GetTopN(ctx context.Context, lbID string, n int) ([]*Item, error) {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()

	zResults, err := r.client.ZRevRangeWithScores(ctx, r.scoresKey(lbID), 0, int64(n-1)).Result()
	if err != nil {
		return nil, err
	}

	if len(zResults) == 0 {
		return []*Item{}, nil
	}

	itemIDs := make([]string, len(zResults))
	for i, z := range zResults {
		itemIDs[i] = z.Member.(string)
	}

	jsonDefaults, err := r.client.HMGet(ctx, r.itemsKey(lbID), itemIDs...).Result()
	if err != nil {
		return nil, err
	}

	items := make([]*Item, 0, len(zResults))
	for i, z := range zResults {
		jsonStr, ok := jsonDefaults[i].(string)
		if !ok || jsonStr == "" {
			continue
		}

		var payload DataPayload
		if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
			continue
		}

		items = append(items, &Item{
			ID:        z.Member.(string),
			Data:      payload.Data,
			Score:     z.Score,
			UpdatedAt: payload.UpdatedAt,
		})
	}
	return items, nil
}

func (r *RedisRepository) IterateItems(ctx context.Context, lbID string, callback func(item *Item) bool) error {
	ctx, cancel := WithOperationTimeout(ctx, RedisRepositoryTimeout)
	defer cancel()

	iter := r.client.HScan(ctx, r.itemsKey(lbID), 0, "", 0).Iterator()
	for iter.Next(ctx) {
		itemID := iter.Val()
		if !iter.Next(ctx) {
			break
		}
		itemDataJSON := iter.Val()

		var payload DataPayload
		if err := json.Unmarshal([]byte(itemDataJSON), &payload); err != nil {
			continue
		}

		item := &Item{
			ID:        itemID,
			Data:      payload.Data,
			Score:     0,
			UpdatedAt: payload.UpdatedAt,
		}

		if !callback(item) {
			break
		}
	}
	return iter.Err()
}
