package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

const (
	// RefreshPolicyRealtime 表示写入后立即重算分数。
	RefreshPolicyRealtime = "realtime"
	// RefreshPolicyScheduled 表示写入后只标记脏数据，由定时任务或手动触发重算。
	RefreshPolicyScheduled = "scheduled"
)

// Item 表示排行榜中的单个条目。
type Item struct {
	ID        string                 `json:"id"`
	Data      map[string]interface{} `json:"data"`
	Score     float64                `json:"score"`
	UpdatedAt time.Time              `json:"updated_at"`
}

// DataPayload 用于在 Redis Hash 中存储的数据结构。
type DataPayload struct {
	Data      map[string]interface{} `json:"data"`
	UpdatedAt time.Time              `json:"updated_at"`
}

// Leaderboard 管理条目和排序逻辑。
type Leaderboard struct {
	ID               string
	Expression       string
	Schema           map[string]interface{}
	RefreshPolicy    string
	CronSpec         string
	LastRecomputedAt time.Time
	program          *vm.Program
	repo             Repository
}

var (
	Leaderboards = make(map[string]*Leaderboard)
	LB_Mu        sync.RWMutex
	envPool      = sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{}, 16)
		},
	}
)

func getEnv(data map[string]interface{}, updatedAt int64) map[string]interface{} {
	env := envPool.Get().(map[string]interface{})
	for k := range env {
		delete(env, k)
	}
	for k, v := range data {
		if vFloat, ok := v.(float64); ok {
			env[k] = vFloat
		} else {
			env[k] = v
		}
	}
	env["now"] = float64(time.Now().Unix())
	if _, ok := env["updated_at"]; !ok {
		env["updated_at"] = float64(updatedAt)
	}
	return env
}

func releaseEnv(env map[string]interface{}) {
	envPool.Put(env)
}

func normalizeRefreshPolicy(policy string) string {
	switch policy {
	case "", RefreshPolicyRealtime:
		return RefreshPolicyRealtime
	case RefreshPolicyScheduled:
		return RefreshPolicyScheduled
	default:
		return ""
	}
}

func compileProgram(expression string, schema map[string]interface{}) (*vm.Program, error) {
	validationEnv := getEnv(schema, 0)
	defer releaseEnv(validationEnv)
	program, err := expr.Compile(expression, expr.Env(validationEnv))
	if err != nil {
		return nil, fmt.Errorf("failed to compile ranking expression: %w", err)
	}
	return program, nil
}

func serializeSchema(schema map[string]interface{}) string {
	if schema == nil {
		return "{}"
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func deserializeSchema(schemaText string) map[string]interface{} {
	if schemaText == "" {
		return map[string]interface{}{}
	}
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaText), &schema); err != nil {
		return map[string]interface{}{}
	}
	return schema
}

func metadataFromLeaderboard(lb *Leaderboard) map[string]string {
	return map[string]string{
		"expression":         lb.Expression,
		"schema":             serializeSchema(lb.Schema),
		"refresh_policy":     lb.RefreshPolicy,
		"cron_spec":          lb.CronSpec,
		"last_recomputed_at": lb.LastRecomputedAt.Format(time.RFC3339Nano),
	}
}

// CreateLeaderboard 创建一个新的排行榜并在全局注册。
func CreateLeaderboard(ctx context.Context, id, expression string, schema map[string]interface{}, refreshPolicy string, cronSpec string, repo Repository) (*Leaderboard, error) {
	policy := normalizeRefreshPolicy(refreshPolicy)
	if policy == "" {
		return nil, fmt.Errorf("invalid refresh policy: %s", refreshPolicy)
	}
	if policy == RefreshPolicyScheduled && cronSpec == "" {
		return nil, fmt.Errorf("cron_spec is required when refresh_policy is scheduled")
	}

	program, err := compileProgram(expression, schema)
	if err != nil {
		return nil, err
	}

	lb := &Leaderboard{
		ID:            id,
		Expression:    expression,
		Schema:        schema,
		RefreshPolicy: policy,
		CronSpec:      cronSpec,
		program:       program,
		repo:          repo,
	}

	if err := repo.SaveMetadata(ctx, id, metadataFromLeaderboard(lb)); err != nil {
		return nil, fmt.Errorf("failed to save leaderboard metadata: %w", err)
	}

	LB_Mu.Lock()
	Leaderboards[id] = lb
	LB_Mu.Unlock()

	return lb, nil
}

// DefaultRepo 用于在运行时恢复排行榜实例。
var DefaultRepo Repository

func SetDefaultRepo(repo Repository) {
	DefaultRepo = repo
}

// GetLeaderboard 获取排行榜实例；如果内存中不存在，则尝试从仓储恢复。
func GetLeaderboard(ctx context.Context, id string) *Leaderboard {
	LB_Mu.RLock()
	lb, ok := Leaderboards[id]
	LB_Mu.RUnlock()
	if ok {
		return lb
	}
	if DefaultRepo == nil {
		return nil
	}

	meta, err := DefaultRepo.GetMetadata(ctx, id)
	if err != nil || len(meta) == 0 {
		return nil
	}

	expression := meta["expression"]
	if expression == "" {
		return nil
	}
	schema := deserializeSchema(meta["schema"])
	policy := normalizeRefreshPolicy(meta["refresh_policy"])
	if policy == "" {
		policy = RefreshPolicyRealtime
	}
	cronSpec := meta["cron_spec"]

	program, err := compileProgram(expression, schema)
	if err != nil {
		fmt.Printf("恢复排行榜 %s 失败: %v\n", id, err)
		return nil
	}

	lb = &Leaderboard{
		ID:            id,
		Expression:    expression,
		Schema:        schema,
		RefreshPolicy: policy,
		CronSpec:      cronSpec,
		program:       program,
		repo:          DefaultRepo,
	}
	if lastRecomputedAt := meta["last_recomputed_at"]; lastRecomputedAt != "" {
		if ts, parseErr := time.Parse(time.RFC3339Nano, lastRecomputedAt); parseErr == nil {
			lb.LastRecomputedAt = ts
		}
	}

	LB_Mu.Lock()
	if existing, exists := Leaderboards[id]; exists {
		LB_Mu.Unlock()
		return existing
	}
	Leaderboards[id] = lb
	LB_Mu.Unlock()

	return lb
}

func (lb *Leaderboard) evaluateScore(data map[string]interface{}, updatedAt time.Time) (float64, error) {
	env := getEnv(data, updatedAt.Unix())
	defer releaseEnv(env)
	output, err := expr.Run(lb.program, env)
	if err != nil {
		return 0, fmt.Errorf("failed to evaluate score: %w", err)
	}

	switch v := output.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case string:
		parsed, parseErr := strconv.ParseFloat(v, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("expression result must be a number, got %T", output)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("expression result must be a number, got %T", output)
	}
}

// UpsertItem 添加或更新排行榜中的条目。
func (lb *Leaderboard) UpsertItem(ctx context.Context, id string, data map[string]interface{}) (*Item, error) {
	ts := time.Now()
	if lb.RefreshPolicy == RefreshPolicyScheduled {
		if err := lb.repo.SaveItemData(ctx, lb.ID, id, data, ts); err != nil {
			return nil, fmt.Errorf("failed to save item data: %w", err)
		}
		if err := lb.repo.MarkItemDirty(ctx, lb.ID, id, true); err != nil {
			return nil, fmt.Errorf("failed to mark item dirty: %w", err)
		}
		return &Item{
			ID:        id,
			Data:      data,
			Score:     0,
			UpdatedAt: ts,
		}, nil
	}

	score, err := lb.evaluateScore(data, ts)
	if err != nil {
		return nil, err
	}
	if err := lb.repo.UpsertItem(ctx, lb.ID, id, score, data, ts); err != nil {
		return nil, fmt.Errorf("failed to upsert item: %w", err)
	}
	return &Item{ID: id, Data: data, Score: score, UpdatedAt: ts}, nil
}

// Recompute 重新计算 dirty 条目的分数。
func (lb *Leaderboard) Recompute(ctx context.Context) error {
	itemIDs, err := lb.repo.GetDirtyItemIDs(ctx, lb.ID)
	if err != nil {
		return fmt.Errorf("failed to get dirty items: %w", err)
	}

	if len(itemIDs) == 0 {
		lb.LastRecomputedAt = time.Now()
		return lb.repo.SaveMetadata(ctx, lb.ID, metadataFromLeaderboard(lb))
	}

	batchSize := 500
	for i := 0; i < len(itemIDs); i += batchSize {
		end := i + batchSize
		if end > len(itemIDs) {
			end = len(itemIDs)
		}
		batchIDs := itemIDs[i:end]

		items, err := lb.repo.GetItems(ctx, lb.ID, batchIDs)
		if err != nil {
			fmt.Printf("批量读取条目失败: %v\n", err)
			continue
		}

		scores := make(map[string]float64)
		validIDs := make([]string, 0, len(items))

		for _, item := range items {
			if item == nil {
				continue
			}
			score, err := lb.evaluateScore(item.Data, item.UpdatedAt)
			if err != nil {
				fmt.Printf("重算条目 %s 分数失败: %v\n", item.ID, err)
				continue
			}
			scores[item.ID] = score
			validIDs = append(validIDs, item.ID)
		}

		if len(scores) > 0 {
			if err := lb.repo.UpdateItemsScores(ctx, lb.ID, scores); err != nil {
				fmt.Printf("批量更新分数失败: %v\n", err)
				continue
			}
		}

		if len(validIDs) > 0 {
			if err := lb.repo.ClearDirtyItemIDs(ctx, lb.ID, validIDs); err != nil {
				fmt.Printf("批量清理 dirty 标记失败: %v\n", err)
			}
		}
	}

	lb.LastRecomputedAt = time.Now()
	if err := lb.repo.SaveMetadata(ctx, lb.ID, metadataFromLeaderboard(lb)); err != nil {
		return fmt.Errorf("failed to update recompute metadata: %w", err)
	}
	return nil
}

// GetTopN 返回分数最高的前 N 个条目。
func (lb *Leaderboard) GetTopN(ctx context.Context, n int) []*Item {
	items, err := lb.repo.GetTopN(ctx, lb.ID, n)
	if err != nil {
		fmt.Printf("获取排行榜前 %d 名失败: %v\n", n, err)
		return []*Item{}
	}

	return items
}
