package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"golang.org/x/sync/singleflight"
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
	lbMu         sync.RWMutex
	envPool      = sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{}, 16)
		},
	}
	restoreGroup singleflight.Group
	coreLogger   = slog.New(slog.NewJSONHandler(os.Stdout, nil))
)

var (
	ErrLeaderboardNotFound = errors.New("leaderboard not found")
	ErrRestoreFailed       = errors.New("leaderboard restore failed")
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

	// 核心优化：维护定向梯队子集，避免后台定时器进行高昂的全局扫表
	if policy == RefreshPolicyScheduled {
		tier := DetermineTier(cronSpec)
		_ = repo.AddScheduledLeaderboard(ctx, id, tier)
	} else {
		_ = repo.RemoveScheduledLeaderboard(ctx, id)
	}

	lbMu.Lock()
	Leaderboards[id] = lb
	lbMu.Unlock()

	return lb, nil
}

// DefaultRepo 用于在运行时恢复排行榜实例。
var DefaultRepo Repository

func SetDefaultRepo(repo Repository) {
	DefaultRepo = repo
}

// GetLeaderboard 获取排行榜实例；如果内存中不存在，则尝试从仓储恢复。
func GetLeaderboard(ctx context.Context, id string) (*Leaderboard, error) {
	lbMu.RLock()
	lb, ok := Leaderboards[id]
	lbMu.RUnlock()
	if ok {
		return lb, nil
	}
	if DefaultRepo == nil {
		return nil, fmt.Errorf("%w: id=%s", ErrLeaderboardNotFound, id)
	}

	v, err, _ := restoreGroup.Do(id, func() (interface{}, error) {
		lbMu.RLock()
		existing, exists := Leaderboards[id]
		lbMu.RUnlock()
		if exists {
			return existing, nil
		}

		meta, repoErr := DefaultRepo.GetMetadata(ctx, id)
		if repoErr != nil {
			return nil, fmt.Errorf("%w: id=%s metadata query failed: %v", ErrRestoreFailed, id, repoErr)
		}
		if len(meta) == 0 {
			return nil, fmt.Errorf("%w: id=%s", ErrLeaderboardNotFound, id)
		}

		expression := meta["expression"]
		if expression == "" {
			return nil, fmt.Errorf("%w: id=%s empty expression", ErrRestoreFailed, id)
		}
		schema := deserializeSchema(meta["schema"])
		policy := normalizeRefreshPolicy(meta["refresh_policy"])
		if policy == "" {
			policy = RefreshPolicyRealtime
		}
		cronSpec := meta["cron_spec"]

		program, compileErr := compileProgram(expression, schema)
		if compileErr != nil {
			return nil, fmt.Errorf("%w: id=%s compile failed: %v", ErrRestoreFailed, id, compileErr)
		}

		restored := &Leaderboard{
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
				restored.LastRecomputedAt = ts
			}
		}

		lbMu.Lock()
		if current, exists := Leaderboards[id]; exists {
			lbMu.Unlock()
			return current, nil
		}
		Leaderboards[id] = restored
		lbMu.Unlock()
		return restored, nil
	})
	if err != nil {
		if errors.Is(err, ErrLeaderboardNotFound) {
			return nil, err
		}
		coreLogger.Error("leaderboard restore failed", "leaderboard_id", id, "error", err)
		return nil, err
	}
	if v == nil {
		return nil, fmt.Errorf("%w: id=%s", ErrLeaderboardNotFound, id)
	}
	return v.(*Leaderboard), nil
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
	cursor := uint64(0)
	for {
		var batchIDs []string
		var err error
		// 1. 采用 SSCAN 分批拉取替代 SMEMBERS，杜绝 1000 万级脏数据引发内存 OOM
		batchIDs, cursor, err = lb.repo.ScanDirtyItemIDs(ctx, lb.ID, cursor, 500)
		if err != nil {
			return fmt.Errorf("failed to scan dirty items: %w", err)
		}

		if len(batchIDs) > 0 {
			items, err := lb.repo.GetItems(ctx, lb.ID, batchIDs)
			if err != nil {
				coreLogger.Error("batch read items failed", "leaderboard_id", lb.ID, "error", err)
				continue
			}

			scores := make(map[string]float64)
			updatedAts := make(map[string]time.Time)

			for _, item := range items {
				if item == nil {
					continue
				}
				score, err := lb.evaluateScore(item.Data, item.UpdatedAt)
				if err != nil {
					coreLogger.Error("recompute item score failed", "leaderboard_id", lb.ID, "item_id", item.ID, "error", err)
					continue
				}
				scores[item.ID] = score
				// 记录计算时的锚点时间，用于防并发覆盖
				updatedAts[item.ID] = item.UpdatedAt
			}

			if len(scores) > 0 {
				// 2. 剥弃 UpdateItemsScores + ClearDirtyItemIDs 极易引发竞态丢失的组合，改用基于 Lua 的单边聚合方法
				if err := lb.repo.CommitRecomputedScores(ctx, lb.ID, scores, updatedAts); err != nil {
					coreLogger.Error("batch atomic commit scores failed", "leaderboard_id", lb.ID, "error", err)
				}
			}
		}

		if cursor == 0 {
			break
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
		coreLogger.Error("get topN failed", "leaderboard_id", lb.ID, "n", n, "error", err)
		return []*Item{}
	}

	return items
}
