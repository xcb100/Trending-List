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
	RefreshPolicyRealtime  = "realtime"
	RefreshPolicyScheduled = "scheduled"
)

type Item struct {
	ID        string                 `json:"id"`
	Data      map[string]interface{} `json:"data"`
	Score     float64                `json:"score"`
	UpdatedAt time.Time              `json:"updated_at"`
}

type DataPayload struct {
	Data      map[string]interface{} `json:"data"`
	UpdatedAt time.Time              `json:"updated_at"`
}

type Leaderboard struct {
	mu               sync.RWMutex
	ID               string
	Expression       string
	Schema           map[string]interface{}
	RefreshPolicy    string
	CronSpec         string
	LastRecomputedAt time.Time
	program          *vm.Program
	repo             Repository
}

type leaderboardSnapshot struct {
	ID               string
	Expression       string
	Schema           map[string]interface{}
	RefreshPolicy    string
	CronSpec         string
	LastRecomputedAt time.Time
	program          *vm.Program
}

type LeaderboardState struct {
	ID               string                 `json:"id"`
	Expression       string                 `json:"expression"`
	Schema           map[string]interface{} `json:"schema"`
	RefreshPolicy    string                 `json:"refresh_policy"`
	CronSpec         string                 `json:"cron_spec"`
	LastRecomputedAt time.Time              `json:"last_recomputed_at"`
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
	ErrLeaderboardExists   = errors.New("leaderboard already exists")
	ErrRestoreFailed       = errors.New("leaderboard restore failed")
	ErrRecomputeFailed     = errors.New("leaderboard recompute failed")
	ErrRecomputeInProgress = errors.New("leaderboard recompute already in progress")
)

func cloneMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func leaderboardCreateLockKey(id string) string {
	return fmt.Sprintf("lb:%s:create_lock", id)
}

func leaderboardRecomputeLockKey(id string) string {
	return fmt.Sprintf("lb:%s:recompute_lock", id)
}

func getEnv(data map[string]interface{}, updatedAt int64) map[string]interface{} {
	env := envPool.Get().(map[string]interface{})
	for k := range env {
		delete(env, k)
	}
	for k, v := range data {
		if vFloat, ok := v.(float64); ok {
			env[k] = vFloat
			continue
		}
		env[k] = v
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
	snapshot := lb.snapshot()
	return metadataFromSnapshot(snapshot)
}

func metadataFromSnapshot(snapshot leaderboardSnapshot) map[string]string {
	return map[string]string{
		"expression":         snapshot.Expression,
		"schema":             serializeSchema(snapshot.Schema),
		"refresh_policy":     snapshot.RefreshPolicy,
		"cron_spec":          snapshot.CronSpec,
		"last_recomputed_at": snapshot.LastRecomputedAt.Format(time.RFC3339Nano),
	}
}

func (lb *Leaderboard) snapshot() leaderboardSnapshot {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	// 快照会复制当前可变状态，这样调用方可以在释放锁之后安全地执行
	// Redis IO、算分等耗时操作，避免长时间持有锁。
	return leaderboardSnapshot{
		ID:               lb.ID,
		Expression:       lb.Expression,
		Schema:           cloneMap(lb.Schema),
		RefreshPolicy:    lb.RefreshPolicy,
		CronSpec:         lb.CronSpec,
		LastRecomputedAt: lb.LastRecomputedAt,
		program:          lb.program,
	}
}

func (lb *Leaderboard) State() LeaderboardState {
	snapshot := lb.snapshot()
	return LeaderboardState{
		ID:               snapshot.ID,
		Expression:       snapshot.Expression,
		Schema:           cloneMap(snapshot.Schema),
		RefreshPolicy:    snapshot.RefreshPolicy,
		CronSpec:         snapshot.CronSpec,
		LastRecomputedAt: snapshot.LastRecomputedAt,
	}
}

func (lb *Leaderboard) setLastRecomputedAt(ts time.Time) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.LastRecomputedAt = ts
}

func (lb *Leaderboard) replaceSchedule(policy, cronSpec string) leaderboardSnapshot {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	previous := leaderboardSnapshot{
		ID:               lb.ID,
		Expression:       lb.Expression,
		Schema:           cloneMap(lb.Schema),
		RefreshPolicy:    lb.RefreshPolicy,
		CronSpec:         lb.CronSpec,
		LastRecomputedAt: lb.LastRecomputedAt,
		program:          lb.program,
	}

	lb.RefreshPolicy = policy
	lb.CronSpec = cronSpec
	return previous
}

func (lb *Leaderboard) restoreFromSnapshot(snapshot leaderboardSnapshot) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.Expression = snapshot.Expression
	lb.Schema = cloneMap(snapshot.Schema)
	lb.RefreshPolicy = snapshot.RefreshPolicy
	lb.CronSpec = snapshot.CronSpec
	lb.LastRecomputedAt = snapshot.LastRecomputedAt
	lb.program = snapshot.program
}

func ensureLeaderboardDoesNotExist(ctx context.Context, id string, repo Repository) error {
	lbMu.RLock()
	_, existsInMemory := Leaderboards[id]
	lbMu.RUnlock()
	if existsInMemory {
		return fmt.Errorf("%w: id=%s", ErrLeaderboardExists, id)
	}

	meta, err := repo.GetMetadata(ctx, id)
	if err != nil {
		return fmt.Errorf("check leaderboard conflict: %w", err)
	}
	if len(meta) > 0 {
		return fmt.Errorf("%w: id=%s", ErrLeaderboardExists, id)
	}
	return nil
}

func CreateLeaderboard(ctx context.Context, id, expression string, schema map[string]interface{}, refreshPolicy string, cronSpec string, repo Repository) (*Leaderboard, error) {
	if repo == nil {
		recordCreate("error")
		return nil, fmt.Errorf("repository is required")
	}

	// 这里使用一个短时 Redis 锁，缩小“冲突检查”和“写入元数据”之间的时间窗，
	// 避免多实例并发创建同一个排行榜时互相覆盖。
	locked, err := repo.AcquireLock(ctx, leaderboardCreateLockKey(id), CreateLockTTL)
	if err != nil {
		recordCreate("error")
		return nil, fmt.Errorf("acquire create lock: %w", err)
	}
	if !locked {
		recordCreate("conflict")
		return nil, fmt.Errorf("%w: id=%s", ErrLeaderboardExists, id)
	}
	defer func() {
		if err := repo.ReleaseLock(ctx, leaderboardCreateLockKey(id)); err != nil {
			coreLogger.Warn("release create lock failed", "leaderboard_id", id, "error", err)
		}
	}()

	if err := ensureLeaderboardDoesNotExist(ctx, id, repo); err != nil {
		if errors.Is(err, ErrLeaderboardExists) {
			recordCreate("conflict")
		} else {
			recordCreate("error")
		}
		return nil, err
	}

	policy := normalizeRefreshPolicy(refreshPolicy)
	if policy == "" {
		recordCreate("error")
		return nil, fmt.Errorf("invalid refresh policy: %s", refreshPolicy)
	}
	if policy == RefreshPolicyScheduled {
		if err := ValidateCronSpec(cronSpec); err != nil {
			recordCreate("error")
			return nil, err
		}
	}

	program, err := compileProgram(expression, schema)
	if err != nil {
		recordCreate("error")
		return nil, err
	}

	lb := &Leaderboard{
		ID:            id,
		Expression:    expression,
		Schema:        cloneMap(schema),
		RefreshPolicy: policy,
		CronSpec:      cronSpec,
		program:       program,
		repo:          repo,
	}

	if err := appendLeaderboardUpsertEvent(ctx, lb.snapshot()); err != nil {
		recordCreate("error")
		return nil, fmt.Errorf("append durable create event: %w", err)
	}

	if err := repo.SaveMetadata(ctx, id, metadataFromLeaderboard(lb)); err != nil {
		recordCreate("error")
		return nil, fmt.Errorf("failed to save leaderboard metadata: %w", err)
	}

	if policy == RefreshPolicyScheduled {
		if err := repo.AddScheduledLeaderboard(ctx, id, DetermineTier(cronSpec)); err != nil {
			recordCreate("error")
			return nil, fmt.Errorf("failed to register scheduled leaderboard: %w", err)
		}
	} else {
		if err := repo.RemoveScheduledLeaderboard(ctx, id); err != nil {
			recordCreate("error")
			return nil, fmt.Errorf("failed to unregister scheduled leaderboard: %w", err)
		}
	}

	lbMu.Lock()
	Leaderboards[id] = lb
	lbMu.Unlock()

	recordCreate("success")
	return lb, nil
}

var DefaultRepo Repository

func SetDefaultRepo(repo Repository) {
	DefaultRepo = repo
}

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

	value, err, _ := restoreGroup.Do(id, func() (interface{}, error) {
		// singleflight 用来收敛同一个排行榜的并发冷恢复，
		// 避免重复读取元数据和重复编译表达式。
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
		if policy == RefreshPolicyScheduled {
			if validateErr := ValidateCronSpec(cronSpec); validateErr != nil {
				return nil, fmt.Errorf("%w: id=%s invalid cron spec: %v", ErrRestoreFailed, id, validateErr)
			}
		}

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
			recordRestore("not_found")
			return nil, err
		}
		recordRestore("error")
		coreLogger.Error("leaderboard restore failed", "leaderboard_id", id, "error", err)
		return nil, err
	}
	if value == nil {
		recordRestore("not_found")
		return nil, fmt.Errorf("%w: id=%s", ErrLeaderboardNotFound, id)
	}
	recordRestore("success")
	return value.(*Leaderboard), nil
}

func (lb *Leaderboard) evaluateScore(data map[string]interface{}, updatedAt time.Time) (float64, error) {
	snapshot := lb.snapshot()
	return evaluateScoreWithProgram(snapshot.program, data, updatedAt)
}

func evaluateScoreWithProgram(program *vm.Program, data map[string]interface{}, updatedAt time.Time) (float64, error) {
	env := getEnv(data, updatedAt.Unix())
	defer releaseEnv(env)

	output, err := expr.Run(program, env)
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

func (lb *Leaderboard) UpsertItem(ctx context.Context, id string, data map[string]interface{}) (*Item, error) {
	snapshot := lb.snapshot()
	ts := time.Now()
	if snapshot.RefreshPolicy == RefreshPolicyScheduled {
		if err := appendItemUpsertEvent(ctx, snapshot.ID, id, data, ts, nil); err != nil {
			recordUpsert(snapshot.RefreshPolicy, "error")
			return nil, fmt.Errorf("append durable item event: %w", err)
		}
		if err := lb.repo.SaveItemData(ctx, snapshot.ID, id, data, ts); err != nil {
			recordUpsert(snapshot.RefreshPolicy, "error")
			return nil, fmt.Errorf("failed to save item data: %w", err)
		}
		if err := lb.repo.MarkItemDirty(ctx, snapshot.ID, id, true); err != nil {
			recordUpsert(snapshot.RefreshPolicy, "error")
			return nil, fmt.Errorf("failed to mark item dirty: %w", err)
		}
		recordUpsert(snapshot.RefreshPolicy, "success")
		return &Item{
			ID:        id,
			Data:      data,
			Score:     0,
			UpdatedAt: ts,
		}, nil
	}

	score, err := evaluateScoreWithProgram(snapshot.program, data, ts)
	if err != nil {
		recordUpsert(snapshot.RefreshPolicy, "error")
		return nil, err
	}
	if err := appendItemUpsertEvent(ctx, snapshot.ID, id, data, ts, &score); err != nil {
		recordUpsert(snapshot.RefreshPolicy, "error")
		return nil, fmt.Errorf("append durable item event: %w", err)
	}
	if err := lb.repo.UpsertItem(ctx, snapshot.ID, id, score, data, ts); err != nil {
		recordUpsert(snapshot.RefreshPolicy, "error")
		return nil, fmt.Errorf("failed to upsert item: %w", err)
	}
	recordUpsert(snapshot.RefreshPolicy, "success")
	return &Item{ID: id, Data: data, Score: score, UpdatedAt: ts}, nil
}

func (lb *Leaderboard) Recompute(ctx context.Context) error {
	return lb.recompute(ctx, "manual", true)
}

func (lb *Leaderboard) recompute(ctx context.Context, trigger string, failOnLocked bool) error {
	snapshot := lb.snapshot()
	startedAt := time.Now()

	// 手动重算和调度重算共用同一把锁，保证同一个排行榜在多实例下
	// 同时最多只有一个批量重算任务在执行。
	locked, err := lb.repo.AcquireLock(ctx, leaderboardRecomputeLockKey(snapshot.ID), ScheduledTaskLockTTL)
	if err != nil {
		recordRecompute(trigger, "error", time.Since(startedAt))
		return fmt.Errorf("acquire recompute lock: %w", err)
	}
	if !locked {
		status := "skipped_locked"
		if failOnLocked {
			status = "in_progress"
		}
		recordRecompute(trigger, status, time.Since(startedAt))
		if failOnLocked {
			return fmt.Errorf("%w: leaderboard_id=%s", ErrRecomputeInProgress, snapshot.ID)
		}
		return nil
	}
	defer func() {
		if err := lb.repo.ReleaseLock(ctx, leaderboardRecomputeLockKey(snapshot.ID)); err != nil {
			coreLogger.Warn("release recompute lock failed", "leaderboard_id", snapshot.ID, "error", err)
		}
	}()

	cursor := uint64(0)
	var failureCount int
	for {
		batchIDs, nextCursor, err := lb.repo.ScanDirtyItemIDs(ctx, snapshot.ID, cursor, 500)
		if err != nil {
			recordRecompute(trigger, "error", time.Since(startedAt))
			return fmt.Errorf("failed to scan dirty items: %w", err)
		}
		cursor = nextCursor

		if len(batchIDs) > 0 {
			items, err := lb.repo.GetItems(ctx, snapshot.ID, batchIDs)
			if err != nil {
				coreLogger.Error("batch read items failed", "leaderboard_id", snapshot.ID, "error", err)
				failureCount++
				if cursor == 0 {
					break
				}
				continue
			}

			resolvedIDs := make(map[string]struct{}, len(items))
			for _, item := range items {
				if item != nil {
					resolvedIDs[item.ID] = struct{}{}
				}
			}

			var unresolvedIDs []string
			for _, id := range batchIDs {
				if _, ok := resolvedIDs[id]; !ok {
					unresolvedIDs = append(unresolvedIDs, id)
				}
			}
			// dirty 标记可能在源数据被删除或损坏后残留，
			// 这里顺手清理，避免这些条目长期卡在待重算集合里。
			if len(unresolvedIDs) > 0 {
				if err := lb.repo.PruneItems(ctx, snapshot.ID, unresolvedIDs); err != nil {
					coreLogger.Error("prune unresolved dirty items failed", "leaderboard_id", snapshot.ID, "count", len(unresolvedIDs), "error", err)
					failureCount++
				}
			}

			scores := make(map[string]float64)
			updatedAts := make(map[string]time.Time)
			for _, item := range items {
				if item == nil {
					continue
				}
				score, err := evaluateScoreWithProgram(snapshot.program, item.Data, item.UpdatedAt)
				if err != nil {
					coreLogger.Error("recompute item score failed", "leaderboard_id", snapshot.ID, "item_id", item.ID, "error", err)
					failureCount++
					continue
				}
				scores[item.ID] = score
				updatedAts[item.ID] = item.UpdatedAt
			}

			if len(scores) > 0 {
				// CommitRecomputedScores 会校验当前存储里的 updated_at，
				// 防止较早启动的重算结果覆盖后来更新过的数据。
				if err := lb.repo.CommitRecomputedScores(ctx, snapshot.ID, scores, updatedAts); err != nil {
					coreLogger.Error("batch atomic commit scores failed", "leaderboard_id", snapshot.ID, "error", err)
					failureCount++
				}
			}
		}

		if cursor == 0 {
			break
		}
	}

	if failureCount > 0 {
		recordRecompute(trigger, "error", time.Since(startedAt))
		return fmt.Errorf("%w: leaderboard_id=%s failure_count=%d", ErrRecomputeFailed, snapshot.ID, failureCount)
	}

	lb.setLastRecomputedAt(time.Now())
	if err := lb.repo.SaveMetadata(ctx, snapshot.ID, metadataFromLeaderboard(lb)); err != nil {
		recordRecompute(trigger, "error", time.Since(startedAt))
		return fmt.Errorf("failed to update recompute metadata: %w", err)
	}
	recordRecompute(trigger, "success", time.Since(startedAt))
	return nil
}

func (lb *Leaderboard) GetTopN(ctx context.Context, n int) ([]*Item, error) {
	snapshot := lb.snapshot()
	items, err := lb.repo.GetTopN(ctx, snapshot.ID, n)
	if err != nil {
		return nil, fmt.Errorf("get topN: %w", err)
	}
	return items, nil
}

func UpdateLeaderboardSchedule(ctx context.Context, lb *Leaderboard, cronSpec string) error {
	if err := ValidateCronSpec(cronSpec); err != nil {
		return err
	}

	previous := lb.replaceSchedule(RefreshPolicyScheduled, cronSpec)
	current := lb.snapshot()

	if err := appendLeaderboardUpsertEvent(ctx, current); err != nil {
		lb.restoreFromSnapshot(previous)
		return fmt.Errorf("append durable schedule event: %w", err)
	}

	if err := lb.repo.SaveMetadata(ctx, current.ID, metadataFromSnapshot(current)); err != nil {
		lb.restoreFromSnapshot(previous)
		return fmt.Errorf("failed to save schedule metadata: %w", err)
	}

	if err := lb.repo.AddScheduledLeaderboard(ctx, current.ID, DetermineTier(cronSpec)); err != nil {
		lb.restoreFromSnapshot(previous)
		_ = lb.repo.SaveMetadata(ctx, current.ID, metadataFromSnapshot(previous))
		if previous.RefreshPolicy == RefreshPolicyScheduled && previous.CronSpec != "" {
			_ = lb.repo.AddScheduledLeaderboard(ctx, current.ID, DetermineTier(previous.CronSpec))
		} else {
			_ = lb.repo.RemoveScheduledLeaderboard(ctx, current.ID)
		}
		return fmt.Errorf("failed to register scheduled leaderboard: %w", err)
	}

	return nil
}

func DeleteLeaderboard(ctx context.Context, id string) error {
	lb, err := GetLeaderboard(ctx, id)
	if err != nil {
		recordDeleteLeaderboard("error")
		return err
	}
	if err := appendLeaderboardDeleteEvent(ctx, id); err != nil {
		recordDeleteLeaderboard("error")
		return fmt.Errorf("append durable delete leaderboard event: %w", err)
	}
	if err := lb.repo.DeleteLeaderboard(ctx, id); err != nil {
		recordDeleteLeaderboard("error")
		return fmt.Errorf("delete leaderboard: %w", err)
	}

	lbMu.Lock()
	delete(Leaderboards, id)
	lbMu.Unlock()
	recordDeleteLeaderboard("success")
	return nil
}

func (lb *Leaderboard) DeleteItem(ctx context.Context, itemID string) error {
	snapshot := lb.snapshot()
	if err := appendItemDeleteEvent(ctx, snapshot.ID, itemID); err != nil {
		recordDeleteItem("error")
		return fmt.Errorf("append durable delete item event: %w", err)
	}
	if err := lb.repo.DeleteItem(ctx, snapshot.ID, itemID); err != nil {
		recordDeleteItem("error")
		return fmt.Errorf("delete item: %w", err)
	}
	recordDeleteItem("success")
	return nil
}
