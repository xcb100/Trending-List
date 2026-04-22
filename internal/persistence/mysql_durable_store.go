package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"trendingList/internal/core"

	_ "github.com/go-sql-driver/mysql"
)

const snapshotMergerConsumerName = "mysql_snapshot_merger"

type MySQLDurableStoreOptions struct {
	Logger           *slog.Logger
	MergeInterval    time.Duration
	MergeBatchSize   int
	CleanupInterval  time.Duration
	CleanupRetention time.Duration
	CleanupBatchSize int
}

type MySQLDurableStore struct {
	db               *sql.DB
	logger           *slog.Logger
	mergeInterval    time.Duration
	mergeBatchSize   int
	cleanupInterval  time.Duration
	cleanupRetention time.Duration
	cleanupBatchSize int

	mu     sync.Mutex
	runCtx context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type durableEventRow struct {
	ID            int64
	Operation     string
	LeaderboardID string
	ItemID        sql.NullString
	Payload       []byte
	CreatedAt     time.Time
}

type leaderboardSnapshotRow struct {
	ID                string
	Expression        string
	SchemaJSON        []byte
	RefreshPolicy     string
	CronSpec          sql.NullString
	DefinitionVersion int64
	IsDeleted         bool
	LastEventID       int64
}

type itemMutationKey struct {
	leaderboardID string
	itemID        string
}

type itemSnapshotMutation struct {
	LeaderboardID string
	ItemID        string
	DataJSON      []byte
	Score         sql.NullFloat64
	ItemUpdatedAt time.Time
	IsDeleted     bool
	LastEventID   int64
}

type itemSnapshotRow struct {
	LeaderboardID string
	ItemID        string
	DataJSON      []byte
	Score         sql.NullFloat64
	ItemUpdatedAt time.Time
	IsDeleted     bool
	LastEventID   int64
}

type itemBatchState struct {
	mutation itemSnapshotMutation
	loaded   bool
	exists   bool
	changed  bool
}

type ReplayResult struct {
	Scope                string `json:"scope"`
	ReplayedLeaderboards int    `json:"replayed_leaderboards"`
	ReplayedItems        int    `json:"replayed_items"`
	RemovedLeaderboards  int    `json:"removed_leaderboards"`
}

func NewMySQLDurableStore(db *sql.DB, opts MySQLDurableStoreOptions) *MySQLDurableStore {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	mergeInterval := opts.MergeInterval
	if mergeInterval <= 0 {
		mergeInterval = time.Second
	}

	mergeBatchSize := opts.MergeBatchSize
	if mergeBatchSize <= 0 {
		mergeBatchSize = 500
	}

	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}

	cleanupRetention := opts.CleanupRetention
	if cleanupRetention <= 0 {
		cleanupRetention = 24 * time.Hour
	}

	cleanupBatchSize := opts.CleanupBatchSize
	if cleanupBatchSize <= 0 {
		cleanupBatchSize = 1000
	}

	return &MySQLDurableStore{
		db:               db,
		logger:           logger,
		mergeInterval:    mergeInterval,
		mergeBatchSize:   mergeBatchSize,
		cleanupInterval:  cleanupInterval,
		cleanupRetention: cleanupRetention,
		cleanupBatchSize: cleanupBatchSize,
	}
}

func (s *MySQLDurableStore) PingContext(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *MySQLDurableStore) EnsureSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS leaderboard_events (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			op_type VARCHAR(64) NOT NULL,
			leaderboard_id VARCHAR(191) NOT NULL,
			item_id VARCHAR(191) NULL,
			payload_json JSON NOT NULL,
			created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			KEY idx_events_created (created_at),
			KEY idx_events_lb_item (leaderboard_id, item_id),
			KEY idx_events_op_id (op_type, id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		`CREATE TABLE IF NOT EXISTS leaderboards (
			id VARCHAR(191) NOT NULL PRIMARY KEY,
			expression TEXT NOT NULL,
			schema_json JSON NOT NULL,
			refresh_policy VARCHAR(32) NOT NULL,
			cron_spec VARCHAR(255) NULL,
			definition_version BIGINT NOT NULL DEFAULT 1,
			is_deleted TINYINT(1) NOT NULL DEFAULT 0,
			last_event_id BIGINT NOT NULL,
			created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
			KEY idx_leaderboards_deleted (is_deleted),
			KEY idx_leaderboards_last_event (last_event_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		`CREATE TABLE IF NOT EXISTS leaderboard_items (
			leaderboard_id VARCHAR(191) NOT NULL,
			item_id VARCHAR(191) NOT NULL,
			data_json JSON NOT NULL,
			score DOUBLE NULL,
			item_updated_at DATETIME(6) NOT NULL,
			is_deleted TINYINT(1) NOT NULL DEFAULT 0,
			last_event_id BIGINT NOT NULL,
			created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
			PRIMARY KEY (leaderboard_id, item_id),
			KEY idx_items_deleted (leaderboard_id, is_deleted),
			KEY idx_items_last_event (leaderboard_id, last_event_id),
			KEY idx_items_updated (leaderboard_id, item_updated_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		`CREATE TABLE IF NOT EXISTS event_consumers (
			consumer_name VARCHAR(128) NOT NULL PRIMARY KEY,
			last_event_id BIGINT NOT NULL DEFAULT 0,
			updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
	}

	for _, stmt := range statements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure mysql schema: %w", err)
		}
	}

	return nil
}

func (s *MySQLDurableStore) AppendEvent(ctx context.Context, event core.DurableEvent) error {
	payload := event.Payload
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now()
	}

	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO leaderboard_events (op_type, leaderboard_id, item_id, payload_json, created_at)
		 VALUES (?, ?, NULLIF(?, ''), ?, ?)`,
		event.Operation,
		event.LeaderboardID,
		event.ItemID,
		payload,
		event.OccurredAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("append mysql durable event: %w", err)
	}
	return nil
}

func (s *MySQLDurableStore) ReplayToRedis(ctx context.Context, repo *core.RedisRepository, leaderboardID string) (ReplayResult, error) {
	if repo == nil {
		return ReplayResult{}, fmt.Errorf("redis repository is required for replay")
	}

	result := ReplayResult{Scope: "all"}
	if leaderboardID != "" {
		result.Scope = leaderboardID
	}

	snapshots, err := s.fetchLeaderboardSnapshots(ctx, leaderboardID)
	if err != nil {
		return ReplayResult{}, err
	}

	existingIDs, err := repo.GetAllLeaderboardIDs(ctx)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("list redis leaderboards for replay: %w", err)
	}

	active := make(map[string]leaderboardSnapshotRow, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.IsDeleted {
			continue
		}
		active[snapshot.ID] = snapshot
	}

	if leaderboardID != "" {
		if _, ok := active[leaderboardID]; !ok {
			if err := repo.DeleteLeaderboard(ctx, leaderboardID); err != nil {
				return ReplayResult{}, fmt.Errorf("delete redis leaderboard during replay: %w", err)
			}
			core.ForgetLeaderboard(leaderboardID)
			result.RemovedLeaderboards = 1
			return result, nil
		}
	}

	for _, existingID := range existingIDs {
		if leaderboardID != "" && existingID != leaderboardID {
			continue
		}
		if _, ok := active[existingID]; ok {
			continue
		}
		if err := repo.DeleteLeaderboard(ctx, existingID); err != nil {
			return ReplayResult{}, fmt.Errorf("prune stale redis leaderboard %s: %w", existingID, err)
		}
		core.ForgetLeaderboard(existingID)
		result.RemovedLeaderboards++
	}

	for _, snapshot := range snapshots {
		if snapshot.IsDeleted {
			continue
		}
		if err := s.replayLeaderboardSnapshot(ctx, repo, snapshot, &result); err != nil {
			return ReplayResult{}, err
		}
	}

	return result, nil
}

func (s *MySQLDurableStore) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return
	}

	s.runCtx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(2)
	go s.mergeLoop()
	go s.cleanupLoop()
}

func (s *MySQLDurableStore) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.wg.Wait()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (s *MySQLDurableStore) mergeLoop() {
	defer s.wg.Done()

	if err := s.drainMergeBatches(s.runCtx); err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("initial mysql snapshot merge failed", "error", err)
	}

	ticker := time.NewTicker(s.mergeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.runCtx.Done():
			return
		case <-ticker.C:
			if err := s.drainMergeBatches(s.runCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("mysql snapshot merge failed", "error", err)
			}
		}
	}
}

func (s *MySQLDurableStore) cleanupLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.runCtx.Done():
			return
		case <-ticker.C:
			if err := s.drainCleanupBatches(s.runCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("mysql event cleanup failed", "error", err)
			}
		}
	}
}

func (s *MySQLDurableStore) drainMergeBatches(ctx context.Context) error {
	for {
		processed, err := s.mergeBatch(ctx)
		if err != nil {
			return err
		}
		if processed < s.mergeBatchSize {
			return nil
		}
	}
}

func (s *MySQLDurableStore) mergeBatch(ctx context.Context) (int, error) {
	lastEventID, err := s.getConsumerCheckpoint(ctx, snapshotMergerConsumerName)
	if err != nil {
		return 0, err
	}

	events, err := s.fetchEventsAfter(ctx, lastEventID, s.mergeBatchSize)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin mysql snapshot merge tx: %w", err)
	}
	defer tx.Rollback()

	itemStates := make(map[itemMutationKey]*itemBatchState)
	for _, event := range events {
		if err := s.applyEvent(ctx, tx, event, itemStates); err != nil {
			return 0, err
		}
	}

	for _, state := range itemStates {
		if !state.changed {
			continue
		}
		if err := s.applyItemMutation(ctx, tx, state.mutation); err != nil {
			return 0, err
		}
	}

	maxEventID := events[len(events)-1].ID
	if err := s.upsertConsumerCheckpoint(ctx, tx, snapshotMergerConsumerName, maxEventID); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit mysql snapshot merge tx: %w", err)
	}

	return len(events), nil
}

func (s *MySQLDurableStore) drainCleanupBatches(ctx context.Context) error {
	for {
		deleted, err := s.cleanupBatch(ctx)
		if err != nil {
			return err
		}
		if deleted < s.cleanupBatchSize {
			return nil
		}
	}
}

func (s *MySQLDurableStore) cleanupBatch(ctx context.Context) (int, error) {
	cutoffID, err := s.getConsumerCheckpoint(ctx, snapshotMergerConsumerName)
	if err != nil {
		return 0, err
	}
	if cutoffID == 0 {
		return 0, nil
	}

	result, err := s.db.ExecContext(
		ctx,
		`DELETE FROM leaderboard_events
		 WHERE id <= ? AND created_at < ?
		 ORDER BY id
		 LIMIT ?`,
		cutoffID,
		time.Now().Add(-s.cleanupRetention).UTC(),
		s.cleanupBatchSize,
	)
	if err != nil {
		return 0, fmt.Errorf("cleanup mysql durable events: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read mysql durable cleanup rows affected: %w", err)
	}
	return int(rowsAffected), nil
}

func (s *MySQLDurableStore) getConsumerCheckpoint(ctx context.Context, consumer string) (int64, error) {
	var lastEventID int64
	err := s.db.QueryRowContext(
		ctx,
		`SELECT last_event_id FROM event_consumers WHERE consumer_name = ?`,
		consumer,
	).Scan(&lastEventID)
	if err == nil {
		return lastEventID, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return 0, fmt.Errorf("query mysql durable checkpoint: %w", err)
}

func (s *MySQLDurableStore) upsertConsumerCheckpoint(ctx context.Context, tx *sql.Tx, consumer string, lastEventID int64) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO event_consumers (consumer_name, last_event_id)
		 VALUES (?, ?)
		 ON DUPLICATE KEY UPDATE
		 last_event_id = GREATEST(last_event_id, VALUES(last_event_id))`,
		consumer,
		lastEventID,
	)
	if err != nil {
		return fmt.Errorf("upsert mysql durable checkpoint: %w", err)
	}
	return nil
}

func (s *MySQLDurableStore) fetchEventsAfter(ctx context.Context, lastEventID int64, limit int) ([]durableEventRow, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, op_type, leaderboard_id, item_id, payload_json, created_at
		 FROM leaderboard_events
		 WHERE id > ?
		 ORDER BY id ASC
		 LIMIT ?`,
		lastEventID,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query mysql durable events: %w", err)
	}
	defer rows.Close()

	var events []durableEventRow
	for rows.Next() {
		var row durableEventRow
		if err := rows.Scan(&row.ID, &row.Operation, &row.LeaderboardID, &row.ItemID, &row.Payload, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan mysql durable event: %w", err)
		}
		events = append(events, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate mysql durable events: %w", err)
	}
	return events, nil
}

func aggregateItemMutations(events []durableEventRow) (map[itemMutationKey]itemSnapshotMutation, error) {
	mutations := make(map[itemMutationKey]itemSnapshotMutation)

	for _, event := range events {
		switch event.Operation {
		case core.DurableOpItemUpsert:
			if !event.ItemID.Valid {
				return nil, fmt.Errorf("item upsert event %d missing item id", event.ID)
			}

			var payload core.DurableItemPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return nil, fmt.Errorf("decode item upsert event %d: %w", event.ID, err)
			}

			dataJSON, err := json.Marshal(payload.Data)
			if err != nil {
				return nil, fmt.Errorf("marshal item snapshot for event %d: %w", event.ID, err)
			}

			score := sql.NullFloat64{}
			if payload.Score != nil {
				score = sql.NullFloat64{Float64: *payload.Score, Valid: true}
			}

			key := itemMutationKey{leaderboardID: event.LeaderboardID, itemID: event.ItemID.String}
			mutations[key] = itemSnapshotMutation{
				LeaderboardID: event.LeaderboardID,
				ItemID:        event.ItemID.String,
				DataJSON:      dataJSON,
				Score:         score,
				ItemUpdatedAt: payload.UpdatedAt.UTC(),
				IsDeleted:     payload.IsDeleted,
				LastEventID:   event.ID,
			}
		case core.DurableOpItemMutate:
			if !event.ItemID.Valid {
				return nil, fmt.Errorf("item mutate event %d missing item id", event.ID)
			}

			var payload core.DurableItemMutationPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return nil, fmt.Errorf("decode item mutate event %d: %w", event.ID, err)
			}

			dataJSON, err := json.Marshal(payload.Data)
			if err != nil {
				return nil, fmt.Errorf("marshal item mutate snapshot for event %d: %w", event.ID, err)
			}

			score := sql.NullFloat64{}
			if payload.Score != nil {
				score = sql.NullFloat64{Float64: *payload.Score, Valid: true}
			}

			key := itemMutationKey{leaderboardID: event.LeaderboardID, itemID: event.ItemID.String}
			mutations[key] = itemSnapshotMutation{
				LeaderboardID: event.LeaderboardID,
				ItemID:        event.ItemID.String,
				DataJSON:      dataJSON,
				Score:         score,
				ItemUpdatedAt: payload.UpdatedAt.UTC(),
				IsDeleted:     false,
				LastEventID:   event.ID,
			}
		case core.DurableOpItemDelete:
			if !event.ItemID.Valid {
				return nil, fmt.Errorf("item delete event %d missing item id", event.ID)
			}

			key := itemMutationKey{leaderboardID: event.LeaderboardID, itemID: event.ItemID.String}
			mutations[key] = itemSnapshotMutation{
				LeaderboardID: event.LeaderboardID,
				ItemID:        event.ItemID.String,
				DataJSON:      []byte(`{}`),
				Score:         sql.NullFloat64{},
				ItemUpdatedAt: event.CreatedAt.UTC(),
				IsDeleted:     true,
				LastEventID:   event.ID,
			}
		}
	}

	return mutations, nil
}

func (s *MySQLDurableStore) fetchLeaderboardSnapshots(ctx context.Context, leaderboardID string) ([]leaderboardSnapshotRow, error) {
	query := `SELECT id, expression, schema_json, refresh_policy, cron_spec, definition_version, is_deleted, last_event_id
	          FROM leaderboards`
	args := make([]interface{}, 0, 1)
	if leaderboardID != "" {
		query += ` WHERE id = ?`
		args = append(args, leaderboardID)
	}
	query += ` ORDER BY id ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query leaderboard snapshots: %w", err)
	}
	defer rows.Close()

	var snapshots []leaderboardSnapshotRow
	for rows.Next() {
		var row leaderboardSnapshotRow
		if err := rows.Scan(
			&row.ID,
			&row.Expression,
			&row.SchemaJSON,
			&row.RefreshPolicy,
			&row.CronSpec,
			&row.DefinitionVersion,
			&row.IsDeleted,
			&row.LastEventID,
		); err != nil {
			return nil, fmt.Errorf("scan leaderboard snapshot: %w", err)
		}
		snapshots = append(snapshots, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate leaderboard snapshots: %w", err)
	}
	return snapshots, nil
}

func (s *MySQLDurableStore) fetchItemSnapshots(ctx context.Context, leaderboardID string) ([]itemSnapshotRow, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT leaderboard_id, item_id, data_json, score, item_updated_at, is_deleted, last_event_id
		 FROM leaderboard_items
		 WHERE leaderboard_id = ?
		 ORDER BY item_id ASC`,
		leaderboardID,
	)
	if err != nil {
		return nil, fmt.Errorf("query item snapshots for %s: %w", leaderboardID, err)
	}
	defer rows.Close()

	var snapshots []itemSnapshotRow
	for rows.Next() {
		var row itemSnapshotRow
		if err := rows.Scan(
			&row.LeaderboardID,
			&row.ItemID,
			&row.DataJSON,
			&row.Score,
			&row.ItemUpdatedAt,
			&row.IsDeleted,
			&row.LastEventID,
		); err != nil {
			return nil, fmt.Errorf("scan item snapshot for %s: %w", leaderboardID, err)
		}
		snapshots = append(snapshots, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate item snapshots for %s: %w", leaderboardID, err)
	}
	return snapshots, nil
}

func (s *MySQLDurableStore) replayLeaderboardSnapshot(ctx context.Context, repo *core.RedisRepository, snapshot leaderboardSnapshotRow, result *ReplayResult) error {
	if err := repo.DeleteLeaderboard(ctx, snapshot.ID); err != nil {
		return fmt.Errorf("reset redis leaderboard %s before replay: %w", snapshot.ID, err)
	}

	metadata := map[string]string{
		"expression":         snapshot.Expression,
		"schema":             string(snapshot.SchemaJSON),
		"refresh_policy":     snapshot.RefreshPolicy,
		"cron_spec":          snapshot.CronSpec.String,
		"last_recomputed_at": "",
	}
	if err := repo.SaveMetadata(ctx, snapshot.ID, metadata); err != nil {
		return fmt.Errorf("restore redis metadata for %s: %w", snapshot.ID, err)
	}

	if snapshot.RefreshPolicy == core.RefreshPolicyScheduled && snapshot.CronSpec.Valid && snapshot.CronSpec.String != "" {
		if err := repo.AddScheduledLeaderboard(ctx, snapshot.ID, core.DetermineTier(snapshot.CronSpec.String)); err != nil {
			return fmt.Errorf("restore redis schedule index for %s: %w", snapshot.ID, err)
		}
	} else {
		if err := repo.RemoveScheduledLeaderboard(ctx, snapshot.ID); err != nil {
			return fmt.Errorf("clear redis schedule index for %s: %w", snapshot.ID, err)
		}
	}

	items, err := s.fetchItemSnapshots(ctx, snapshot.ID)
	if err != nil {
		return err
	}

	for _, item := range items {
		if item.IsDeleted {
			continue
		}

		var data map[string]interface{}
		if err := json.Unmarshal(item.DataJSON, &data); err != nil {
			return fmt.Errorf("decode item snapshot %s/%s: %w", snapshot.ID, item.ItemID, err)
		}

		if snapshot.RefreshPolicy == core.RefreshPolicyScheduled {
			if err := repo.SaveItemData(ctx, snapshot.ID, item.ItemID, data, item.ItemUpdatedAt); err != nil {
				return fmt.Errorf("restore scheduled item data %s/%s: %w", snapshot.ID, item.ItemID, err)
			}
			if err := repo.MarkItemDirty(ctx, snapshot.ID, item.ItemID, true); err != nil {
				return fmt.Errorf("mark scheduled item dirty %s/%s: %w", snapshot.ID, item.ItemID, err)
			}
		} else {
			score := 0.0
			if item.Score.Valid {
				score = item.Score.Float64
			}
			if err := repo.UpsertItem(ctx, snapshot.ID, item.ItemID, score, data, item.ItemUpdatedAt); err != nil {
				return fmt.Errorf("restore realtime item %s/%s: %w", snapshot.ID, item.ItemID, err)
			}
		}
		result.ReplayedItems++
	}

	core.ForgetLeaderboard(snapshot.ID)
	result.ReplayedLeaderboards++
	return nil
}

func (s *MySQLDurableStore) applyLeaderboardEvent(ctx context.Context, tx *sql.Tx, event durableEventRow) error {
	switch event.Operation {
	case core.DurableOpLeaderboardUpsert:
		var payload core.DurableLeaderboardPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode leaderboard upsert event %d: %w", event.ID, err)
		}

		schemaJSON, err := json.Marshal(payload.Schema)
		if err != nil {
			return fmt.Errorf("marshal leaderboard snapshot for event %d: %w", event.ID, err)
		}

		_, err = tx.ExecContext(
			ctx,
			`INSERT INTO leaderboards
			 (id, expression, schema_json, refresh_policy, cron_spec, definition_version, is_deleted, last_event_id)
			 VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, 0, ?)
			 ON DUPLICATE KEY UPDATE
			 expression = IF(VALUES(last_event_id) >= last_event_id, VALUES(expression), expression),
			 schema_json = IF(VALUES(last_event_id) >= last_event_id, VALUES(schema_json), schema_json),
			 refresh_policy = IF(VALUES(last_event_id) >= last_event_id, VALUES(refresh_policy), refresh_policy),
			 cron_spec = IF(VALUES(last_event_id) >= last_event_id, VALUES(cron_spec), cron_spec),
			 definition_version = IF(VALUES(last_event_id) >= last_event_id, VALUES(definition_version), definition_version),
			 is_deleted = IF(VALUES(last_event_id) >= last_event_id, VALUES(is_deleted), is_deleted),
			 last_event_id = GREATEST(last_event_id, VALUES(last_event_id))`,
			payload.ID,
			payload.Expression,
			schemaJSON,
			payload.RefreshPolicy,
			payload.CronSpec,
			payload.DefinitionVersion,
			event.ID,
		)
		if err != nil {
			return fmt.Errorf("upsert leaderboard snapshot from event %d: %w", event.ID, err)
		}
	case core.DurableOpLeaderboardDelete:
		_, err := tx.ExecContext(
			ctx,
			`INSERT INTO leaderboards
			 (id, expression, schema_json, refresh_policy, cron_spec, definition_version, is_deleted, last_event_id)
			 VALUES (?, '', JSON_OBJECT(), ?, NULL, 1, 1, ?)
			 ON DUPLICATE KEY UPDATE
			 is_deleted = IF(VALUES(last_event_id) >= last_event_id, VALUES(is_deleted), is_deleted),
			 last_event_id = GREATEST(last_event_id, VALUES(last_event_id))`,
			event.LeaderboardID,
			core.RefreshPolicyRealtime,
			event.ID,
		)
		if err != nil {
			return fmt.Errorf("delete leaderboard snapshot from event %d: %w", event.ID, err)
		}
	}

	return nil
}

func (s *MySQLDurableStore) applyEvent(ctx context.Context, tx *sql.Tx, event durableEventRow, itemStates map[itemMutationKey]*itemBatchState) error {
	switch event.Operation {
	case core.DurableOpLeaderboardUpsert, core.DurableOpLeaderboardDelete:
		return s.applyLeaderboardEvent(ctx, tx, event)
	case core.DurableOpItemUpsert, core.DurableOpItemDelete, core.DurableOpItemMutate:
		return s.applyItemEvent(ctx, tx, event, itemStates)
	default:
		return nil
	}
}

func (s *MySQLDurableStore) applyItemEvent(ctx context.Context, tx *sql.Tx, event durableEventRow, itemStates map[itemMutationKey]*itemBatchState) error {
	if !event.ItemID.Valid {
		return fmt.Errorf("item event %d missing item id", event.ID)
	}

	key := itemMutationKey{leaderboardID: event.LeaderboardID, itemID: event.ItemID.String}
	state, err := s.loadItemBatchState(ctx, tx, key, itemStates)
	if err != nil {
		return err
	}

	switch event.Operation {
	case core.DurableOpItemUpsert:
		var payload core.DurableItemPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode item upsert event %d: %w", event.ID, err)
		}
		dataJSON, err := json.Marshal(payload.Data)
		if err != nil {
			return fmt.Errorf("marshal item snapshot for event %d: %w", event.ID, err)
		}
		score := sql.NullFloat64{}
		if payload.Score != nil {
			score = sql.NullFloat64{Float64: *payload.Score, Valid: true}
		}
		state.mutation = itemSnapshotMutation{
			LeaderboardID: event.LeaderboardID,
			ItemID:        event.ItemID.String,
			DataJSON:      dataJSON,
			Score:         score,
			ItemUpdatedAt: payload.UpdatedAt.UTC(),
			IsDeleted:     payload.IsDeleted,
			LastEventID:   event.ID,
		}
		state.loaded = true
		state.exists = true
		state.changed = true
	case core.DurableOpItemDelete:
		state.mutation = itemSnapshotMutation{
			LeaderboardID: event.LeaderboardID,
			ItemID:        event.ItemID.String,
			DataJSON:      []byte(`{}`),
			Score:         sql.NullFloat64{},
			ItemUpdatedAt: event.CreatedAt.UTC(),
			IsDeleted:     true,
			LastEventID:   event.ID,
		}
		state.loaded = true
		state.exists = true
		state.changed = true
	case core.DurableOpItemMutate:
		var payload core.DurableItemMutationPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode item mutate event %d: %w", event.ID, err)
		}
		if !state.exists || state.mutation.IsDeleted || !state.mutation.ItemUpdatedAt.Equal(payload.ExpectedUpdatedAt.UTC()) {
			return nil
		}
		dataJSON, err := json.Marshal(payload.Data)
		if err != nil {
			return fmt.Errorf("marshal item mutate snapshot for event %d: %w", event.ID, err)
		}
		score := sql.NullFloat64{}
		if payload.Score != nil {
			score = sql.NullFloat64{Float64: *payload.Score, Valid: true}
		}
		state.mutation = itemSnapshotMutation{
			LeaderboardID: event.LeaderboardID,
			ItemID:        event.ItemID.String,
			DataJSON:      dataJSON,
			Score:         score,
			ItemUpdatedAt: payload.UpdatedAt.UTC(),
			IsDeleted:     false,
			LastEventID:   event.ID,
		}
		state.loaded = true
		state.exists = true
		state.changed = true
	}

	return nil
}

func (s *MySQLDurableStore) loadItemBatchState(ctx context.Context, tx *sql.Tx, key itemMutationKey, itemStates map[itemMutationKey]*itemBatchState) (*itemBatchState, error) {
	if state, ok := itemStates[key]; ok {
		return state, nil
	}

	row, found, err := s.fetchItemSnapshotInTx(ctx, tx, key.leaderboardID, key.itemID)
	if err != nil {
		return nil, err
	}

	state := &itemBatchState{loaded: true, exists: found}
	if found {
		state.mutation = itemSnapshotMutation{
			LeaderboardID: row.LeaderboardID,
			ItemID:        row.ItemID,
			DataJSON:      row.DataJSON,
			Score:         row.Score,
			ItemUpdatedAt: row.ItemUpdatedAt,
			IsDeleted:     row.IsDeleted,
			LastEventID:   row.LastEventID,
		}
	}
	itemStates[key] = state
	return state, nil
}

func (s *MySQLDurableStore) fetchItemSnapshotInTx(ctx context.Context, tx *sql.Tx, leaderboardID string, itemID string) (itemSnapshotRow, bool, error) {
	var row itemSnapshotRow
	err := tx.QueryRowContext(
		ctx,
		`SELECT leaderboard_id, item_id, data_json, score, item_updated_at, is_deleted, last_event_id
		 FROM leaderboard_items
		 WHERE leaderboard_id = ? AND item_id = ?`,
		leaderboardID,
		itemID,
	).Scan(
		&row.LeaderboardID,
		&row.ItemID,
		&row.DataJSON,
		&row.Score,
		&row.ItemUpdatedAt,
		&row.IsDeleted,
		&row.LastEventID,
	)
	if err == nil {
		return row, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return itemSnapshotRow{}, false, nil
	}
	return itemSnapshotRow{}, false, fmt.Errorf("query item snapshot for %s/%s in tx: %w", leaderboardID, itemID, err)
}

func (s *MySQLDurableStore) applyItemMutation(ctx context.Context, tx *sql.Tx, mutation itemSnapshotMutation) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO leaderboard_items
		 (leaderboard_id, item_id, data_json, score, item_updated_at, is_deleted, last_event_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		 data_json = IF(VALUES(last_event_id) >= last_event_id, VALUES(data_json), data_json),
		 score = IF(VALUES(last_event_id) >= last_event_id, VALUES(score), score),
		 item_updated_at = IF(VALUES(last_event_id) >= last_event_id, VALUES(item_updated_at), item_updated_at),
		 is_deleted = IF(VALUES(last_event_id) >= last_event_id, VALUES(is_deleted), is_deleted),
		 last_event_id = GREATEST(last_event_id, VALUES(last_event_id))`,
		mutation.LeaderboardID,
		mutation.ItemID,
		mutation.DataJSON,
		mutation.Score,
		mutation.ItemUpdatedAt,
		mutation.IsDeleted,
		mutation.LastEventID,
	)
	if err != nil {
		return fmt.Errorf("upsert item snapshot for %s/%s: %w", mutation.LeaderboardID, mutation.ItemID, err)
	}
	return nil
}
