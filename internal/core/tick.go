package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	scheduleCache sync.Map
	defaultParser = cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
)

func getParsedSchedule(spec string, parser cron.Parser) (cron.Schedule, error) {
	if val, ok := scheduleCache.Load(spec); ok {
		return val.(cron.Schedule), nil
	}

	schedule, err := parser.Parse(spec)
	if err == nil {
		scheduleCache.Store(spec, schedule)
	}
	return schedule, err
}

func ValidateCronSpec(spec string) error {
	if strings.TrimSpace(spec) == "" {
		return fmt.Errorf("cron_spec is required when refresh_policy is scheduled")
	}
	if _, err := getParsedSchedule(spec, defaultParser); err != nil {
		return fmt.Errorf("invalid cron_spec: %w", err)
	}
	return nil
}

func DetermineTier(spec string) string {
	schedule, err := getParsedSchedule(spec, defaultParser)
	if err != nil {
		return Tier5s
	}

	now := time.Now()
	next1 := schedule.Next(now)
	next2 := schedule.Next(next1)
	diff := next2.Sub(next1)

	// 把 cron 规格归并到少量粗粒度分层，可以避免每秒扫描所有定时榜，
	// 同时又保持实现足够简单。
	switch {
	case diff >= 6*time.Hour:
		return Tier6h
	case diff >= 30*time.Minute:
		return Tier30m
	case diff >= time.Minute:
		return Tier1m
	default:
		return Tier5s
	}
}

func StartCronScheduler(ctx context.Context) {
	go startTierLoop(ctx, Tier5s, 5*time.Second)
	go startTierLoop(ctx, Tier1m, time.Minute)
	go startTierLoop(ctx, Tier30m, 30*time.Minute)
	go startTierLoop(ctx, Tier6h, 6*time.Hour)
}

func startTierLoop(ctx context.Context, tier string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = ProcessCronTick(ctx, tier)
		}
	}
}

func ProcessCronTick(ctx context.Context, tier string) error {
	if DefaultRepo == nil {
		recordSchedulerTick(tier, "error")
		return fmt.Errorf("default repository is not configured")
	}

	ids, err := DefaultRepo.GetScheduledLeaderboardIDs(ctx, tier)
	if err != nil {
		recordSchedulerTick(tier, "error")
		return err
	}

	hadFailure := false
	for _, id := range ids {
		lb, err := GetLeaderboard(ctx, id)
		if err != nil {
			hadFailure = true
			continue
		}
		state := lb.State()
		if state.RefreshPolicy != RefreshPolicyScheduled || state.CronSpec == "" {
			continue
		}

		schedule, err := getParsedSchedule(state.CronSpec, defaultParser)
		if err != nil {
			hadFailure = true
			coreLogger.Error("failed to parse cron spec", "leaderboard_id", id, "cron_spec", state.CronSpec, "error", err)
			continue
		}

		if state.LastRecomputedAt.IsZero() {
			if err := runScheduledRecompute(ctx, lb); err != nil {
				hadFailure = true
				coreLogger.Error("init cron recompute failed", "leaderboard_id", id, "error", err)
			}
			continue
		}

		nextTime := schedule.Next(state.LastRecomputedAt)
		if time.Now().Before(nextTime) {
			continue
		}

		coreLogger.Info("internal cron trigger recompute starts", "leaderboard_id", id)
		if err := runScheduledRecompute(ctx, lb); err != nil {
			hadFailure = true
			coreLogger.Error("cron recompute failed", "leaderboard_id", id, "error", err)
		}
	}

	if hadFailure {
		// 从调用方视角看，这次 tick 仍然可能处理成功了一部分排行榜，
		// 因此这里不直接把整次 tick 记为失败，而是通过指标和日志暴露部分异常。
		recordSchedulerTick(tier, "partial_error")
		return nil
	}
	recordSchedulerTick(tier, "success")
	return nil
}

func runScheduledRecompute(ctx context.Context, lb *Leaderboard) error {
	recomputeCtx, cancel := WithScheduledTaskTimeout(ctx)
	defer cancel()
	return lb.recompute(recomputeCtx, "scheduled", false)
}
