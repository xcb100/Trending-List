package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

var scheduleCache sync.Map // 用于缓存解析好的 cron 语法，极大降低高频心跳下的 CPU 解析开销
var defaultParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func getParsedSchedule(spec string, parser cron.Parser) (cron.Schedule, error) {
	if val, ok := scheduleCache.Load(spec); ok {
		return val.(cron.Schedule), nil
	}
	s, err := parser.Parse(spec)
	if err == nil {
		scheduleCache.Store(spec, s)
	}
	return s, err
}

// DetermineTier 计算 cron 表达式的触发间隔，并将其降级归入相应的定时分级队列。
func DetermineTier(spec string) string {
	schedule, err := getParsedSchedule(spec, defaultParser)
	if err != nil {
		return Tier5s // 语法解析失败则扔入最高频队列兜底，交由 ProcessCronTick 进一步处理或报错
	}

	now := time.Now()
	next1 := schedule.Next(now)
	next2 := schedule.Next(next1)
	diff := next2.Sub(next1)

	if diff >= 6*time.Hour {
		return Tier6h
	} else if diff >= 30*time.Minute {
		return Tier30m
	} else if diff >= time.Minute {
		return Tier1m
	}
	return Tier5s
}

// StartCronScheduler 启动内置的分布式 Cron 调度器，适用于高精度 (例如 15s) 场景
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

// ProcessCronTick 接收统一定时器的心跳并根据分布式锁处理 recompute
func ProcessCronTick(ctx context.Context, tier string) error {
	// ⚠️ 核心性能优化：不再执行 O(N) 的全局全套榜单大扫表，而是从对应梯队的调度大盘 Subset 中拉走只需处理的项
	ids, err := DefaultRepo.GetScheduledLeaderboardIDs(ctx, tier)
	if err != nil {
		return err
	}

	for _, id := range ids {
		lb, err := GetLeaderboard(ctx, id)
		if err != nil {
			continue
		}

		if lb.RefreshPolicy != RefreshPolicyScheduled || lb.CronSpec == "" {
			continue
		}

		spec := lb.CronSpec
		// 容错处理部分外部语法
		if strings.HasPrefix(spec, "@every") {
			// robfig/cron 能够识别原生的 @every
		}

		schedule, err := getParsedSchedule(spec, defaultParser)
		if err != nil {
			coreLogger.Error("failed to parse cron spec", "leaderboard_id", id, "cron_spec", spec, "error", err)
			continue
		}

		last := lb.LastRecomputedAt
		if last.IsZero() {
			// 之前从未执行过，立即执行一次 (并用分布式锁保护)
			lockKey := fmt.Sprintf("lb:%s:cron_lock", id)
			ok, _ := DefaultRepo.AcquireLock(ctx, lockKey, 30*time.Second)
			if ok {
				if err := lb.Recompute(ctx); err != nil {
					coreLogger.Error("init cron recompute failed", "leaderboard_id", id, "error", err)
				}
			}
			continue
		}

		// 通过排期表计算下一次应该触发执行的时间
		nextTime := schedule.Next(last)

		// 如果现在的时间已经超出或等于应该执行的时间，那就执行重算
		if time.Now().After(nextTime) || time.Now().Equal(nextTime) {
			lockKey := fmt.Sprintf("lb:%s:cron_lock", id)

			// SETNX 争抢分布式锁。抢到的节点负责本轮排期重算任务；
			// 我们给它配一个 30秒 的 TTL 自动过期，避免服务宕机死锁。
			ok, _ := DefaultRepo.AcquireLock(ctx, lockKey, 30*time.Second)
			if !ok {
				// 获取锁失败，说明别的节点（Pod副本）已经接管了本次运算，本节点跳过
				continue
			}

			coreLogger.Info("internal cron trigger recompute starts", "leaderboard_id", id)
			if err := lb.Recompute(ctx); err != nil {
				coreLogger.Error("cron recompute failed", "leaderboard_id", id, "error", err)
			}
			// 运算完毕后锁自动过期或被下一次迭代覆盖，保护机制完成
		}
	}
	return nil
}
