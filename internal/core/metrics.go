package core

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	leaderboardsLoaded = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "leaderboards_loaded",
			Help: "Number of leaderboard definitions currently loaded in memory.",
		},
		func() float64 {
			lbMu.RLock()
			defer lbMu.RUnlock()
			return float64(len(Leaderboards))
		},
	)

	leaderboardCreateTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leaderboard_create_total",
			Help: "Total number of leaderboard creation attempts.",
		},
		[]string{"status"},
	)

	leaderboardRestoreTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leaderboard_restore_total",
			Help: "Total number of leaderboard restore attempts from storage.",
		},
		[]string{"status"},
	)

	leaderboardUpsertTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leaderboard_upsert_total",
			Help: "Total number of leaderboard item upsert attempts.",
		},
		[]string{"refresh_policy", "status"},
	)

	leaderboardRecomputeTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leaderboard_recompute_total",
			Help: "Total number of leaderboard recompute attempts.",
		},
		[]string{"trigger", "status"},
	)

	leaderboardRecomputeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "leaderboard_recompute_duration_seconds",
			Help:    "Duration of leaderboard recompute attempts.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		},
		[]string{"trigger", "status"},
	)

	leaderboardExpressionUpdateTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leaderboard_expression_update_total",
			Help: "Total number of leaderboard expression update attempts.",
		},
		[]string{"status"},
	)

	leaderboardExpressionUpdateDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "leaderboard_expression_update_duration_seconds",
			Help:    "Duration of leaderboard expression update attempts.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		},
		[]string{"status"},
	)

	leaderboardDeleteTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leaderboard_delete_total",
			Help: "Total number of leaderboard deletion attempts.",
		},
		[]string{"status"},
	)

	itemDeleteTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leaderboard_item_delete_total",
			Help: "Total number of leaderboard item deletion attempts.",
		},
		[]string{"status"},
	)

	schedulerTickTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leaderboard_scheduler_tick_total",
			Help: "Total number of scheduler tick executions.",
		},
		[]string{"tier", "status"},
	)
)

func init() {
	// 核心指标在进程启动时统一注册，避免业务代码在热路径上重复判断或初始化。
	prometheus.MustRegister(
		leaderboardsLoaded,
		leaderboardCreateTotal,
		leaderboardRestoreTotal,
		leaderboardUpsertTotal,
		leaderboardRecomputeTotal,
		leaderboardRecomputeDuration,
		leaderboardExpressionUpdateTotal,
		leaderboardExpressionUpdateDuration,
		leaderboardDeleteTotal,
		itemDeleteTotal,
		schedulerTickTotal,
	)
}

func recordCreate(status string) {
	leaderboardCreateTotal.WithLabelValues(status).Inc()
}

func recordRestore(status string) {
	leaderboardRestoreTotal.WithLabelValues(status).Inc()
}

func recordUpsert(refreshPolicy, status string) {
	leaderboardUpsertTotal.WithLabelValues(refreshPolicy, status).Inc()
}

func recordRecompute(trigger, status string, duration time.Duration) {
	// 重算同时记录次数和耗时，便于区分“失败变多”与“变慢但仍成功”两类问题。
	leaderboardRecomputeTotal.WithLabelValues(trigger, status).Inc()
	leaderboardRecomputeDuration.WithLabelValues(trigger, status).Observe(duration.Seconds())
}

func recordExpressionUpdate(status string, duration time.Duration) {
	leaderboardExpressionUpdateTotal.WithLabelValues(status).Inc()
	leaderboardExpressionUpdateDuration.WithLabelValues(status).Observe(duration.Seconds())
}

func recordDeleteLeaderboard(status string) {
	leaderboardDeleteTotal.WithLabelValues(status).Inc()
}

func recordDeleteItem(status string) {
	itemDeleteTotal.WithLabelValues(status).Inc()
}

func recordSchedulerTick(tier, status string) {
	schedulerTickTotal.WithLabelValues(tier, status).Inc()
}
