package api

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewBusinessMux(readinessTimeout time.Duration, readinessCheck func(context.Context) error) *http.ServeMux {
	mux := http.NewServeMux()
	// 业务端口只挂业务读写接口，便于和内部运维接口做网络层隔离。
	mux.HandleFunc("GET /livez", LivenessHandler())
	mux.HandleFunc("GET /readyz", ReadinessHandler(readinessTimeout, readinessCheck))
	mux.HandleFunc("GET /healthz", HealthHandler(readinessTimeout, readinessCheck))
	mux.HandleFunc("POST /leaderboard", MetricsMiddleware("/leaderboard", CreateLeaderboardHandler))
	mux.HandleFunc("POST /leaderboard/{id}/item", MetricsMiddleware("/leaderboard/{id}/item", UpdateItemHandler))
	mux.HandleFunc("DELETE /leaderboard/{id}/item/{item_id}", MetricsMiddleware("/leaderboard/{id}/item/{item_id}", DeleteItemHandler))
	mux.HandleFunc("GET /leaderboard/{id}", MetricsMiddleware("/leaderboard/{id}", GetLeaderboardHandler))
	mux.HandleFunc("DELETE /leaderboard/{id}", MetricsMiddleware("/leaderboard/{id}", DeleteLeaderboardHandler))
	mux.HandleFunc("POST /leaderboard/{id}/schedule", MetricsMiddleware("/leaderboard/{id}/schedule", ScheduleUpdateHandler))
	mux.HandleFunc("POST /leaderboard/{id}/recompute", MetricsMiddleware("/leaderboard/{id}/recompute", RecomputeLeaderboardHandler))
	return mux
}

func NewInternalMux(readinessTimeout time.Duration, readinessCheck func(context.Context) error, internalToken string) *http.ServeMux {
	mux := http.NewServeMux()
	// 内部端口承载健康检查、指标和调度入口，便于后续在集群内单独暴露。
	mux.HandleFunc("GET /livez", LivenessHandler())
	mux.HandleFunc("GET /readyz", ReadinessHandler(readinessTimeout, readinessCheck))
	mux.HandleFunc("GET /healthz", HealthHandler(readinessTimeout, readinessCheck))
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("POST /system/cron/tick", RequireInternalToken(internalToken, SystemCronTickHandler))
	return mux
}
