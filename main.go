package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"awesomeProject/internal/api"
	"awesomeProject/internal/core"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Redis 连接配置
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     os.Getenv("REDIS_PASSWORD"),
		DialTimeout:  1 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolTimeout:  2 * time.Second,
	})
	defer rdb.Close()

	redisRepo := core.NewRedisRepository(rdb)
	core.SetDefaultRepo(redisRepo)

	// ---------------- 配置业务 API 路由 (端口 8080) ----------------
	mux := http.NewServeMux()
	mux.HandleFunc("POST /leaderboard", api.MetricsMiddleware("/leaderboard", api.CreateLeaderboardHandler))
	mux.HandleFunc("POST /leaderboard/{id}/item", api.MetricsMiddleware("/leaderboard/{id}/item", api.UpdateItemHandler))
	mux.HandleFunc("GET /leaderboard/{id}", api.MetricsMiddleware("/leaderboard/{id}", api.GetLeaderboardHandler))
	mux.HandleFunc("POST /leaderboard/{id}/schedule", api.MetricsMiddleware("/leaderboard/{id}/schedule", api.ScheduleUpdateHandler))
	mux.HandleFunc("POST /leaderboard/{id}/recompute", api.MetricsMiddleware("/leaderboard/{id}/recompute", api.RecomputeLeaderboardHandler))

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// ---------------- 配置 Prometheus 监控路由 (内部端口 9090) ----------------
	// 生产环境最佳实践：将 /metrics 暴露在独立的内部端口，防止外部用户直接访问监控数据造成泄漏
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    ":9090",
		Handler: metricsMux,
	}

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Println("Business API Server listening on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Business server error:", err)
		}
	}()

	go func() {
		log.Println("Prometheus Metrics Server listening on :9090")
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Metrics server error:", err)
		}
	}()

	<-stop
	log.Println("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server.Shutdown(ctx)
	metricsServer.Shutdown(ctx)
}
