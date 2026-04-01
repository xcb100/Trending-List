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

	mux := http.NewServeMux()
	mux.HandleFunc("POST /leaderboard", api.CreateLeaderboardHandler)
	mux.HandleFunc("POST /leaderboard/{id}/item", api.UpdateItemHandler)
	mux.HandleFunc("GET /leaderboard/{id}", api.GetLeaderboardHandler)
	mux.HandleFunc("POST /leaderboard/{id}/schedule", api.ScheduleUpdateHandler)
	mux.HandleFunc("POST /leaderboard/{id}/recompute", api.RecomputeLeaderboardHandler)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Println("Server listening on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-stop
	log.Println("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server.Shutdown(ctx)
}
