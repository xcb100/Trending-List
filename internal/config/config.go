package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	BusinessAddr             string
	InternalAddr             string
	ShutdownTimeout          time.Duration
	SchedulerEnabled         bool
	HealthcheckTimeout       time.Duration
	RedisAddr                string
	RedisPassword            string
	RedisDB                  int
	RedisDialTimeout         time.Duration
	RedisReadTimeout         time.Duration
	RedisWriteTimeout        time.Duration
	RedisPoolTimeout         time.Duration
	HTTPReadHeaderTimeout    time.Duration
	HTTPReadTimeout          time.Duration
	HTTPWriteTimeout         time.Duration
	HTTPIdleTimeout          time.Duration
	RedisRepositoryTimeout   time.Duration
	ScheduledTaskTimeout     time.Duration
	ScheduledTaskLockTTL     time.Duration
	LeaderboardCreateLockTTL time.Duration
}

func Load() (Config, error) {
	// 这里集中收敛所有环境变量，保证运行参数只有一个装配入口，
	// 便于本地、容器和 K8s 环境保持一致。
	cfg := Config{
		BusinessAddr:             getenv("BUSINESS_ADDR", ":8080"),
		InternalAddr:             getenv("INTERNAL_ADDR", ":9090"),
		ShutdownTimeout:          getenvDuration("SHUTDOWN_TIMEOUT", 5*time.Second),
		SchedulerEnabled:         getenvBool("SCHEDULER_ENABLED", true),
		HealthcheckTimeout:       getenvDuration("HEALTHCHECK_TIMEOUT", 1*time.Second),
		RedisAddr:                getenv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:            os.Getenv("REDIS_PASSWORD"),
		RedisDialTimeout:         getenvDuration("REDIS_DIAL_TIMEOUT", 1*time.Second),
		RedisReadTimeout:         getenvDuration("REDIS_READ_TIMEOUT", 1*time.Second),
		RedisWriteTimeout:        getenvDuration("REDIS_WRITE_TIMEOUT", 1*time.Second),
		RedisPoolTimeout:         getenvDuration("REDIS_POOL_TIMEOUT", 2*time.Second),
		HTTPReadHeaderTimeout:    getenvDuration("HTTP_READ_HEADER_TIMEOUT", 2*time.Second),
		HTTPReadTimeout:          getenvDuration("HTTP_READ_TIMEOUT", 5*time.Second),
		HTTPWriteTimeout:         getenvDuration("HTTP_WRITE_TIMEOUT", 10*time.Second),
		HTTPIdleTimeout:          getenvDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		RedisRepositoryTimeout:   getenvDuration("REDIS_REPOSITORY_TIMEOUT", 800*time.Millisecond),
		ScheduledTaskTimeout:     getenvDuration("SCHEDULED_TASK_TIMEOUT", 5*time.Second),
		LeaderboardCreateLockTTL: getenvDuration("LEADERBOARD_CREATE_LOCK_TTL", 5*time.Second),
	}
	cfg.ScheduledTaskLockTTL = getenvDuration("SCHEDULED_TASK_LOCK_TTL", cfg.ScheduledTaskTimeout+5*time.Second)

	// 对需要强类型校验的配置单独解析，避免静默写入无效值。
	db, err := getenvInt("REDIS_DB", 0)
	if err != nil {
		return Config{}, fmt.Errorf("parse REDIS_DB: %w", err)
	}
	cfg.RedisDB = db

	return cfg, nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		// 时长格式非法时回退到默认值，保证服务仍然可启动。
		return fallback
	}
	return parsed
}

func getenvBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		// 布尔值非法时回退到默认值，避免因为单个配置拼写错误直接失败。
		return fallback
	}
	return parsed
}

func getenvInt(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}
