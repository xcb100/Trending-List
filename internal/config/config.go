package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	BusinessAddr               string
	InternalAddr               string
	InternalAPIToken           string
	ShutdownTimeout            time.Duration
	SchedulerEnabled           bool
	HealthcheckTimeout         time.Duration
	RedisAddr                  string
	RedisPassword              string
	RedisDB                    int
	RedisDialTimeout           time.Duration
	RedisReadTimeout           time.Duration
	RedisWriteTimeout          time.Duration
	RedisPoolTimeout           time.Duration
	HTTPReadHeaderTimeout      time.Duration
	HTTPReadTimeout            time.Duration
	HTTPWriteTimeout           time.Duration
	HTTPIdleTimeout            time.Duration
	RedisRepositoryTimeout     time.Duration
	ScheduledTaskTimeout       time.Duration
	ScheduledTaskLockTTL       time.Duration
	LeaderboardCreateLockTTL   time.Duration
	MySQLDSN                   string
	MySQLMaxOpenConns          int
	MySQLMaxIdleConns          int
	MySQLConnMaxLifetime       time.Duration
	MySQLEventMergeInterval    time.Duration
	MySQLEventMergeBatchSize   int
	MySQLEventCleanupInterval  time.Duration
	MySQLEventCleanupRetention time.Duration
	MySQLEventCleanupBatchSize int
}

func Load() (Config, error) {
	// 这里集中收敛所有环境变量，保证运行参数只有一个装配入口，
	// 便于本地、容器和 K8s 环境保持一致。
	shutdownTimeout, err := getenvDuration("SHUTDOWN_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse SHUTDOWN_TIMEOUT: %w", err)
	}
	schedulerEnabled, err := getenvBool("SCHEDULER_ENABLED", true)
	if err != nil {
		return Config{}, fmt.Errorf("parse SCHEDULER_ENABLED: %w", err)
	}
	healthcheckTimeout, err := getenvDuration("HEALTHCHECK_TIMEOUT", 1*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse HEALTHCHECK_TIMEOUT: %w", err)
	}
	redisDialTimeout, err := getenvDuration("REDIS_DIAL_TIMEOUT", 1*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse REDIS_DIAL_TIMEOUT: %w", err)
	}
	redisReadTimeout, err := getenvDuration("REDIS_READ_TIMEOUT", 1*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse REDIS_READ_TIMEOUT: %w", err)
	}
	redisWriteTimeout, err := getenvDuration("REDIS_WRITE_TIMEOUT", 1*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse REDIS_WRITE_TIMEOUT: %w", err)
	}
	redisPoolTimeout, err := getenvDuration("REDIS_POOL_TIMEOUT", 2*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse REDIS_POOL_TIMEOUT: %w", err)
	}
	httpReadHeaderTimeout, err := getenvDuration("HTTP_READ_HEADER_TIMEOUT", 2*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse HTTP_READ_HEADER_TIMEOUT: %w", err)
	}
	httpReadTimeout, err := getenvDuration("HTTP_READ_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse HTTP_READ_TIMEOUT: %w", err)
	}
	httpWriteTimeout, err := getenvDuration("HTTP_WRITE_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse HTTP_WRITE_TIMEOUT: %w", err)
	}
	httpIdleTimeout, err := getenvDuration("HTTP_IDLE_TIMEOUT", 60*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse HTTP_IDLE_TIMEOUT: %w", err)
	}
	redisRepositoryTimeout, err := getenvDuration("REDIS_REPOSITORY_TIMEOUT", 800*time.Millisecond)
	if err != nil {
		return Config{}, fmt.Errorf("parse REDIS_REPOSITORY_TIMEOUT: %w", err)
	}
	scheduledTaskTimeout, err := getenvDuration("SCHEDULED_TASK_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse SCHEDULED_TASK_TIMEOUT: %w", err)
	}
	createLockTTL, err := getenvDuration("LEADERBOARD_CREATE_LOCK_TTL", 5*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse LEADERBOARD_CREATE_LOCK_TTL: %w", err)
	}
	mysqlConnMaxLifetime, err := getenvDuration("MYSQL_CONN_MAX_LIFETIME", 30*time.Minute)
	if err != nil {
		return Config{}, fmt.Errorf("parse MYSQL_CONN_MAX_LIFETIME: %w", err)
	}
	mysqlEventMergeInterval, err := getenvDuration("MYSQL_EVENT_MERGE_INTERVAL", time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse MYSQL_EVENT_MERGE_INTERVAL: %w", err)
	}
	mysqlEventCleanupInterval, err := getenvDuration("MYSQL_EVENT_CLEANUP_INTERVAL", time.Minute)
	if err != nil {
		return Config{}, fmt.Errorf("parse MYSQL_EVENT_CLEANUP_INTERVAL: %w", err)
	}
	mysqlEventCleanupRetention, err := getenvDuration("MYSQL_EVENT_CLEANUP_RETENTION", 24*time.Hour)
	if err != nil {
		return Config{}, fmt.Errorf("parse MYSQL_EVENT_CLEANUP_RETENTION: %w", err)
	}
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		return Config{}, fmt.Errorf("MYSQL_DSN is required")
	}

	cfg := Config{
		BusinessAddr:               getenv("BUSINESS_ADDR", ":8080"),
		InternalAddr:               getenv("INTERNAL_ADDR", ":9090"),
		InternalAPIToken:           os.Getenv("INTERNAL_API_TOKEN"),
		ShutdownTimeout:            shutdownTimeout,
		SchedulerEnabled:           schedulerEnabled,
		HealthcheckTimeout:         healthcheckTimeout,
		RedisAddr:                  getenv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:              os.Getenv("REDIS_PASSWORD"),
		RedisDialTimeout:           redisDialTimeout,
		RedisReadTimeout:           redisReadTimeout,
		RedisWriteTimeout:          redisWriteTimeout,
		RedisPoolTimeout:           redisPoolTimeout,
		HTTPReadHeaderTimeout:      httpReadHeaderTimeout,
		HTTPReadTimeout:            httpReadTimeout,
		HTTPWriteTimeout:           httpWriteTimeout,
		HTTPIdleTimeout:            httpIdleTimeout,
		RedisRepositoryTimeout:     redisRepositoryTimeout,
		ScheduledTaskTimeout:       scheduledTaskTimeout,
		LeaderboardCreateLockTTL:   createLockTTL,
		MySQLDSN:                   mysqlDSN,
		MySQLConnMaxLifetime:       mysqlConnMaxLifetime,
		MySQLEventMergeInterval:    mysqlEventMergeInterval,
		MySQLEventCleanupInterval:  mysqlEventCleanupInterval,
		MySQLEventCleanupRetention: mysqlEventCleanupRetention,
	}
	scheduledTaskLockTTL, err := getenvDuration("SCHEDULED_TASK_LOCK_TTL", cfg.ScheduledTaskTimeout+5*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("parse SCHEDULED_TASK_LOCK_TTL: %w", err)
	}
	cfg.ScheduledTaskLockTTL = scheduledTaskLockTTL

	// 对需要强类型校验的配置单独解析，避免静默写入无效值。
	db, err := getenvInt("REDIS_DB", 0)
	if err != nil {
		return Config{}, fmt.Errorf("parse REDIS_DB: %w", err)
	}
	cfg.RedisDB = db

	mysqlMaxOpenConns, err := getenvInt("MYSQL_MAX_OPEN_CONNS", 20)
	if err != nil {
		return Config{}, fmt.Errorf("parse MYSQL_MAX_OPEN_CONNS: %w", err)
	}
	cfg.MySQLMaxOpenConns = mysqlMaxOpenConns

	mysqlMaxIdleConns, err := getenvInt("MYSQL_MAX_IDLE_CONNS", 5)
	if err != nil {
		return Config{}, fmt.Errorf("parse MYSQL_MAX_IDLE_CONNS: %w", err)
	}
	cfg.MySQLMaxIdleConns = mysqlMaxIdleConns

	mysqlEventMergeBatchSize, err := getenvInt("MYSQL_EVENT_MERGE_BATCH_SIZE", 500)
	if err != nil {
		return Config{}, fmt.Errorf("parse MYSQL_EVENT_MERGE_BATCH_SIZE: %w", err)
	}
	cfg.MySQLEventMergeBatchSize = mysqlEventMergeBatchSize

	mysqlEventCleanupBatchSize, err := getenvInt("MYSQL_EVENT_CLEANUP_BATCH_SIZE", 1000)
	if err != nil {
		return Config{}, fmt.Errorf("parse MYSQL_EVENT_CLEANUP_BATCH_SIZE: %w", err)
	}
	cfg.MySQLEventCleanupBatchSize = mysqlEventCleanupBatchSize

	return cfg, nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func getenvBool(key string, fallback bool) (bool, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, err
	}
	return parsed, nil
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
