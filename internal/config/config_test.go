package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadParsesTypedConfig(t *testing.T) {
	t.Setenv("BUSINESS_ADDR", ":18080")
	t.Setenv("INTERNAL_ADDR", ":19090")
	t.Setenv("INTERNAL_API_TOKEN", "demo-token")
	t.Setenv("SHUTDOWN_TIMEOUT", "7s")
	t.Setenv("SCHEDULER_ENABLED", "false")
	t.Setenv("HEALTHCHECK_TIMEOUT", "1500ms")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_DB", "3")
	t.Setenv("REDIS_DIAL_TIMEOUT", "1500ms")
	t.Setenv("REDIS_READ_TIMEOUT", "2s")
	t.Setenv("REDIS_WRITE_TIMEOUT", "2500ms")
	t.Setenv("REDIS_POOL_TIMEOUT", "3s")
	t.Setenv("HTTP_READ_HEADER_TIMEOUT", "3s")
	t.Setenv("HTTP_READ_TIMEOUT", "6s")
	t.Setenv("HTTP_WRITE_TIMEOUT", "11s")
	t.Setenv("HTTP_IDLE_TIMEOUT", "70s")
	t.Setenv("REDIS_REPOSITORY_TIMEOUT", "900ms")
	t.Setenv("SCHEDULED_TASK_TIMEOUT", "8s")
	t.Setenv("SCHEDULED_TASK_LOCK_TTL", "12s")
	t.Setenv("LEADERBOARD_CREATE_LOCK_TTL", "6s")
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/trending?parseTime=true")
	t.Setenv("MYSQL_MAX_OPEN_CONNS", "30")
	t.Setenv("MYSQL_MAX_IDLE_CONNS", "8")
	t.Setenv("MYSQL_CONN_MAX_LIFETIME", "45m")
	t.Setenv("MYSQL_EVENT_MERGE_INTERVAL", "2s")
	t.Setenv("MYSQL_EVENT_MERGE_BATCH_SIZE", "700")
	t.Setenv("MYSQL_EVENT_CLEANUP_INTERVAL", "2m")
	t.Setenv("MYSQL_EVENT_CLEANUP_RETENTION", "48h")
	t.Setenv("MYSQL_EVENT_CLEANUP_BATCH_SIZE", "1500")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned unexpected error: %v", err)
	}

	if cfg.BusinessAddr != ":18080" {
		t.Fatalf("expected business addr to be overridden, got %s", cfg.BusinessAddr)
	}
	if cfg.InternalAddr != ":19090" {
		t.Fatalf("expected internal addr to be overridden, got %s", cfg.InternalAddr)
	}
	if cfg.InternalAPIToken != "demo-token" {
		t.Fatalf("expected internal token to be loaded, got %q", cfg.InternalAPIToken)
	}
	if cfg.RedisDB != 3 {
		t.Fatalf("expected redis db 3, got %d", cfg.RedisDB)
	}
	if cfg.SchedulerEnabled {
		t.Fatal("expected scheduler to be disabled")
	}
	if cfg.ShutdownTimeout != 7*time.Second {
		t.Fatalf("expected shutdown timeout 7s, got %s", cfg.ShutdownTimeout)
	}
	if cfg.HealthcheckTimeout != 1500*time.Millisecond {
		t.Fatalf("expected healthcheck timeout 1500ms, got %s", cfg.HealthcheckTimeout)
	}
	if cfg.ScheduledTaskLockTTL != 12*time.Second {
		t.Fatalf("expected task lock ttl 12s, got %s", cfg.ScheduledTaskLockTTL)
	}
	if cfg.MySQLDSN == "" {
		t.Fatal("expected mysql dsn to be loaded")
	}
	if cfg.MySQLMaxOpenConns != 30 || cfg.MySQLMaxIdleConns != 8 {
		t.Fatalf("expected mysql pool sizing to be parsed, got open=%d idle=%d", cfg.MySQLMaxOpenConns, cfg.MySQLMaxIdleConns)
	}
	if cfg.MySQLConnMaxLifetime != 45*time.Minute {
		t.Fatalf("expected mysql conn max lifetime 45m, got %s", cfg.MySQLConnMaxLifetime)
	}
	if cfg.MySQLEventMergeInterval != 2*time.Second || cfg.MySQLEventMergeBatchSize != 700 {
		t.Fatalf("expected mysql merge settings to be parsed, got interval=%s batch=%d", cfg.MySQLEventMergeInterval, cfg.MySQLEventMergeBatchSize)
	}
	if cfg.MySQLEventCleanupInterval != 2*time.Minute || cfg.MySQLEventCleanupRetention != 48*time.Hour || cfg.MySQLEventCleanupBatchSize != 1500 {
		t.Fatalf(
			"expected mysql cleanup settings to be parsed, got interval=%s retention=%s batch=%d",
			cfg.MySQLEventCleanupInterval,
			cfg.MySQLEventCleanupRetention,
			cfg.MySQLEventCleanupBatchSize,
		)
	}
}

func TestLoadRejectsInvalidTypedConfig(t *testing.T) {
	t.Setenv("MYSQL_DSN", "user:pass@tcp(mysql:3306)/trending?parseTime=true")
	t.Setenv("HTTP_READ_TIMEOUT", "definitely-not-a-duration")

	_, err := Load()
	if err == nil {
		t.Fatal("expected Load() to reject invalid duration")
	}
	if !strings.Contains(err.Error(), "HTTP_READ_TIMEOUT") {
		t.Fatalf("expected error to reference invalid key, got %v", err)
	}
}

func TestLoadRequiresMySQLDSN(t *testing.T) {
	_, err := Load()
	if err == nil {
		t.Fatal("expected Load() to reject empty MYSQL_DSN")
	}
	if !strings.Contains(err.Error(), "MYSQL_DSN") {
		t.Fatalf("expected error to mention MYSQL_DSN, got %v", err)
	}
}
