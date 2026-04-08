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
}

func TestLoadRejectsInvalidTypedConfig(t *testing.T) {
	t.Setenv("HTTP_READ_TIMEOUT", "definitely-not-a-duration")

	_, err := Load()
	if err == nil {
		t.Fatal("expected Load() to reject invalid duration")
	}
	if !strings.Contains(err.Error(), "HTTP_READ_TIMEOUT") {
		t.Fatalf("expected error to reference invalid key, got %v", err)
	}
}
