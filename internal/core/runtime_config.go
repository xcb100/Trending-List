package core

import "time"

type RuntimeConfig struct {
	RedisRepositoryTimeout time.Duration
	ScheduledTaskTimeout   time.Duration
	ScheduledTaskLockTTL   time.Duration
	CreateLockTTL          time.Duration
}

func ConfigureRuntime(cfg RuntimeConfig) {
	// 运行时默认值定义在 core 层，这里允许由外部配置在启动时统一覆盖。
	if cfg.RedisRepositoryTimeout > 0 {
		RedisRepositoryTimeout = cfg.RedisRepositoryTimeout
	}
	if cfg.ScheduledTaskTimeout > 0 {
		ScheduledTaskTimeout = cfg.ScheduledTaskTimeout
	}
	if cfg.ScheduledTaskLockTTL > 0 {
		ScheduledTaskLockTTL = cfg.ScheduledTaskLockTTL
	}
	if cfg.CreateLockTTL > 0 {
		CreateLockTTL = cfg.CreateLockTTL
	}
}
