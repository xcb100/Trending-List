package core

import (
	"context"
	"time"
)

const (
	defaultRedisRepositoryTimeout = 800 * time.Millisecond
	defaultScheduledTaskTimeout   = 5 * time.Second
	defaultScheduledTaskLockTTL   = defaultScheduledTaskTimeout + 5*time.Second
	defaultCreateLockTTL          = 5 * time.Second
)

var (
	RedisRepositoryTimeout = defaultRedisRepositoryTimeout
	ScheduledTaskTimeout   = defaultScheduledTaskTimeout
	ScheduledTaskLockTTL   = defaultScheduledTaskLockTTL
	CreateLockTTL          = defaultCreateLockTTL
)

func WithOperationTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	// 已经有更短 deadline 的上游 context 不再额外包一层，
	// 避免把真实调用超时意外放宽。
	if timeout <= 0 {
		return ctx, func() {}
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func WithScheduledTaskTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	// 调度重算统一走单独超时，避免后台任务无限占用连接和锁。
	return WithOperationTimeout(ctx, ScheduledTaskTimeout)
}
