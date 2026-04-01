package core

import (
	"context"
	"time"
)

const (
	// RedisRepositoryTimeout 是仓储层访问 Redis 的默认超时时间。
	RedisRepositoryTimeout = 800 * time.Millisecond
	// ScheduledTaskTimeout 是定时任务执行一次重算的默认超时时间。
	ScheduledTaskTimeout = 5 * time.Second
)

// WithOperationTimeout 为一次操作附加默认超时。
// 如果上游 ctx 已经带了更短的截止时间，则直接复用，避免放大超时窗口。
func WithOperationTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= timeout {
			return ctx, func() {}
		}
	}
	return context.WithTimeout(ctx, timeout)
}
