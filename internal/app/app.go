package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"awesomeProject/internal/api"
	"awesomeProject/internal/config"
	"awesomeProject/internal/core"

	"github.com/redis/go-redis/v9"
)

type App struct {
	Config         config.Config
	RedisClient    *redis.Client
	Repository     *core.RedisRepository
	BusinessServer *http.Server
	InternalServer *http.Server
	runCtx         context.Context
	cancel         context.CancelFunc
	startOnce      sync.Once
	runErrMu       sync.RWMutex
	runErr         error
}

func New(cfg config.Config) *App {
	runCtx, cancel := context.WithCancel(context.Background())

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		DialTimeout:  cfg.RedisDialTimeout,
		ReadTimeout:  cfg.RedisReadTimeout,
		WriteTimeout: cfg.RedisWriteTimeout,
		PoolTimeout:  cfg.RedisPoolTimeout,
	})

	repo := core.NewRedisRepository(rdb)
	core.SetDefaultRepo(repo)
	core.ConfigureRuntime(core.RuntimeConfig{
		RedisRepositoryTimeout: cfg.RedisRepositoryTimeout,
		ScheduledTaskTimeout:   cfg.ScheduledTaskTimeout,
		ScheduledTaskLockTTL:   cfg.ScheduledTaskLockTTL,
		CreateLockTTL:          cfg.LeaderboardCreateLockTTL,
	})

	readinessCheck := func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	}

	// 业务服务和内部服务统一在这里装配，main 只需要处理进程生命周期，
	// 不需要关心具体的传输层细节。
	return &App{
		Config:      cfg,
		RedisClient: rdb,
		Repository:  repo,
		runCtx:      runCtx,
		cancel:      cancel,
		BusinessServer: &http.Server{
			Addr:              cfg.BusinessAddr,
			Handler:           api.NewBusinessMux(cfg.HealthcheckTimeout, readinessCheck),
			ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
			ReadTimeout:       cfg.HTTPReadTimeout,
			WriteTimeout:      cfg.HTTPWriteTimeout,
			IdleTimeout:       cfg.HTTPIdleTimeout,
		},
		InternalServer: &http.Server{
			Addr:              cfg.InternalAddr,
			Handler:           api.NewInternalMux(cfg.HealthcheckTimeout, readinessCheck, cfg.InternalAPIToken),
			ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
			ReadTimeout:       cfg.HTTPReadTimeout,
			WriteTimeout:      cfg.HTTPWriteTimeout,
			IdleTimeout:       cfg.HTTPIdleTimeout,
		},
	}
}

func (a *App) Done() <-chan struct{} {
	return a.runCtx.Done()
}

func (a *App) RunError() error {
	a.runErrMu.RLock()
	defer a.runErrMu.RUnlock()
	return a.runErr
}

func (a *App) setRunError(err error) {
	if err == nil {
		return
	}

	a.runErrMu.Lock()
	defer a.runErrMu.Unlock()
	if a.runErr != nil {
		return
	}

	a.runErr = err
	a.cancel()
}

func (a *App) Start(ctx context.Context) {
	a.startOnce.Do(func() {
		if ctx != nil {
			go func() {
				// 把应用内部 context 绑定到外部进程 context，
				// 这样调度器和后续后台任务都会随着进程一起退出。
				<-ctx.Done()
				a.cancel()
			}()
		}

		go func() {
			slog.Info("business api server listening", "addr", a.Config.BusinessAddr)
			if err := a.BusinessServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				wrappedErr := fmt.Errorf("business server: %w", err)
				slog.Error("business api server exited unexpectedly", "error", wrappedErr)
				a.setRunError(wrappedErr)
			}
		}()

		go func() {
			slog.Info("internal server listening", "addr", a.Config.InternalAddr)
			if err := a.InternalServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				wrappedErr := fmt.Errorf("internal server: %w", err)
				slog.Error("internal server exited unexpectedly", "error", wrappedErr)
				a.setRunError(wrappedErr)
			}
		}()

		if a.Config.SchedulerEnabled {
			// 调度器使用应用级 context，而不是 Background，
			// 这样关闭服务时可以及时停止周期性重算循环。
			core.StartCronScheduler(a.runCtx)
		}
	})
}

func (a *App) Shutdown(ctx context.Context) error {
	a.cancel()

	var errs []error
	if err := a.BusinessServer.Shutdown(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := a.InternalServer.Shutdown(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := a.RedisClient.Close(); err != nil {
		slog.Error("close redis client failed", "error", err)
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
