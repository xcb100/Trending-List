package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"trendingList/internal/app"
	"trendingList/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 启动时先完成配置加载，后续所有服务装配都依赖这份运行时配置。
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config failed", "error", err)
		os.Exit(1)
	}

	application := app.New(cfg)
	// 用信号驱动主 context，统一控制 HTTP 服务和后台调度器退出。
	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application.Start(runCtx)
	<-application.Done()

	runErr := application.RunError()
	if runErr != nil {
		slog.Error("application stopped due to runtime error", "error", runErr)
	} else {
		slog.Info("shutdown signal received")
	}

	// 关闭阶段使用单独的超时 context，避免某个依赖阻塞导致进程长时间无法退出。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := application.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown finished with error", "error", err)
		if runErr == nil {
			os.Exit(1)
		}
	}

	if runErr != nil {
		os.Exit(1)
	}
}
