package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"awesomeProject/internal/app"
	"awesomeProject/internal/config"
)

func main() {
	// 启动时先完成配置加载，后续所有服务装配都依赖这份运行时配置。
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("load config:", err)
	}

	application := app.New(cfg)
	// 用信号驱动主 context，统一控制 HTTP 服务和后台调度器退出。
	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application.Start(runCtx)
	<-runCtx.Done()

	log.Println("Shutting down...")

	// 关闭阶段使用单独的超时 context，避免某个依赖阻塞导致进程长时间无法退出。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := application.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
