package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type healthResponse struct {
	Status string `json:"status"`
}

func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, http.StatusOK, healthResponse{Status: "ok"})
	}
}

func ReadinessHandler(timeout time.Duration, check func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 就绪检查可以依赖外部组件状态，这里当前主要用于确认 Redis 可访问。
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		if err := check(ctx); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "not_ready",
				"error":  err.Error(),
			})
			return
		}

		sendJSON(w, http.StatusOK, healthResponse{Status: "ready"})
	}
}

func HealthHandler(timeout time.Duration, check func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// healthz 用于给人工排障或上层系统提供一个更直观的综合健康视图。
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		if err := check(ctx); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "degraded",
				"error":  err.Error(),
			})
			return
		}

		sendJSON(w, http.StatusOK, healthResponse{Status: "ok"})
	}
}
