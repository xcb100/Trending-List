package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"trendingList/internal/core"
)

func SystemCronTickHandler(w http.ResponseWriter, r *http.Request) {
	// 该入口只应该由内部系统调用，用于统一触发所有调度层的补偿或联调执行。
	for _, tier := range []string{core.Tier5s, core.Tier1m, core.Tier30m, core.Tier6h} {
		if err := core.ProcessCronTick(r.Context(), tier); err != nil {
			sendError(w, http.StatusInternalServerError, "system cron tick failed: "+err.Error())
			return
		}
	}

	sendJSON(w, http.StatusOK, map[string]interface{}{
		"status": "ticked_all_tiers",
		"time":   time.Now().Format(time.RFC3339),
	})
}

type SystemReplayRequest struct {
	LeaderboardID string `json:"leaderboard_id"`
}

type ReplayFunc func(r *http.Request, leaderboardID string) (map[string]interface{}, error)

func SystemReplayDurableHandler(replay ReplayFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SystemReplayRequest
		if r.Body != nil {
			err := json.NewDecoder(r.Body).Decode(&req)
			if err != nil && !errors.Is(err, io.EOF) {
				sendError(w, http.StatusBadRequest, "invalid request body")
				return
			}
		}

		payload, err := replay(r, req.LeaderboardID)
		if err != nil {
			sendError(w, http.StatusInternalServerError, "system replay failed: "+err.Error())
			return
		}

		sendJSON(w, http.StatusOK, payload)
	}
}
