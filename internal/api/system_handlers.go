package api

import (
	"net/http"
	"time"

	"awesomeProject/internal/core"
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
