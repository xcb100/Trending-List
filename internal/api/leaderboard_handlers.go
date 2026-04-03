package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"awesomeProject/internal/core"
)

func CreateLeaderboardHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateLeaderboardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ID == "" || req.Expression == "" {
		sendError(w, http.StatusBadRequest, "id and expression are required")
		return
	}

	lb, err := core.CreateLeaderboard(r.Context(), req.ID, req.Expression, req.Schema, req.RefreshPolicy, req.CronSpec, core.DefaultRepo)
	if err != nil {
		// 重复创建返回冲突，便于调用方区分“参数错误”和“资源已存在”。
		if errors.Is(err, core.ErrLeaderboardExists) {
			sendError(w, http.StatusConflict, err.Error())
			return
		}
		sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	state := lb.State()
	sendJSON(w, http.StatusCreated, map[string]string{
		"status":         "created",
		"id":             req.ID,
		"refresh_policy": state.RefreshPolicy,
		"cron_spec":      state.CronSpec,
	})
}

func UpdateItemHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "leaderboard id is required")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "leaderboard not found")
			return
		}
		sendError(w, http.StatusInternalServerError, "failed to load leaderboard")
		return
	}

	var req UpdateItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ItemID == "" {
		sendError(w, http.StatusBadRequest, "item_id is required")
		return
	}

	item, err := lb.UpsertItem(r.Context(), req.ItemID, req.Data)
	if err != nil {
		sendError(w, http.StatusBadRequest, "failed to update item: "+err.Error())
		return
	}

	state := lb.State()
	response := map[string]interface{}{
		"item":           item,
		"refresh_policy": state.RefreshPolicy,
	}
	// 定时榜写入后只是落原始数据并标记 dirty，不会立刻更新榜单分数。
	if state.RefreshPolicy == core.RefreshPolicyScheduled {
		response["status"] = "dirty"
	} else {
		response["status"] = "updated"
	}

	sendJSON(w, http.StatusOK, response)
}

func GetLeaderboardHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "leaderboard id is required")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "leaderboard not found")
			return
		}
		sendError(w, http.StatusInternalServerError, "failed to load leaderboard")
		return
	}

	n := 10
	if nStr := r.URL.Query().Get("n"); nStr != "" {
		// 查询参数非法时回退到默认值，避免因为单次坏请求放大成 500。
		if val, err := strconv.Atoi(nStr); err == nil && val > 0 {
			n = val
		}
	}

	items, err := lb.GetTopN(r.Context(), n)
	if err != nil {
		sendError(w, http.StatusInternalServerError, "failed to read leaderboard items")
		return
	}

	state := lb.State()
	sendJSON(w, http.StatusOK, map[string]interface{}{
		"items":              items,
		"refresh_policy":     state.RefreshPolicy,
		"cron_spec":          state.CronSpec,
		"last_recomputed_at": state.LastRecomputedAt,
	})
}

func ScheduleUpdateHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "leaderboard id is required")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "leaderboard not found")
			return
		}
		sendError(w, http.StatusInternalServerError, "failed to load leaderboard")
		return
	}

	var req ScheduleUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.CronSpec == "" {
		sendError(w, http.StatusBadRequest, "cron_spec is required")
		return
	}

	if err := core.UpdateLeaderboardSchedule(r.Context(), lb, req.CronSpec); err != nil {
		sendError(w, http.StatusBadRequest, "failed to update schedule: "+err.Error())
		return
	}

	sendJSON(w, http.StatusOK, map[string]string{"status": "scheduled"})
}

func RecomputeLeaderboardHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "leaderboard id is required")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "leaderboard not found")
			return
		}
		sendError(w, http.StatusInternalServerError, "failed to load leaderboard")
		return
	}

	if err := lb.Recompute(r.Context()); err != nil {
		// 这里显式返回 409，告诉调用方当前已有同榜重算在执行。
		if errors.Is(err, core.ErrRecomputeInProgress) {
			sendError(w, http.StatusConflict, err.Error())
			return
		}
		sendError(w, http.StatusInternalServerError, "recompute failed: "+err.Error())
		return
	}

	state := lb.State()
	sendJSON(w, http.StatusOK, map[string]interface{}{
		"status":             "recomputed",
		"id":                 state.ID,
		"last_recomputed_at": state.LastRecomputedAt,
	})
}

func UpdateExpressionHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "leaderboard id is required")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "leaderboard not found")
			return
		}
		sendError(w, http.StatusInternalServerError, "failed to load leaderboard")
		return
	}

	var req UpdateExpressionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Expression == "" {
		sendError(w, http.StatusBadRequest, "expression is required")
		return
	}

	if err := core.UpdateLeaderboardExpression(r.Context(), lb, req.Expression, req.Schema); err != nil {
		// 表达式更新会触发全量重算，因此也可能因为已有重算任务而冲突。
		if errors.Is(err, core.ErrRecomputeInProgress) {
			sendError(w, http.StatusConflict, err.Error())
			return
		}
		sendError(w, http.StatusBadRequest, "failed to update expression: "+err.Error())
		return
	}

	state := lb.State()
	sendJSON(w, http.StatusOK, map[string]interface{}{
		"status":          "expression_updated",
		"id":              state.ID,
		"expression":      state.Expression,
		"refresh_policy":  state.RefreshPolicy,
		"last_recomputed": state.LastRecomputedAt,
		"schema_provided": req.Schema != nil,
	})
}

func DeleteItemHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	itemID := r.PathValue("item_id")
	if lbID == "" || itemID == "" {
		sendError(w, http.StatusBadRequest, "leaderboard id and item id are required")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "leaderboard not found")
			return
		}
		sendError(w, http.StatusInternalServerError, "failed to load leaderboard")
		return
	}

	if err := lb.DeleteItem(r.Context(), itemID); err != nil {
		sendError(w, http.StatusInternalServerError, "failed to delete item: "+err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func DeleteLeaderboardHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "leaderboard id is required")
		return
	}

	if err := core.DeleteLeaderboard(r.Context(), lbID); err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "leaderboard not found")
			return
		}
		sendError(w, http.StatusInternalServerError, "failed to delete leaderboard")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
