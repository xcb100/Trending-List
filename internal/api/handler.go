package api

import (
	"awesomeProject/internal/core"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

// 请求和响应结构体
type CreateLeaderboardRequest struct {
	ID            string                 `json:"id"`
	Expression    string                 `json:"expression"` // 例如 "views * 0.5 + likes * 2"
	Schema        map[string]interface{} `json:"schema"`     // 例如 {"views": 0, "likes": 0}
	RefreshPolicy string                 `json:"refresh_policy"`
	CronSpec      string                 `json:"cron_spec"` // 例如 "@every 1m"
}

type UpdateItemRequest struct {
	ItemID string                 `json:"item_id"`
	Data   map[string]interface{} `json:"data"` // 例如 {"views": 100, "likes": 10}
}

type ScheduleUpdateRequest struct {
	CronSpec string `json:"cron_spec"` // 例如 "@every 1m"
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func sendError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Error: msg})
}

func sendJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// Handlers

// API 名称: CreateLeaderboardHandler
// 输入: JSON (包含 id, expression, schema 等 CreateLeaderboardRequest 字段)
// 输出: JSON (创建状态 status, id 等) 或 Error (状态码 400)
// 目的功能: 接收并创建一个新的排行榜（leaderboard）
func CreateLeaderboardHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateLeaderboardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "请求体无效")
		return
	}

	if req.ID == "" || req.Expression == "" {
		sendError(w, http.StatusBadRequest, "ID 和 expression 是必填项")
		return
	}

	lb, err := core.CreateLeaderboard(r.Context(), req.ID, req.Expression, req.Schema, req.RefreshPolicy, req.CronSpec, core.DefaultRepo)
	if err != nil {
		sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	sendJSON(w, http.StatusCreated, map[string]string{
		"status":         "created",
		"id":             req.ID,
		"refresh_policy": lb.RefreshPolicy,
		"cron_spec":      lb.CronSpec,
	})
}

// API 名称: UpdateItemHandler
// 输入: JSON (包含 item_id, data map)，路径参数 id (排行榜标识)
// 输出: JSON (更新状态，包含 item 实体) 或 Error
// 目的功能: 允许更新或增加某一个具体项的数据（积分将据策略实时或延迟计算）
func UpdateItemHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "需要排行榜 ID")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "未找到排行榜")
			return
		}
		sendError(w, http.StatusInternalServerError, "读取排行榜失败")
		return
	}

	var req UpdateItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "请求体无效")
		return
	}

	if req.ItemID == "" {
		sendError(w, http.StatusBadRequest, "条目 ID 是必填项")
		return
	}

	item, err := lb.UpsertItem(r.Context(), req.ItemID, req.Data)
	if err != nil {
		sendError(w, http.StatusBadRequest, "更新条目失败: "+err.Error())
		return
	}

	response := map[string]interface{}{
		"item":           item,
		"refresh_policy": lb.RefreshPolicy,
	}
	if lb.RefreshPolicy == core.RefreshPolicyScheduled {
		response["status"] = "dirty"
	} else {
		response["status"] = "updated"
	}
	sendJSON(w, http.StatusOK, response)
}

// API 名称: GetLeaderboardHandler
// 输入: 路径参数 id，查询参数 n（可选前 N 名）
// 输出: JSON (包含 items 列表, 策略等元数据)
// 目的功能: 获取指定排行榜前N名的所有数据及其实时积分
func GetLeaderboardHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "需要排行榜 ID")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "未找到排行榜")
			return
		}
		sendError(w, http.StatusInternalServerError, "读取排行榜失败")
		return
	}

	nStr := r.URL.Query().Get("n")
	n := 10 // 默认值
	if nStr != "" {
		if val, err := strconv.Atoi(nStr); err == nil && val > 0 {
			n = val
		}
	}

	items := lb.GetTopN(r.Context(), n)
	sendJSON(w, http.StatusOK, map[string]interface{}{
		"items":              items,
		"refresh_policy":     lb.RefreshPolicy,
		"cron_spec":          lb.CronSpec,
		"last_recomputed_at": lb.LastRecomputedAt,
	})
}

// API 名称: ScheduleUpdateHandler
// 输入: JSON (包含 cron_spec)，路径参数 id
// 输出: JSON (状态 scheduled)
// 目的功能: 配置指定排行榜为延迟定时计算策略，并打上所需 cron spec。现支持外部定时任务调用刷新。
func ScheduleUpdateHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "需要排行榜 ID")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "未找到排行榜")
			return
		}
		sendError(w, http.StatusInternalServerError, "读取排行榜失败")
		return
	}

	var req ScheduleUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "请求体无效")
		return
	}

	if req.CronSpec == "" {
		sendError(w, http.StatusBadRequest, "Cron 规格是必填项")
		return
	}

	lb.RefreshPolicy = core.RefreshPolicyScheduled
	lb.CronSpec = req.CronSpec
	if err := core.DefaultRepo.SaveMetadata(r.Context(), lb.ID, map[string]string{
		"expression":         lb.Expression,
		"refresh_policy":     lb.RefreshPolicy,
		"cron_spec":          lb.CronSpec,
		"last_recomputed_at": lb.LastRecomputedAt.Format(time.RFC3339Nano),
	}); err != nil {
		sendError(w, http.StatusInternalServerError, "保存调度配置失败: "+err.Error())
		return
	}

	// 添加至对应的分级调度队列
	tier := core.DetermineTier(req.CronSpec)
	_ = core.DefaultRepo.AddScheduledLeaderboard(r.Context(), lb.ID, tier)

	sendJSON(w, http.StatusOK, map[string]string{"status": "scheduled"})
}

// API 名称: RecomputeLeaderboardHandler
// 输入: 路径参数 id
// 输出: JSON (重算状态、最新时间戳)
// 目的功能: 执行全量的脏数据重新计算，计算出各项最新积分。由于移除了内部 scheduler，当前方法多用于外部 k8s CronJob 触发使用。
func RecomputeLeaderboardHandler(w http.ResponseWriter, r *http.Request) {
	lbID := r.PathValue("id")
	if lbID == "" {
		sendError(w, http.StatusBadRequest, "需要排行榜 ID")
		return
	}

	lb, err := core.GetLeaderboard(r.Context(), lbID)
	if err != nil {
		if errors.Is(err, core.ErrLeaderboardNotFound) {
			sendError(w, http.StatusNotFound, "未找到排行榜")
			return
		}
		sendError(w, http.StatusInternalServerError, "读取排行榜失败")
		return
	}

	if err := lb.Recompute(r.Context()); err != nil {
		sendError(w, http.StatusInternalServerError, "手动重算失败: "+err.Error())
		return
	}

	sendJSON(w, http.StatusOK, map[string]interface{}{
		"status":             "recomputed",
		"id":                 lb.ID,
		"last_recomputed_at": lb.LastRecomputedAt,
	})
}

// API 名称: SystemCronTickHandler
// 输入: 空 (建议 1 次/分钟 的外部 K8s CronJob 触发)
// 输出: JSON (状态)
// 目的功能: 提供单个全局心跳接口。由于已经切换为基于 Redis 分布式锁的内部 5秒级 调度，此接口降级为手动触发的备用手段。
func SystemCronTickHandler(w http.ResponseWriter, r *http.Request) {
	for _, t := range []string{core.Tier5s, core.Tier1m, core.Tier30m, core.Tier6h} {
		if err := core.ProcessCronTick(r.Context(), t); err != nil {
			sendError(w, http.StatusInternalServerError, "执行系统全局 Cron 发生异常: "+err.Error())
			return
		}
	}
	sendJSON(w, http.StatusOK, map[string]interface{}{
		"status": "ticked_all_tiers",
		"time":   time.Now().Format(time.RFC3339),
	})
}
