package api

type CreateLeaderboardRequest struct {
	ID            string                 `json:"id"`
	Expression    string                 `json:"expression"`
	Schema        map[string]interface{} `json:"schema"`
	RefreshPolicy string                 `json:"refresh_policy"`
	CronSpec      string                 `json:"cron_spec"`
}

type UpdateItemRequest struct {
	ItemID string                 `json:"item_id"`
	Data   map[string]interface{} `json:"data"`
}

type ScheduleUpdateRequest struct {
	CronSpec string `json:"cron_spec"`
}

type UpdateExpressionRequest struct {
	Expression string                 `json:"expression"`
	Schema     map[string]interface{} `json:"schema"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
