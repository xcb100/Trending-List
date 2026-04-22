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

type ItemMutationOp struct {
	Field string  `json:"field"`
	Op    string  `json:"op"`
	Value float64 `json:"value"`
}

type MutateItemRequest struct {
	ItemID string           `json:"item_id"`
	Ops    []ItemMutationOp `json:"ops"`
}

type ScheduleUpdateRequest struct {
	CronSpec string `json:"cron_spec"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
