# 热点榜单微服务

这是一个基于 Go 的热点榜单微服务，支持：

- 自定义榜单字段 schema
- 自定义表达式排序
- `realtime` 实时更新分数
- `scheduled` 定时批量重算分数
- `POST /leaderboard/{id}/recompute` 手动触发重算
- Redis 仓储访问超时控制

## 接口说明

### 1. 创建榜单

`POST /leaderboard`

请求体示例：

```json
{
  "id": "top_videos",
  "expression": "views + likes * 2",
  "schema": {
    "views": 0,
    "likes": 0
  },
  "refresh_policy": "realtime"
}
```

定时榜单示例：

```json
{
  "id": "hot_articles",
  "expression": "views * 0.5 + likes * 5",
  "schema": {
    "views": 0,
    "likes": 0
  },
  "refresh_policy": "scheduled",
  "cron_spec": "@every 10s"
}
```

### 2. 写入条目

`POST /leaderboard/{id}/item`

```json
{
  "item_id": "video_123",
  "data": {
    "views": 1000,
    "likes": 50
  }
}
```

- `realtime`：写入后立即计算并更新分数
- `scheduled`：写入后只保存原始数据并标记 dirty，等待 cron 或手动重算

### 3. 查看榜单

`GET /leaderboard/{id}?n=10`

### 4. 手动重算榜单

`POST /leaderboard/{id}/recompute`

## 运行方式

确保本地 Redis 可用，默认地址是 `localhost:6379`。

```powershell
go run .
```

如需自定义 Redis 地址，可设置环境变量 `REDIS_ADDR`（以及可选的 `REDIS_PASSWORD`）。

## 测试

```powershell
go test ./...
```

## 设计说明

- 榜单元数据会持久化 `expression`、`schema`、`refresh_policy`、`cron_spec`
- `scheduled` 模式通过 dirty 集合追踪待重算条目
- 仓储层通过 `Repository` 抽象屏蔽业务层与具体存储实现
- 默认使用 Redis 仓储作为运行时恢复与读写存储
- Redis 仓储层会基于 `ctx` 自动附加默认超时，避免存储调用无限阻塞
- 如果上游已经传入更短的 deadline，会直接复用上游 deadline，不会放大超时窗口
- HTTP 服务和 Redis 客户端也额外配置了网络级超时，作为第二层保护

## 后续建议

如果后续部署到 K8s，多副本场景更推荐把 cron 从应用内迁出到 `CronJob`，或者至少为应用内调度补充分布式锁。当前版本已经把手动重算接口准备好了，后续接 K8s `CronJob` 会比较顺。
