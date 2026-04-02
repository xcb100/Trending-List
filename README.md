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

## 核心特性与架构设计

本项目在设计与压测过程（详情参考 `TEST_PLAN_AND_RESULTS.md`）中引入了多项生产级优化与并发控制策略：

### 1. 动态表达式与脏数据懒更新
- **自定义积分引擎**：基于 `expr` 提供动态规则解析，支持业务方随时配置复杂的加权算分逻辑。
- **脏数据懒写机制 (Lazy Evaluation)**：对于配置了 `scheduled` 策略的榜单，高频的条目变动只会暂存负载数据并打上 `dirty` 标记。复杂的公式算分被延后至后台批处理，极大提升了主流程的写入吞吐。
- **AST 解析缓存**：针对高频使用的排行榜和 Cron 语法树，使用本地 `sync.Map` 进行解析对象缓存，减少每次处理时的并发 CPU 开销。

### 2. 高性能分批处理与存取优化
- **双层 Pipeline 存取**：Redis 仓储层利用 Pipeline 将多条 HSET/ZADD/SREM 命令打包发送，有效降低 RTT（往返时延）。
- **后台分批重算 (Batching)**：`Recompute` 任务通过限制每次读取和写入的 Batch Size (如 500 条/批)，平滑消化并重算庞大的脏数据集，避免大批量数据导致的 Redis 阻塞和内存尖刺。
- **恒定内存流控 (OOM 防御)**：清算海量脏数据时不使用危险的 `SMEMBERS` 全量读取，而是改造为基于游标的 `SSCAN` 分页提取。在大数据情况下，Go 进程的内存开销始终保持恒定，降低 OOM 风险。

### 3. 数据一致性与高可用控制
- **Lua 乐观锁防御 ABA 并发覆盖**：针对 "读取脏数据 -> 计算分数 -> 删除脏标记" 流程中极易产生的竞态问题，使用 Redis Lua 原子脚本，将 `UpdatedAt` 时间戳作为 CAS 乐观锁版本号。若在计算期间发生新的用户请求触发更新，系统精准中止当前批次的脏标记清除，做到 **0 数据丢失和安全覆盖**。
- **Singleflight 防雪崩**：对于系统冷启动或特定榜单配置被并发大量访问的场景，应用通过 `golang.org/x/sync/singleflight` 机制收束请求。不管瞬间涌入多少并发，同一个榜单的元数据重建操作只会对 Redis 发起一次拉取，保护底层存储。
- **强制请求级超时管控**：所有连接 Redis 的仓储操作强制继承并附加细粒度的时间设定（Timeouts），遇到外部链路抖动立刻熔断释放，防止协程长连接泄漏。

### 4. 分布式高精度定时调度
针对 K8s 原生 CronJob 依赖 YAML 配置且仅支持分钟级精度的痛点，内置轻量级调度器：
- **微秒级降级队列 (Tiered Routing)**：支持秒级执行要求（如 `*/5 * * * * *`）。通过在缓存层拆分出 `5s`, `1m`, `30m`, `6h` 的分布式子集（Subset），后台定时器通过直接访问对应子集的 Set 来获取需要执行任务的榜单，避开了传统定时任务轮询全量数据的 O(N) 性能瓶颈。
- **分布式抢占锁 (Distributed Locking)**：微服务多副本场景下，基于 Redis `SETNX` (附带兜底 TTL 过期) 自动抢占后台重算任务，避免多节点并发写发生数据踩踏。

### 5. 生产级可观测性隔离
- **内外网端口隔离**：业务 API 默认运行于 `8080` 端口，而 Prometheus 相关探针与系统进程（GC、Goroutine）数据被保护在内部专用的 `9090` 端口，防止运营数据泄漏。
- **结构化打点与日志**：
  - 基于官方 `log/slog` 构建包含了请求标识与上下文标签的底层日志系统。
  - 通过注入 Middleware 输出标准的 HTTP Metrics，能直接用于绘制 P99/P95 与每秒 QPS。
## 测试

```powershell
go test ./...
```
（包含完整的单测、并发防击穿断言与各种自动化极端测试脚本，详情请见 `cmd/loadtest_*` 的基准压测模块）

## 设计说明

- 榜单元数据会持久化 `expression`、`schema`、`refresh_policy`、`cron_spec`
- `scheduled` 模式通过 dirty 集合追踪待重算条目
- 仓储层通过 `Repository` 抽象屏蔽业务层与具体存储实现
- Redis 仓储层会基于 `ctx` 自动附加默认超时，避免存储调用无限阻塞
- 如果上游已经传入更短的 deadline，会直接复用上游 deadline，不会放大超时窗口
- HTTP 服务和 Redis 客户端也额外配置了网络级超时，作为第二层保护
