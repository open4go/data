# data 优化方案

| # | 优先级 | 问题 | 位置 | 建议 | 工作量 |
|---|---|---|---|---|---|
| 1 | 高 | 所有统计 key 都**没有 TTL**（按天、按小时、按商品、按门店），Redis 内存会无限增长 | `event/stats.go` ProcessEvent | 在 pipeline 里对每个 key 加 `Expire`（例如 90 天），或者按天汇总落到 Mongo 后再清理 | S |
| 2 | 高 | `pipe.Exec` 的错误被丢弃，Redis 故障时统计会静默丢失 | `event/stats.go:140` | 记日志并返回 error，必要时重试或写入死信 | S |
| 3 | 高 | 没有幂等：同一事件被重复投递时会重复累加（Kafka 至少一次投递很常见） | `ProcessEvent` | 事件携带 `event_id`，用 `SETNX dedup:{id} EX 1d` 去重 | M |
| 4 | 中 | `orderByStore` 是 Gauge，却一直 `Add`，跨天不会清零，进程重启后又从 0 开始，数据既不是当日值也不是累计值 | `event/prometheus.go:48` | 改成 Counter（让 Prometheus 计算 rate），或者从 Redis 读当日值后 `Set` | S |
| 5 | 中 | Prometheus 标签基数不受控：`tenant_id × store_id` 会随商户数线性增长 | `event/metrics.go` | 门店维度只保留在 Redis 或看板，Prometheus 只保留租户级指标 | S |
| 6 | 中 | `stats/stats.go` 定义了同名指标（默认 Registry，标签不同），看起来是旧版本的遗留，被导入时容易引起混淆 | `stats/stats.go` | 删除 | S |
| 7 | 中 | 金额是 float64，并且用 `IncrByFloat` 累计，会有浮点误差 | `stats.go`、`object.go` | 改为以分为单位的整数，用 `IncrBy` | M |
| 8 | 低 | `GetRedisDataHandler` 出错时 `Fatal`；`GetProductSalesTrend` 是空实现 | `db/db.go`、`query.go:368` | 返回 error；实现或删除空函数 | S |
| 9 | 低 | `fake.go` 造数代码放在生产包里 | `event/fake.go` | 移到 `internal/fake` 或 cmd 工具 | S |
| 10 | 中 | govulncheck 发现 7 个问题：x/net v0.51.0、quic-go v0.59.0（GO-2026-5676）、docker（来自旧版 log v0.0.18） | `go.mod` | 升级 log 到 v0.0.23+、x/net、quic-go | S |
| 11 | 中 | 没有测试，没有 README | — | 用 miniredis 测 ProcessEvent 和查询 | M |
