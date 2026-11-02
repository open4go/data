# data — 实时经营数据统计（Redis + Prometheus）

> 仓库：https://github.com/open4go/data ｜ 模块：`github.com/open4go/data` ｜ Go 1.25.8 ｜ 最近提交：2026-04-27（最新 tag v0.0.x，共 33 个 tag）

## 项目简介
接收注册、订单、支付、退款、充值事件，按租户和日期在 Redis 里累计计数、金额和排行（Hash、ZSet、HyperLogLog），同时更新自定义 Prometheus Registry 中的业务指标；提供看板查询函数（日指标、趋势、周汇总、退款率、商品销量排行），以及造数函数（Simulate*）。

## 目录结构
```
data/
├── db/db.go          GetRedisDataHandler（open4go/db "data"）
├── event/type.go     EventRegistration/Order/Payment/Refund/Recharge
├── event/stats.go    Event{EventType,Timestamp,Data,TenantID}、ProcessEvent
├── event/metrics.go  Registry 与 dashboard_* 指标（带 tenant_id 标签）
├── event/prometheus.go updatePrometheusMetrics
├── event/query.go    GetDashboardMetrics / GetMetricsByDate / GetTrendMetrics / GetWeeklySummary / GetHourlyRefundRateTrend / GetProductSalesRank / GetRefundRateWithTenant …
├── event/fake.go     SimulateEvent / SimulateBatch / SimulateTodayData
└── stats/stats.go    另一套同名指标（默认 Registry，标签不同）
```

## 使用示例
```go
event.ProcessEvent(ctx, event.Event{
    EventType: event.EventOrder, Timestamp: time.Now(), TenantID: mid,
    Data: map[string]interface{}{"store_id": sid, "amount": 25.5, "items": items},
})
m := event.GetDashboardMetrics(ctx, mid)
http.Handle("/metrics", promhttp.HandlerFor(event.Registry, promhttp.HandlerOpts{}))
```
Redis key 示例：`{tenant}:order:total:amount:{yyyyMMdd}`、`{tenant}:product:sales:rank:qty:{date}`。

## 依赖
`prometheus/client_golang`、`go-redis/v9 v9.7.3`、`open4go/{db v0.0.13, log v0.0.18}`

## 构建
`go build`、`go vet` 通过；没有测试；CI 失败。

## 与其他仓库的关系
依赖 `db`、`log`；事件来源可能是 r2day/order-api 等服务（open4go 内没有调用方）。
