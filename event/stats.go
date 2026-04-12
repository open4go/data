// stats/stats.go
package event

import (
	"context"
	"fmt"
	"time"

	"github.com/open4go/data/db"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
)

// 事件类型常量
const (
	EventRegistration = "registration"
	EventOrder        = "order"
	EventPayment      = "payment"
	EventRefund       = "refund"
	EventRecharge     = "recharge"
)

type Event struct {
	EventType string                 `json:"event_type"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

var (
	rdb     *redis.Client
	mongoDB *mongo.Collection
)

// Init 初始化 Redis 和 MongoDB 客户端（在 main.go 中调用一次）
func Init(redisClient *redis.Client, mongoColl *mongo.Collection) {
	rdb = redisClient
	mongoDB = mongoColl
}

// ====================================
//          事件处理（统计核心）
// ====================================

func ProcessEvent(e Event) {
	ctx := context.Background()
	date := e.Timestamp.Format("20060102")

	rds := db.GetRedisDataHandler(ctx)
	pipe := rds.Pipeline()

	switch e.EventType {
	case EventRegistration:
		source := fmt.Sprintf("%v", e.Data["source"])
		userID := fmt.Sprintf("%v", e.Data["user_id"])

		pipe.HIncrBy(ctx, "reg:source:"+date+":"+source, "count", 1)
		pipe.PFAdd(ctx, "reg:unique:hll:"+date, userID)
		pipe.ZIncrBy(ctx, "reg:source:rank:"+date, 1, source)

	case EventOrder:
		storeID := fmt.Sprintf("%v", e.Data["store_id"])
		amount := getFloat(e.Data, "amount")

		storeKey := "order:store:" + storeID + ":" + date
		pipe.HIncrBy(ctx, storeKey, "count", 1)
		pipe.HIncrByFloat(ctx, storeKey, "amount", amount)
		pipe.ZIncrBy(ctx, "order:amount:rank:"+date, amount, storeID)

		pipe.IncrByFloat(ctx, "order:total:amount:"+date, amount)
		pipe.Incr(ctx, "order:total:count:"+date)

	case EventRefund:
		storeID := fmt.Sprintf("%v", e.Data["store_id"])
		amount := getFloat(e.Data, "amount")

		refundKey := "refund:store:" + storeID + ":" + date
		pipe.HIncrBy(ctx, refundKey, "count", 1)
		pipe.HIncrByFloat(ctx, refundKey, "amount", amount)

		pipe.IncrByFloat(ctx, "refund:total:amount:"+date, amount)
		pipe.Incr(ctx, "refund:total:count:"+date)

	case EventPayment, EventRecharge:
		// 后续扩展
	}

	_, _ = pipe.Exec(ctx)

	// 持久化到 MongoDB Time Series
	mongoDB.InsertOne(ctx, e)
}

func getFloat(data map[string]interface{}, key string) float64 {
	if v, ok := data[key].(float64); ok {
		return v
	}
	return 0.0
}

// ====================================
//          数据统计结果获取函数
// ====================================

// GetDashboardMetrics 获取今日仪表盘核心指标（供前端 /init 接口使用）
//
// JSON Sample:
//
//	{
//	  "regSources": [
//	    {"Member": "wechat", "Score": 42},
//	    {"Member": "app", "Score": 28}
//	  ],
//	  "orderByStore": [
//	    {"Member": "STORE001", "Score": 12500.5},
//	    {"Member": "STORE002", "Score": 8900}
//	  ],
//	  "totalOrderAmount": "21400.5",
//	  "totalOrderCount": "156",
//	  "totalRefundAmount": "1200",
//	  "totalRefundCount": "8",
//	  "refundRate": 5.61,
//	  "uniqueUsers": 65
//	}
func GetDashboardMetrics() map[string]interface{} {
	date := time.Now().Format("20060102")
	return GetMetricsByDate(date)
}

// GetMetricsByDate 获取指定日期的统计指标
//
// JSON Sample: 同 GetDashboardMetrics（只是日期可自定义）
func GetMetricsByDate(date string) map[string]interface{} {
	ctx := context.Background()
	m := make(map[string]interface{})

	// 1. 注册来源 Top 10
	sources, _ := rdb.ZRevRangeWithScores(ctx, "reg:source:rank:"+date, 0, 9).Result()
	m["regSources"] = sources

	// 2. 门店下单金额排行
	stores, _ := rdb.ZRevRangeWithScores(ctx, "order:amount:rank:"+date, 0, 9).Result()
	m["orderByStore"] = stores

	// 3. 总订单金额和数量
	totalAmount, _ := rdb.Get(ctx, "order:total:amount:"+date).Result()
	totalCount, _ := rdb.Get(ctx, "order:total:count:"+date).Result()
	m["totalOrderAmount"] = totalAmount
	m["totalOrderCount"] = totalCount

	// 4. 退单统计
	refundAmount, _ := rdb.Get(ctx, "refund:total:amount:"+date).Result()
	refundCount, _ := rdb.Get(ctx, "refund:total:count:"+date).Result()
	m["totalRefundAmount"] = refundAmount
	m["totalRefundCount"] = refundCount

	// 5. 退单率
	if totalAmount != "" && refundAmount != "" {
		refundRate := calculateRefundRate(totalAmount, refundAmount)
		m["refundRate"] = refundRate
	}

	// 6. 注册唯一用户数（HyperLogLog）
	uniqueUsers, _ := rdb.PFCount(ctx, "reg:unique:hll:"+date).Result()
	m["uniqueUsers"] = uniqueUsers

	return m
}

// GetRegistrationSources 获取注册来源详细数据（支持所有来源）
//
// JSON Sample:
// [
//
//	{"Member": "wechat", "Score": 142},
//	{"Member": "app", "Score": 89},
//	{"Member": "miniapp", "Score": 37}
//
// ]
func GetRegistrationSources(date string) []redis.Z {
	if date == "" {
		date = time.Now().Format("20060102")
	}
	ctx := context.Background()
	result, _ := rdb.ZRevRangeWithScores(ctx, "reg:source:rank:"+date, 0, -1).Result()
	return result
}

// GetOrderByStore 获取门店下单金额排行（支持指定 Top N）
//
// JSON Sample:
// [
//
//	{"Member": "STORE001", "Score": 12500.5},
//	{"Member": "STORE005", "Score": 9800},
//	{"Member": "STORE003", "Score": 4500}
//
// ]
func GetOrderByStore(date string, topN int64) []redis.Z {
	if date == "" {
		date = time.Now().Format("20060102")
	}
	if topN <= 0 {
		topN = 10
	}
	ctx := context.Background()
	result, _ := rdb.ZRevRangeWithScores(ctx, "order:amount:rank:"+date, 0, topN-1).Result()
	return result
}

// GetRefundRate 获取今日退单率（百分比）
//
// JSON Sample: 直接返回数字，例如 5.61
func GetRefundRate() float64 {
	date := time.Now().Format("20060102")
	ctx := context.Background()

	totalAmountStr, _ := rdb.Get(ctx, "order:total:amount:"+date).Result()
	refundAmountStr, _ := rdb.Get(ctx, "refund:total:amount:"+date).Result()

	return calculateRefundRate(totalAmountStr, refundAmountStr)
}

// calculateRefundRate 内部计算退单率
func calculateRefundRate(totalAmountStr, refundAmountStr string) float64 {
	var total, refund float64
	fmt.Sscanf(totalAmountStr, "%f", &total)
	fmt.Sscanf(refundAmountStr, "%f", &refund)

	if total == 0 {
		return 0
	}
	return (refund / total) * 100
}

// GetHourlyTrend 获取今日每小时订单金额趋势（当前简化版）
// 后续可改为 RedisTimeSeries 或按小时 Hash 实现
//
// JSON Sample:
//
//	{
//	  "08": 1240.5,
//	  "09": 2350,
//	  "10": 1890.75,
//	  ...
//	}
func GetHourlyTrend() map[string]float64 {
	// 当前返回空 map，待你需要完整实现时再补充
	return make(map[string]float64)
}

// GetRefundByStore 获取各门店退单统计（类似门店订单）
//
// JSON Sample:
// [
//
//	{"Member": "STORE001", "Score": 450.0},   // Score 为退单金额
//	{"Member": "STORE002", "Score": 299}
//
// ]
func GetRefundByStore(date string, topN int64) []redis.Z {
	if date == "" {
		date = time.Now().Format("20060102")
	}
	if topN <= 0 {
		topN = 10
	}
	ctx := context.Background()
	result, _ := rdb.ZRevRangeWithScores(ctx, "refund:store:rank:"+date, 0, topN-1).Result() // 注意：需在 ProcessEvent 中维护该 Sorted Set
	return result
}
