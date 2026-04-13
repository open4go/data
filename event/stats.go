package event

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/open4go/data/db"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
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
	// 新增：支持多商户
	TenantID string `json:"tenant_id"`
}

var (
	//rdb *redis.Client
	//mongoDB *mongo.Collection

	// ==================== Prometheus 指标（带 tenant 标签）================
	regTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dashboard_registrations_total",
		Help: "Total number of user registrations by source",
	}, []string{"tenant_id", "source"})

	orderTotalAmount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_order_total_amount",
		Help: "Total order amount today",
	}, []string{"tenant_id"})

	orderTotalCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_order_total_count",
		Help: "Total order count today",
	}, []string{"tenant_id"})

	orderByStore = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_order_amount_by_store",
		Help: "Order amount by store",
	}, []string{"tenant_id", "store_id"})

	refundTotalAmount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_refund_total_amount",
		Help: "Total refund amount today",
	}, []string{"tenant_id"})

	refundRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_refund_rate_percent",
		Help: "Refund rate in percent",
	}, []string{"tenant_id"})
)

// ====================================
//          事件处理（统计核心）
// ====================================

func ProcessEvent(e Event) {
	if e.TenantID == "" {
		e.TenantID = "default" // 防止空 tenant
	}

	ctx := context.Background()
	date := e.Timestamp.Format("20060102")
	ns := e.TenantID + ":" // namespace 前缀

	rds := db.GetRedisDataHandler(ctx)
	pipe := rds.Pipeline()

	switch e.EventType {
	case EventRegistration:
		source := fmt.Sprintf("%v", e.Data["source"])
		userID := fmt.Sprintf("%v", e.Data["user_id"])

		pipe.HIncrBy(ctx, ns+"reg:source:"+date+":"+source, "count", 1)
		pipe.PFAdd(ctx, ns+"reg:unique:hll:"+date, userID)
		pipe.ZIncrBy(ctx, ns+"reg:source:rank:"+date, 1, source)

	case EventOrder:
		storeID := fmt.Sprintf("%v", e.Data["store_id"])
		amount := getFloat(e.Data, "amount")

		storeKey := ns + "order:store:" + storeID + ":" + date
		pipe.HIncrBy(ctx, storeKey, "count", 1)
		pipe.HIncrByFloat(ctx, storeKey, "amount", amount)
		pipe.ZIncrBy(ctx, ns+"order:amount:rank:"+date, amount, storeID)

		pipe.IncrByFloat(ctx, ns+"order:total:amount:"+date, amount)
		pipe.Incr(ctx, ns+"order:total:count:"+date)

	case EventRefund:
		storeID := fmt.Sprintf("%v", e.Data["store_id"])
		amount := getFloat(e.Data, "amount")

		refundKey := ns + "refund:store:" + storeID + ":" + date
		pipe.HIncrBy(ctx, refundKey, "count", 1)
		pipe.HIncrByFloat(ctx, refundKey, "amount", amount)

		// 维护退单排行
		pipe.ZIncrBy(ctx, ns+"refund:store:rank:"+date, amount, storeID)

		pipe.IncrByFloat(ctx, ns+"refund:total:amount:"+date, amount)
		pipe.Incr(ctx, ns+"refund:total:count:"+date)

	case EventPayment, EventRecharge:
		// 后续扩展时同样加上 ns 前缀
	}

	_, _ = pipe.Exec(ctx)

	// 持久化到 MongoDB（建议也加上 tenant_id）
	//mongoDB.InsertOne(ctx, e)

	// 更新 Prometheus 指标（带 tenant_id 标签）
	updatePrometheusMetrics(ctx, e)
}

func getFloat(data map[string]interface{}, key string) float64 {
	if v, ok := data[key].(float64); ok {
		return v
	}
	return 0.0
}

// ====================================
//          Prometheus 指标更新
// ====================================

func updatePrometheusMetrics(ctx context.Context, e Event) {
	rdb := db.GetRedisDataHandler(ctx)
	if e.TenantID == "" {
		e.TenantID = "default"
	}
	date := e.Timestamp.Format("20060102")
	ns := e.TenantID + ":"

	switch e.EventType {
	case EventRegistration:
		source := fmt.Sprintf("%v", e.Data["source"])
		regTotal.WithLabelValues(e.TenantID, source).Inc()

	case EventOrder:
		storeID := fmt.Sprintf("%v", e.Data["store_id"])
		amount := getFloat(e.Data, "amount")

		// 更新总金额和总数
		if totalAmtStr, err := rdb.Get(ctx, ns+"order:total:amount:"+date).Result(); err == nil {
			if amt, err := strconv.ParseFloat(totalAmtStr, 64); err == nil {
				orderTotalAmount.WithLabelValues(e.TenantID).Set(amt)
			}
		}
		if totalCntStr, err := rdb.Get(ctx, ns+"order:total:count:"+date).Result(); err == nil {
			if cnt, err := strconv.ParseFloat(totalCntStr, 64); err == nil {
				orderTotalCount.WithLabelValues(e.TenantID).Set(cnt)
			}
		}

		orderByStore.WithLabelValues(e.TenantID, storeID).Add(amount)

	case EventRefund:
		if refundAmtStr, err := rdb.Get(ctx, ns+"refund:total:amount:"+date).Result(); err == nil {
			if amt, err := strconv.ParseFloat(refundAmtStr, 64); err == nil {
				refundTotalAmount.WithLabelValues(e.TenantID).Set(amt)
			}
		}
		rate := GetRefundRateWithTenant(ctx, e.TenantID)
		refundRate.WithLabelValues(e.TenantID).Set(rate)
	}
}

// ====================================
//          数据统计结果获取函数（全部支持 tenant）
// ====================================

// GetDashboardMetrics 获取今日仪表盘核心指标（需传入 tenantID）
func GetDashboardMetrics(ctx context.Context, tenantID string) map[string]interface{} {
	if tenantID == "" {
		tenantID = "default"
	}
	date := time.Now().Format("20060102")
	return GetMetricsByDate(ctx, tenantID, date)
}

// GetMetricsByDate 获取指定日期的统计指标
func GetMetricsByDate(ctx context.Context, tenantID, date string) map[string]interface{} {
	rdb := db.GetRedisDataHandler(ctx)
	if tenantID == "" {
		tenantID = "default"
	}
	ns := tenantID + ":"
	m := make(map[string]interface{})

	sources, _ := rdb.ZRevRangeWithScores(ctx, ns+"reg:source:rank:"+date, 0, 9).Result()
	m["regSources"] = sources

	stores, _ := rdb.ZRevRangeWithScores(ctx, ns+"order:amount:rank:"+date, 0, 9).Result()
	m["orderByStore"] = stores

	totalAmount, _ := rdb.Get(ctx, ns+"order:total:amount:"+date).Result()
	totalCount, _ := rdb.Get(ctx, ns+"order:total:count:"+date).Result()
	m["totalOrderAmount"] = totalAmount
	m["totalOrderCount"] = totalCount

	refundAmount, _ := rdb.Get(ctx, ns+"refund:total:amount:"+date).Result()
	refundCount, _ := rdb.Get(ctx, ns+"refund:total:count:"+date).Result()
	m["totalRefundAmount"] = refundAmount
	m["totalRefundCount"] = refundCount

	if totalAmount != "" && refundAmount != "" {
		m["refundRate"] = calculateRefundRate(totalAmount, refundAmount)
	}

	uniqueUsers, _ := rdb.PFCount(ctx, ns+"reg:unique:hll:"+date).Result()
	m["uniqueUsers"] = uniqueUsers

	return m
}

// GetRegistrationSources 支持 tenant
func GetRegistrationSources(ctx context.Context, tenantID, date string) []redis.Z {
	rdb := db.GetRedisDataHandler(ctx)
	if tenantID == "" {
		tenantID = "default"
	}
	if date == "" {
		date = time.Now().Format("20060102")
	}
	ns := tenantID + ":"
	result, _ := rdb.ZRevRangeWithScores(ctx, ns+"reg:source:rank:"+date, 0, -1).Result()
	return result
}

// GetOrderByStore 支持 tenant
func GetOrderByStore(ctx context.Context, tenantID, date string, topN int64) []redis.Z {
	rdb := db.GetRedisDataHandler(ctx)
	if tenantID == "" {
		tenantID = "default"
	}
	if date == "" {
		date = time.Now().Format("20060102")
	}
	if topN <= 0 {
		topN = 10
	}
	ns := tenantID + ":"
	result, _ := rdb.ZRevRangeWithScores(ctx, ns+"order:amount:rank:"+date, 0, topN-1).Result()
	return result
}

// GetRefundRateWithTenant 支持多租户的退单率
func GetRefundRateWithTenant(ctx context.Context, tenantID string) float64 {
	rdb := db.GetRedisDataHandler(ctx)

	if tenantID == "" {
		tenantID = "default"
	}
	date := time.Now().Format("20060102")
	ns := tenantID + ":"

	totalAmountStr, _ := rdb.Get(ctx, ns+"order:total:amount:"+date).Result()
	refundAmountStr, _ := rdb.Get(ctx, ns+"refund:total:amount:"+date).Result()

	return calculateRefundRate(totalAmountStr, refundAmountStr)
}

// GetRefundByStore 支持 tenant
func GetRefundByStore(ctx context.Context, tenantID, date string, topN int64) []redis.Z {
	rdb := db.GetRedisDataHandler(ctx)
	if tenantID == "" {
		tenantID = "default"
	}
	if date == "" {
		date = time.Now().Format("20060102")
	}
	if topN <= 0 {
		topN = 10
	}
	ns := tenantID + ":"
	result, _ := rdb.ZRevRangeWithScores(ctx, ns+"refund:store:rank:"+date, 0, topN-1).Result()
	return result
}

func calculateRefundRate(totalAmountStr, refundAmountStr string) float64 {
	var total, refund float64
	fmt.Sscanf(totalAmountStr, "%f", &total)
	fmt.Sscanf(refundAmountStr, "%f", &refund)
	if total == 0 {
		return 0
	}
	return (refund / total) * 100
}

// ====================================
//          趋势数据获取函数（最近 N 天）
// ====================================

// GetTrendMetrics 获取最近 N 天的关键指标趋势（推荐用于前端趋势图）
//
// 返回格式示例（JSON Sample）:
//
//	{
//	  "dates": ["20260407", "20260408", ..., "20260413"],
//	  "orderAmount": [12400.5, 15890.0, 21340.5, ..., 18900],
//	  "orderCount":   [89, 112, 156, ..., 134],
//	  "refundRate":   [3.2, 4.1, 5.61, ..., 2.8],
//	  "uniqueUsers":  [45, 62, 65, ..., 58],
//	  "regCount":     [23, 31, 28, ..., 35]
//	}
func GetTrendMetrics(ctx context.Context, tenantID string, days int) map[string]interface{} {
	rdb := db.GetRedisDataHandler(ctx)

	if tenantID == "" {
		tenantID = "default"
	}
	if days <= 0 || days > 90 {
		days = 7 // 默认最近7天
	}
	ns := tenantID + ":"

	result := make(map[string]interface{})
	dates := make([]string, days)
	orderAmount := make([]float64, days)
	orderCount := make([]float64, days)
	refundRateArr := make([]float64, days)
	uniqueUsers := make([]float64, days)
	regCount := make([]float64, days) // 可选：每日注册总数

	now := time.Now()
	for i := 0; i < days; i++ {
		d := now.AddDate(0, 0, -i).Format("20060102")
		dates[days-1-i] = d // 从旧到新排序，方便前端画图

		// 1. 订单总金额
		amtStr, _ := rdb.Get(ctx, ns+"order:total:amount:"+d).Result()
		amt, _ := strconv.ParseFloat(amtStr, 64)
		orderAmount[days-1-i] = amt

		// 2. 订单总数
		cntStr, _ := rdb.Get(ctx, ns+"order:total:count:"+d).Result()
		cnt, _ := strconv.ParseFloat(cntStr, 64)
		orderCount[days-1-i] = cnt

		// 3. 退单率
		refundAmtStr, _ := rdb.Get(ctx, ns+"refund:total:amount:"+d).Result()
		refundRateArr[days-1-i] = calculateRefundRate(amtStr, refundAmtStr)

		// 4. 唯一用户数
		u, _ := rdb.PFCount(ctx, ns+"reg:unique:hll:"+d).Result()
		uniqueUsers[days-1-i] = float64(u)

		// 5. 注册总数（从任意来源求和，或维护单独 counter）
		regTotal, _ := rdb.Get(ctx, ns+"reg:total:count:"+d).Result() // 需要在 ProcessEvent 中维护
		reg, _ := strconv.ParseFloat(regTotal, 64)
		regCount[days-1-i] = reg
	}

	result["dates"] = dates
	result["orderAmount"] = orderAmount
	result["orderCount"] = orderCount
	result["refundRate"] = refundRateArr
	result["uniqueUsers"] = uniqueUsers
	result["regCount"] = regCount

	return result
}

// GetWeeklySummary 获取本周 vs 上周的汇总对比（周环比）
// 返回示例：
//
//	{
//	  "thisWeekOrderAmount": 124500.75,
//	  "lastWeekOrderAmount": 98000.5,
//	  "weekOverWeekPercent": 27.04,     // 正数表示增长，负数表示下降
//	  "thisWeekOrderCount": 892,
//	  "lastWeekOrderCount": 745
//	}
func GetWeeklySummary(ctx context.Context, tenantID string) map[string]interface{} {
	rdb := db.GetRedisDataHandler(ctx)
	if tenantID == "" {
		tenantID = "default"
	}
	ns := tenantID + ":"

	now := time.Now()
	thisWeekAmount := 0.0
	thisWeekCount := 0.0
	lastWeekAmount := 0.0
	lastWeekCount := 0.0

	// 计算本周（最近7天）和上周（前7天）
	for i := 0; i < 7; i++ {
		// 本周日期
		thisDate := now.AddDate(0, 0, -i).Format("20060102")
		// 上周日期（比本周再往前7天）
		lastDate := now.AddDate(0, 0, -i-7).Format("20060102")

		// 本周订单金额
		amtStr, _ := rdb.Get(ctx, ns+"order:total:amount:"+thisDate).Result()
		amt, _ := strconv.ParseFloat(amtStr, 64)
		thisWeekAmount += amt

		// 本周订单数量
		cntStr, _ := rdb.Get(ctx, ns+"order:total:count:"+thisDate).Result()
		cnt, _ := strconv.ParseFloat(cntStr, 64)
		thisWeekCount += cnt

		// 上周订单金额
		lastAmtStr, _ := rdb.Get(ctx, ns+"order:total:amount:"+lastDate).Result()
		lastAmt, _ := strconv.ParseFloat(lastAmtStr, 64)
		lastWeekAmount += lastAmt

		// 上周订单数量
		lastCntStr, _ := rdb.Get(ctx, ns+"order:total:count:"+lastDate).Result()
		lastCnt, _ := strconv.ParseFloat(lastCntStr, 64)
		lastWeekCount += lastCnt
	}

	weekOverWeek := 0.0
	if lastWeekAmount > 0 {
		weekOverWeek = (thisWeekAmount - lastWeekAmount) / lastWeekAmount * 100
	}

	return map[string]interface{}{
		"thisWeekOrderAmount": thisWeekAmount,
		"thisWeekOrderCount":  thisWeekCount,
		"lastWeekOrderAmount": lastWeekAmount,
		"lastWeekOrderCount":  lastWeekCount,
		"weekOverWeekPercent": weekOverWeek, // 周环比百分比
		"weekOverWeekAmount":  thisWeekAmount - lastWeekAmount,
	}
}
