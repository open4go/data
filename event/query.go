package event

import (
	"context"
	"fmt"
	"github.com/open4go/data/db"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

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
	_, _ = fmt.Sscanf(totalAmountStr, "%f", &total)
	_, _ = fmt.Sscanf(refundAmountStr, "%f", &refund)
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

// GetHourlyRefundRateTrend 获取最近 N 小时的退单率趋势（订单 vs 退单比率）
//
// 返回 JSON Sample:
//
//	{
//	  "hours": ["20260413_14", "20260413_15", "20260413_16", ...],
//	  "orderCount": [23, 41, 38, ...],
//	  "refundCount": [2, 5, 3, ...],
//	  "refundRate": [8.7, 12.2, 7.9, ...]     // 单位：百分比
//	}
func GetHourlyRefundRateTrend(ctx context.Context, tenantID string, hours int) map[string]interface{} {
	rdb := db.GetRedisDataHandler(ctx)
	if tenantID == "" {
		tenantID = "default"
	}
	if hours <= 0 || hours > 168 {
		hours = 24
	}
	ns := tenantID + ":"

	result := make(map[string]interface{})
	hourLabels := make([]string, hours)
	orderCount := make([]float64, hours)
	refundCount := make([]float64, hours)
	refundRate := make([]float64, hours)

	now := time.Now()

	for i := 0; i < hours; i++ {
		hourStr := now.Add(time.Duration(-i) * time.Hour).Format("20060102_15")
		hourLabels[hours-1-i] = hourStr

		// 每小时订单数
		orderStr, _ := rdb.Get(ctx, ns+"order:hourly:count:"+hourStr).Result()
		orderCnt, _ := strconv.ParseFloat(orderStr, 64)

		// 每小时退单数（需在 ProcessEvent 中增加退单每小时统计）
		refundStr, _ := rdb.Get(ctx, ns+"refund:hourly:count:"+hourStr).Result()
		refundCnt, _ := strconv.ParseFloat(refundStr, 64)

		orderCount[hours-1-i] = orderCnt
		refundCount[hours-1-i] = refundCnt

		// 计算退单率
		if orderCnt > 0 {
			refundRate[hours-1-i] = (refundCnt / orderCnt) * 100
		} else {
			refundRate[hours-1-i] = 0
		}
	}

	result["hours"] = hourLabels
	result["orderCount"] = orderCount
	result["refundCount"] = refundCount
	result["refundRate"] = refundRate

	return result
}

// GetProductSalesRank 获取指定日期的商品销量排行榜
//
// JSON Sample:
// [
//
//	{"Member": "P001", "Score": 45},     // Score = 销量数量
//	{"Member": "P005", "Score": 32}
//
// ]
func GetProductSalesRank(ctx context.Context, tenantID, date string, topN int64, byAmount bool) []redis.Z {
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

	var key string
	if byAmount {
		key = ns + "product:sales:rank:amount:" + date // 按销售额排行
	} else {
		key = ns + "product:sales:rank:qty:" + date // 按销量数量排行（默认）
	}

	result, _ := rdb.ZRevRangeWithScores(ctx, key, 0, topN-1).Result()
	return result
}

// GetProductSalesTrend 获取某商品最近 N 天的销量趋势（可选）
func GetProductSalesTrend(tenantID, productID string, days int) map[string]interface{} {
	// 可后续实现，这里先留空
	return map[string]interface{}{"product_id": productID, "data": []float64{}}
}
