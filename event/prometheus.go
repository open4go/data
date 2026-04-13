package event

import (
	"context"
	"fmt"
	"github.com/open4go/data/db"
	"strconv"
)

// ====================================
//          Prometheus 指标更新
// ====================================

// updatePrometheusMetrics 实时更新 Prometheus 业务指标（Gauge）
// 注意：每小时趋势数据不需要在这里更新，趋势图通过独立 API 从 Redis 读取
func updatePrometheusMetrics(ctx context.Context, e Event) {
	rds := db.GetRedisDataHandler(ctx)

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

		// 更新今日总订单金额和总数（Gauge）
		if totalAmtStr, err := rds.Get(ctx, ns+"order:total:amount:"+date).Result(); err == nil {
			if amt, err := strconv.ParseFloat(totalAmtStr, 64); err == nil {
				orderTotalAmount.WithLabelValues(e.TenantID).Set(amt)
			}
		}
		if totalCntStr, err := rds.Get(ctx, ns+"order:total:count:"+date).Result(); err == nil {
			if cnt, err := strconv.ParseFloat(totalCntStr, 64); err == nil {
				orderTotalCount.WithLabelValues(e.TenantID).Set(cnt)
			}
		}

		// 按门店订单金额累计
		orderByStore.WithLabelValues(e.TenantID, storeID).Add(amount)

	case EventRefund:
		// 更新今日总退单金额
		if refundAmtStr, err := rds.Get(ctx, ns+"refund:total:amount:"+date).Result(); err == nil {
			if amt, err := strconv.ParseFloat(refundAmtStr, 64); err == nil {
				refundTotalAmount.WithLabelValues(e.TenantID).Set(amt)
			}
		}

		// 更新今日退单率
		rate := GetRefundRateWithTenant(ctx, e.TenantID)
		refundRate.WithLabelValues(e.TenantID).Set(rate)

	case EventPayment:
		// 可后续扩展支付成功相关指标
	case EventRecharge:
		// 可后续扩展充值相关指标
	}
}
