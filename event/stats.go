package event

import (
	"context"
	"fmt"
	"github.com/open4go/log"
	"time"

	"github.com/open4go/data/db"
)

type Event struct {
	EventType string                 `json:"event_type"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
	// 新增：支持多商户
	TenantID string `json:"tenant_id"`
}

// ====================================
//
//	事件处理（统计核心）
//
// ====================================

func ProcessEvent(ctx context.Context, e Event) {

	log.Log(ctx).WithField("e", e).Debug("before process event")
	if e.TenantID == "" {
		e.TenantID = "default" // 防止空 tenant
	}
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
		//{
		//  "event_type": "order",
		//  "tenant_id": "TENANT001",
		//  "data": {
		//    "order_id": "ORD123",
		//    "store_id": "STORE001",
		//    "amount": 599.8,
		//    "items": [
		//      {
		//        "product_id": "P001",
		//        "product_name": "商品A",
		//        "quantity": 2,
		//        "price": 199.9
		//      },
		//      {
		//        "product_id": "P002",
		//        "product_name": "商品B",
		//        "quantity": 1,
		//        "price": 199.9
		//      }
		//    ]
		//  }
		//}
		storeID := fmt.Sprintf("%v", e.Data["store_id"])
		amount := getFloat(e.Data, "amount")

		storeKey := ns + "order:store:" + storeID + ":" + date
		pipe.HIncrBy(ctx, storeKey, "count", 1)
		pipe.HIncrByFloat(ctx, storeKey, "amount", amount)
		pipe.ZIncrBy(ctx, ns+"order:amount:rank:"+date, amount, storeID)

		pipe.IncrByFloat(ctx, ns+"order:total:amount:"+date, amount)
		pipe.Incr(ctx, ns+"order:total:count:"+date)

		// ==================== 商品销量统计（优化版） ====================
		if items, ok := e.Data["items"].([]interface{}); ok {
			for _, item := range items {
				log.Log(ctx).WithField("item", item).Debug("current item")
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				log.Log(ctx).WithField("itemMap", itemMap).Debug("current itemMap")
				productID := fmt.Sprintf("%v", itemMap["product_id"])
				if productID == "" {
					continue
				}
				log.Log(ctx).WithField("productID", productID).Debug("current productID")
				quantity := getFloat(itemMap, "quantity")
				price := getFloat(itemMap, "price")
				log.Log(ctx).WithField("quantity", quantity).Debug("current quantity")
				if quantity <= 0 {
					continue
				}

				// 使用统一的日期+productID key
				productKey := date + ":" + productID
				log.Log(ctx).WithField("productKey", quantity).Debug("current productKey")
				// 商品总销量（数量）
				pipe.IncrByFloat(ctx, ns+"product:sales:qty:"+productKey, quantity)

				// 商品总销售额
				pipe.IncrByFloat(ctx, ns+"product:sales:amount:"+productKey, quantity*price)

				// 商品销量排行（按数量）
				pipe.ZIncrBy(ctx, ns+"product:sales:rank:qty:"+date, quantity, productID)

				// 商品销售额排行（按金额）
				pipe.ZIncrBy(ctx, ns+"product:sales:rank:amount:"+date, quantity*price, productID)
			}
		}

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
		hourStr := e.Timestamp.Format("20060102_15")
		pipe.Incr(ctx, ns+"refund:hourly:count:"+hourStr)
		pipe.IncrByFloat(ctx, ns+"refund:hourly:amount:"+hourStr, amount)

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
