package event

import (
	"context"
	"encoding/json"
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
		storeID := fmt.Sprintf("%v", e.Data["store_id"])
		amount := getFloat(e.Data, "amount")

		storeKey := ns + "order:store:" + storeID + ":" + date
		pipe.HIncrBy(ctx, storeKey, "count", 1)
		pipe.HIncrByFloat(ctx, storeKey, "amount", amount)
		pipe.ZIncrBy(ctx, ns+"order:amount:rank:"+date, amount, storeID)

		pipe.IncrByFloat(ctx, ns+"order:total:amount:"+date, amount)
		pipe.Incr(ctx, ns+"order:total:count:"+date)

		// ==================== 商品销量统计（使用结构体版） ====================
		if itemsRaw, ok := e.Data["items"]; ok {
			log.Log(ctx).WithField("itemsRaw_type", fmt.Sprintf("%T", itemsRaw)).Debug("items raw type")

			var productItems []ProductItem

			// 支持两种常见格式
			switch v := itemsRaw.(type) {
			case []interface{}:
				log.Log(ctx).Debug("解析为 []interface{}")
				for _, item := range v {
					if data, err := json.Marshal(item); err == nil {
						var p ProductItem
						if json.Unmarshal(data, &p) == nil {
							productItems = append(productItems, p)
						}
					}
				}
			case []map[string]interface{}:
				log.Log(ctx).Debug("解析为 []map[string]interface{}")
				for _, m := range v {
					if data, err := json.Marshal(m); err == nil {
						var p ProductItem
						if json.Unmarshal(data, &p) == nil {
							productItems = append(productItems, p)
						}
					}
				}
			case []ProductItem: // 直接是结构体切片的情况
				productItems = v
			}

			log.Log(ctx).WithField("count", len(productItems)).Info("解析后的商品数量")

			for _, item := range productItems {
				if item.ProductID == "" || item.Quantity <= 0 {
					continue
				}

				log.Log(ctx).WithField("product_id", item.ProductID).
					WithField("quantity", item.Quantity).
					WithField("product_name", item.ProductName).
					WithField("price", item.Price).Debug("处理商品")
				productKey := date + ":" + item.ProductID

				// 商品总销量（数量）
				pipe.IncrByFloat(ctx, ns+"product:sales:qty:"+productKey, item.Quantity)

				// 商品总销售额
				pipe.IncrByFloat(ctx, ns+"product:sales:amount:"+productKey, item.Quantity*item.Price)

				// 商品销量排行（按数量）
				pipe.ZIncrBy(ctx, ns+"product:sales:rank:qty:"+date, item.Quantity, item.ProductID)

				// 商品销售额排行（按金额）
				pipe.ZIncrBy(ctx, ns+"product:sales:rank:amount:"+date, item.Quantity*item.Price, item.ProductID)
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
