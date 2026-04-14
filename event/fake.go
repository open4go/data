package event

import (
	"context"
	"fmt"
	"time"

	"github.com/open4go/log"
)

// ====================================
//          数据模拟生成函数（支持多租户 + 商品结构体）
// ====================================

// SimulateEvent 生成模拟事件并立即调用 ProcessEvent
func SimulateEvent(ctx context.Context, eventType string, tenantID string) {
	if tenantID == "" {
		tenantID = "default"
	}

	event := GenerateEvent(ctx, eventType, tenantID)
	ProcessEvent(ctx, event)

	fmt.Printf("✅ 已模拟生成事件: %s | Tenant: %s | 时间: %s\n",
		eventType, tenantID, event.Timestamp.Format("2006-01-02 15:04:05"))
}

// GenerateEvent 生成模拟事件数据（使用结构体生成商品）
func GenerateEvent(ctx context.Context, eventType string, tenantID string) Event {
	if tenantID == "" {
		tenantID = "default"
	}

	now := time.Now().UTC()

	switch eventType {
	case EventRegistration:
		sources := []string{"wechat", "app", "miniapp", "web", "ios"}
		return Event{
			EventType: EventRegistration,
			TenantID:  tenantID,
			Timestamp: now,
			Data: map[string]interface{}{
				"user_id": fmt.Sprintf("user_%d", now.UnixNano()%100000),
				"source":  sources[now.UnixNano()%int64(len(sources))],
			},
		}

	case EventOrder:
		stores := []string{"STORE001", "STORE002", "STORE003", "STORE004", "STORE005"}

		// 商品库（使用结构体）
		products := []ProductItem{
			{ProductID: "P001", ProductName: "无线耳机", Price: 199.0},
			{ProductID: "P002", ProductName: "机械键盘", Price: 399.0},
			{ProductID: "P003", ProductName: "智能手表", Price: 899.0},
			{ProductID: "P004", ProductName: "咖啡机", Price: 599.0},
			{ProductID: "P005", ProductName: "保温杯", Price: 89.0},
			{ProductID: "P006", ProductName: "笔记本支架", Price: 129.0},
			{ProductID: "P007", ProductName: "手机壳", Price: 49.0},
			{ProductID: "P008", ProductName: "充电宝", Price: 159.0},
		}

		itemCount := 1 + int(now.UnixNano()%4) // 随机 1~4 个商品
		items := make([]ProductItem, 0, itemCount)
		totalAmount := 0.0

		for i := 0; i < itemCount; i++ {
			p := products[int(now.UnixNano())%len(products)]
			qty := 1 + int(now.UnixNano()%5) // 数量 1~5

			item := ProductItem{
				ProductID:   p.ProductID,
				ProductName: p.ProductName,
				Quantity:    float64(qty),
				Price:       p.Price,
			}

			items = append(items, item)
			totalAmount += p.Price * float64(qty)

			log.Log(ctx).WithField("product_id", item.ProductID).
				WithField("quantity", item.Quantity).
				WithField("product_name", item.ProductName).
				WithField("price", item.Price).Debug("生成商品项")

		}

		log.Log(ctx).WithField("item_count", len(items)).WithField("total_amount", totalAmount).Debug("生成订单完成")

		// 将 []ProductItem 转为 []interface{}，以兼容 ProcessEvent 中的 map 处理
		itemsInterface := make([]interface{}, len(items))
		for i, item := range items {
			itemsInterface[i] = item
		}

		return Event{
			EventType: EventOrder,
			TenantID:  tenantID,
			Timestamp: now,
			Data: map[string]interface{}{
				"order_id": fmt.Sprintf("ORD_%d", now.UnixNano()%1000000),
				"store_id": stores[now.UnixNano()%int64(len(stores))],
				"amount":   totalAmount,
				"items":    itemsInterface, // 转为 interface{} 兼容现有逻辑
			},
		}

	case EventRefund:
		stores := []string{"STORE001", "STORE002", "STORE003", "STORE004"}
		amounts := []float64{99.9, 199.0, 299.5, 499.0, 599.0}
		return Event{
			EventType: EventRefund,
			TenantID:  tenantID,
			Timestamp: now,
			Data: map[string]interface{}{
				"order_id": fmt.Sprintf("ORD_%d", now.UnixNano()%1000000),
				"store_id": stores[now.UnixNano()%int64(len(stores))],
				"amount":   amounts[now.UnixNano()%int64(len(amounts))],
				"reason":   []string{"用户取消", "尺寸不合适", "质量问题", "重复下单"}[now.UnixNano()%4],
			},
		}

	case EventPayment:
		return Event{
			EventType: EventPayment,
			TenantID:  tenantID,
			Timestamp: now,
			Data: map[string]interface{}{
				"order_id": fmt.Sprintf("ORD_%d", now.UnixNano()%1000000),
				"amount":   299.5,
				"method":   []string{"wechat", "alipay", "credit_card"}[now.UnixNano()%3],
			},
		}

	case EventRecharge:
		channels := []string{"wechat", "alipay", "bank"}
		return Event{
			EventType: EventRecharge,
			TenantID:  tenantID,
			Timestamp: now,
			Data: map[string]interface{}{
				"user_id": fmt.Sprintf("user_%d", now.UnixNano()%100000),
				"amount":  []float64{100, 500, 1000, 2000}[now.UnixNano()%4],
				"channel": channels[now.UnixNano()%int64(len(channels))],
			},
		}

	default:
		return GenerateEvent(ctx, EventRegistration, tenantID)
	}
}

// SimulateBatch 批量模拟
func SimulateBatch(ctx context.Context, count int, tenantID string) {
	if tenantID == "" {
		tenantID = "default"
	}

	fmt.Printf("🚀 开始批量模拟 %d 个事件 | Tenant: %s...\n", count, tenantID)

	for i := 0; i < count; i++ {
		r := i % 10
		var eventType string
		switch {
		case r < 3:
			eventType = EventRegistration
		case r < 7:
			eventType = EventOrder
		case r < 9:
			eventType = EventRefund
		default:
			eventType = EventRecharge
		}

		SimulateEvent(ctx, eventType, tenantID)

		if i%20 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	fmt.Println("✅ 批量模拟完成！")
}

// SimulateTodayData 快速生成今日测试数据
func SimulateTodayData(ctx context.Context, tenantID string) {
	if tenantID == "" {
		tenantID = "default"
	}

	fmt.Printf("🎯 开始生成今日测试数据 | Tenant: %s...\n", tenantID)

	// 注册事件
	for i := 0; i < 45; i++ {
		SimulateEvent(ctx, EventRegistration, tenantID)
	}

	// 下单事件（重点，每个订单都包含商品）
	for i := 0; i < 80; i++ {
		SimulateEvent(ctx, EventOrder, tenantID)
	}

	// 退单事件
	for i := 0; i < 15; i++ {
		SimulateEvent(ctx, EventRefund, tenantID)
	}

	// 充值事件
	for i := 0; i < 15; i++ {
		SimulateEvent(ctx, EventRecharge, tenantID)
	}

	fmt.Println("✅ 今日测试数据生成完成！（订单已包含商品信息）")
	fmt.Println("   → 商品销量排行榜已可正常统计")
}
