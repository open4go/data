package event

import (
	"fmt"
	"time"
)

// ====================================
//          数据模拟生成函数（支持多租户 + 商品信息）
// ====================================

// SimulateEvent 生成模拟事件并立即调用 ProcessEvent
func SimulateEvent(eventType string, tenantID string) {
	if tenantID == "" {
		tenantID = "default"
	}

	event := GenerateEvent(eventType, tenantID)
	ProcessEvent(event)

	fmt.Printf("✅ 已模拟生成事件: %s | Tenant: %s | 时间: %s\n",
		eventType, tenantID, event.Timestamp.Format("2006-01-02 15:04:05"))
}

// GenerateEvent 生成模拟事件数据（新增商品信息）
func GenerateEvent(eventType string, tenantID string) Event {
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
		// 商品库（更丰富）
		products := []map[string]interface{}{
			{"product_id": "P001", "product_name": "无线耳机", "price": 199.0},
			{"product_id": "P002", "product_name": "机械键盘", "price": 399.0},
			{"product_id": "P003", "product_name": "智能手表", "price": 899.0},
			{"product_id": "P004", "product_name": "咖啡机", "price": 599.0},
			{"product_id": "P005", "product_name": "保温杯", "price": 89.0},
			{"product_id": "P006", "product_name": "笔记本电脑支架", "price": 129.0},
			{"product_id": "P007", "product_name": "手机壳", "price": 49.0},
			{"product_id": "P008", "product_name": "充电宝", "price": 159.0},
		}

		// 随机生成 1~4 个商品
		itemCount := 1 + int(now.UnixNano()%4)
		items := make([]map[string]interface{}, 0, itemCount)

		totalAmount := 0.0
		for i := 0; i < itemCount; i++ {
			p := products[int(now.UnixNano())%len(products)]
			quantity := 1 + int(now.UnixNano()%5) // 1~5件

			item := map[string]interface{}{
				"product_id":   p["product_id"],
				"product_name": p["product_name"],
				"quantity":     float64(quantity),
				"price":        p["price"],
			}
			items = append(items, item)

			totalAmount += p["price"].(float64) * float64(quantity)
		}

		return Event{
			EventType: EventOrder,
			TenantID:  tenantID,
			Timestamp: now,
			Data: map[string]interface{}{
				"order_id": fmt.Sprintf("ORD_%d", now.UnixNano()%1000000),
				"store_id": stores[now.UnixNano()%int64(len(stores))],
				"amount":   totalAmount,
				"items":    items, // 新增商品列表
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
		return GenerateEvent(EventRegistration, tenantID)
	}
}

// SimulateBatch 批量模拟（保持不变）
func SimulateBatch(count int, tenantID string) {
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

		SimulateEvent(eventType, tenantID)

		if i%20 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	fmt.Println("✅ 批量模拟完成！")
}

// SimulateTodayData 快速生成今日测试数据（订单会包含丰富商品信息）
func SimulateTodayData(tenantID string) {
	if tenantID == "" {
		tenantID = "default"
	}

	fmt.Printf("🎯 开始生成今日测试数据 | Tenant: %s...\n", tenantID)

	// 注册事件
	for i := 0; i < 45; i++ {
		SimulateEvent(EventRegistration, tenantID)
	}

	// 下单事件（重点！每个订单都会带商品信息）
	for i := 0; i < 80; i++ { // 增加到80条，让商品数据更丰富
		SimulateEvent(EventOrder, tenantID)
	}

	// 退单事件
	for i := 0; i < 15; i++ {
		SimulateEvent(EventRefund, tenantID)
	}

	// 充值事件
	for i := 0; i < 15; i++ {
		SimulateEvent(EventRecharge, tenantID)
	}

	fmt.Println("✅ 今日测试数据生成完成！（订单已包含商品信息）")
	fmt.Println("   → 商品销量排行榜已可正常统计")
}
