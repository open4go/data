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

		products := []struct {
			ID    string
			Name  string
			Price float64
		}{
			{"P001", "无线耳机", 199.0},
			{"P002", "机械键盘", 399.0},
			{"P003", "智能手表", 899.0},
			{"P004", "咖啡机", 599.0},
			{"P005", "保温杯", 89.0},
			{"P006", "笔记本支架", 129.0},
			{"P007", "手机壳", 49.0},
			{"P008", "充电宝", 159.0},
		}

		itemCount := 1 + int(now.UnixNano()%4)
		items := make([]map[string]interface{}, 0, itemCount)
		totalAmount := 0.0

		for i := 0; i < itemCount; i++ {
			p := products[int(now.UnixNano())%len(products)]
			qty := 1 + int(now.UnixNano()%5)

			item := map[string]interface{}{
				"product_id":   p.ID,
				"product_name": p.Name,
				"quantity":     float64(qty),
				"price":        p.Price,
			}
			items = append(items, item)
			totalAmount += p.Price * float64(qty)
		}

		return Event{
			EventType: EventOrder,
			TenantID:  tenantID,
			Timestamp: now,
			Data: map[string]interface{}{
				"order_id": fmt.Sprintf("ORD_%d", now.UnixNano()%1000000),
				"store_id": stores[now.UnixNano()%int64(len(stores))],
				"amount":   totalAmount,
				"items":    items,
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
