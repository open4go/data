package event

import (
	"fmt"
	"time"
)

// ====================================
//          数据模拟生成函数（支持多租户）
// ====================================

// SimulateEvent 生成模拟事件并立即调用 ProcessEvent
// 参数说明：
//
//	eventType: 事件类型（如 EventOrder）
//	tenantID:  商户ID（必填，支持多租户）
func SimulateEvent(eventType string, tenantID string) {
	if tenantID == "" {
		tenantID = "default"
	}

	event := GenerateEvent(eventType, tenantID)
	ProcessEvent(event)

	fmt.Printf("✅ 已模拟生成事件: %s | Tenant: %s | 时间: %s\n",
		eventType, tenantID, event.Timestamp.Format("2006-01-02 15:04:05"))
}

// GenerateEvent 生成模拟事件数据（支持 tenantID）
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
		amounts := []float64{99.9, 199.0, 299.5, 499.0, 1299.0, 89.9}
		return Event{
			EventType: EventOrder,
			TenantID:  tenantID,
			Timestamp: now,
			Data: map[string]interface{}{
				"order_id": fmt.Sprintf("ORD_%d", now.UnixNano()%1000000),
				"store_id": stores[now.UnixNano()%int64(len(stores))],
				"amount":   amounts[now.UnixNano()%int64(len(amounts))],
			},
		}

	case EventRefund:
		stores := []string{"STORE001", "STORE002", "STORE003", "STORE004"}
		amounts := []float64{99.9, 199.0, 299.5, 499.0}
		return Event{
			EventType: EventRefund,
			TenantID:  tenantID,
			Timestamp: now,
			Data: map[string]interface{}{
				"order_id": fmt.Sprintf("ORD_%d", now.UnixNano()%1000000),
				"store_id": stores[now.UnixNano()%int64(len(stores))],
				"amount":   amounts[now.UnixNano()%int64(len(amounts))],
				"reason":   "用户取消",
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
				"method":   "wechat",
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

// SimulateBatch 批量模拟生成指定数量的事件（支持 tenantID）
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

// SimulateTodayData 快速生成今日测试数据（支持多租户）
func SimulateTodayData(tenantID string) {
	if tenantID == "" {
		tenantID = "default"
	}

	fmt.Printf("🎯 开始生成今日测试数据 | Tenant: %s...\n", tenantID)

	// 注册事件（45条）
	for i := 0; i < 45; i++ {
		SimulateEvent(EventRegistration, tenantID)
	}

	// 下单事件（68条）
	for i := 0; i < 68; i++ {
		SimulateEvent(EventOrder, tenantID)
	}

	// 退单事件（12条）
	for i := 0; i < 12; i++ {
		SimulateEvent(EventRefund, tenantID)
	}

	// 充值事件（15条）
	for i := 0; i < 15; i++ {
		SimulateEvent(EventRecharge, tenantID)
	}

	fmt.Println("✅ 今日测试数据生成完成！可刷新前端查看效果")
}
