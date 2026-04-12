package event

import (
	"fmt"
	"time"
)

// ====================================
//          数据模拟生成函数（用于测试）
// ====================================

// SimulateEvent 生成模拟事件并立即调用 ProcessEvent 写入 Redis + MongoDB
// 参数：eventType 使用 stats 包中定义的常量
func SimulateEvent(eventType string) {
	event := GenerateEvent(eventType)
	ProcessEvent(event)
	fmt.Printf("✅ 已模拟生成事件: %s | 时间: %s\n", eventType, event.Timestamp.Format("2006-01-02 15:04:05"))
}

// GenerateEvent 生成模拟事件数据（不立即处理，仅返回 Event）
func GenerateEvent(eventType string) Event {
	now := time.Now().UTC()

	switch eventType {
	case EventRegistration:
		sources := []string{"wechat", "app", "miniapp", "web", "ios"}
		return Event{
			EventType: EventRegistration,
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
			Timestamp: now,
			Data: map[string]interface{}{
				"user_id": fmt.Sprintf("user_%d", now.UnixNano()%100000),
				"amount":  []float64{100, 500, 1000, 2000}[now.UnixNano()%4],
				"channel": channels[now.UnixNano()%int64(len(channels))],
			},
		}

	default:
		// 默认返回一个注册事件
		return GenerateEvent(EventRegistration)
	}
}

// SimulateBatch 批量模拟生成指定数量的事件（适合压力测试）
func SimulateBatch(count int) {
	fmt.Printf("🚀 开始批量模拟 %d 个事件...\n", count)

	for i := 0; i < count; i++ {
		// 按一定比例随机生成不同类型事件
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

		SimulateEvent(eventType)

		// 适当延时，避免瞬间产生过多数据（可选）
		if i%20 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	fmt.Println("✅ 批量模拟完成！")
}

// SimulateTodayData 快速生成今天的基础测试数据（推荐首次启动时调用）
func SimulateTodayData() {
	fmt.Println("🎯 开始生成今日测试数据...")

	// 注册事件
	for i := 0; i < 45; i++ {
		SimulateEvent(EventRegistration)
	}

	// 下单事件
	for i := 0; i < 68; i++ {
		SimulateEvent(EventOrder)
	}

	// 退单事件
	for i := 0; i < 12; i++ {
		SimulateEvent(EventRefund)
	}

	// 充值事件
	for i := 0; i < 15; i++ {
		SimulateEvent(EventRecharge)
	}

	fmt.Println("✅ 今日测试数据生成完成！可刷新前端查看效果")
}
