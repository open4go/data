package stats

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	// ... 原有 import
)

var (
	// 注册相关
	regTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dashboard_registrations_total",
		Help: "Total number of user registrations",
	}, []string{"source"})

	regUnique = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dashboard_unique_users",
		Help: "Approximate unique users today (HyperLogLog)",
	})

	// 订单相关
	orderTotalAmount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dashboard_order_total_amount",
		Help: "Total order amount today",
	})
	orderTotalCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dashboard_order_total_count",
		Help: "Total order count today",
	})

	orderByStore = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_order_amount_by_store",
		Help: "Order amount by store",
	}, []string{"store_id"})

	// 退单相关
	refundTotalAmount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dashboard_refund_total_amount",
		Help: "Total refund amount today",
	})
	refundRate = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dashboard_refund_rate_percent",
		Help: "Refund rate in percent",
	})
)
