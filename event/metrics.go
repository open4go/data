package event

import "github.com/prometheus/client_golang/prometheus/promauto"
import "github.com/prometheus/client_golang/prometheus"

// 自定义 Registry，只暴露业务指标
var (
	Registry = prometheus.NewRegistry()

	// ==================== 业务指标定义 ====================
	regTotal = promauto.With(Registry).NewCounterVec(prometheus.CounterOpts{
		Name: "dashboard_registrations_total",
		Help: "Total number of user registrations by source",
	}, []string{"tenant_id", "source"})

	orderTotalAmount = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_order_total_amount",
		Help: "Total order amount today",
	}, []string{"tenant_id"})

	orderTotalCount = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_order_total_count",
		Help: "Total order count today",
	}, []string{"tenant_id"})

	orderByStore = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_order_amount_by_store",
		Help: "Order amount by store",
	}, []string{"tenant_id", "store_id"})

	refundTotalAmount = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_refund_total_amount",
		Help: "Total refund amount today",
	}, []string{"tenant_id"})

	refundRate = promauto.With(Registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "dashboard_refund_rate_percent",
		Help: "Refund rate in percent",
	}, []string{"tenant_id"})
)
