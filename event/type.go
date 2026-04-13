package event

// 事件类型常量（强烈建议使用常量，避免字符串拼写错误）
const (
	EventRegistration = "registration"
	EventOrder        = "order"
	EventPayment      = "payment"
	EventRefund       = "refund"
	EventRecharge     = "recharge"
)
