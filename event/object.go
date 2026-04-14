package event

// ProductItem 商品项结构体
type ProductItem struct {
	ProductID   string  `json:"product_id"`
	ProductName string  `json:"product_name"`
	Quantity    float64 `json:"quantity"`
	Price       float64 `json:"price"`
}
