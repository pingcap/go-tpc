package dbgen

type OrderLine struct {
	order Order
}

var _orderLineLoader = func(order interface{}) error {
	if err := (*orderLoader)(order); err != nil {
		return err
	}
	if err := (*lineItemLoader)(order); err != nil {
		return err
	}
	return nil
}

var orderLineLoader = &_orderLineLoader
