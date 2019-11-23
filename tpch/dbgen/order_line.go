package dbgen

type OrderLine struct {
	order Order
}

func (o OrderLine) loader() error {
	panic("implement me")
}
