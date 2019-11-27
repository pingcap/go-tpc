package dbgen

type OrderLine struct {
	order Order
}

type orderLineLoader struct {
}

func (o orderLineLoader) Load(item interface{}) error {
	if err := tDefs[TOrder].loader.Load(item); err != nil {
		return err
	}
	if err := tDefs[TOrder].loader.Flush(); err != nil {
		return nil
	}
	if err := tDefs[TLine].loader.Load(item); err != nil {
		return err
	}
	return nil
}

func (o orderLineLoader) Flush() error {
	if err := tDefs[TLine].loader.Flush(); err != nil {
		return err
	}
	return nil
}

func newOrderLineLoader() orderLineLoader {
	return orderLineLoader{}
}
