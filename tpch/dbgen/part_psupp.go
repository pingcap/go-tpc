package dbgen

type partPsuppLoader struct{}

func (p partPsuppLoader) Load(item interface{}) error {
	if err := tDefs[TPart].loader.Load(item); err != nil {
		return err
	}
	if err := tDefs[TPsupp].loader.Load(item); err != nil {
		return err
	}
	return nil
}

func (p partPsuppLoader) Flush() error {
	return nil
}

func newPartPsuppLoader() partPsuppLoader {
	return partPsuppLoader{}
}
