package dbgen

var _partPsuppLoader = func(part interface{}) error {
	if err := (*partLoader)(part); err != nil {
		return err
	}
	if err := (*partSuppLoader)(part); err != nil {
		return err
	}
	return nil
}
var partPsuppLoader = &_partPsuppLoader
