package dbgen

type Region struct{}

func (r Region) loader() error {
	panic("implement me")
}

func sdNull(_ table, _ dssHuge) long {
	return 0
}
