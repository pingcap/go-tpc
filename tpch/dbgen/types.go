package dbgen

const (
	TNone Table = iota - 1
	TPart
	TPsupp
	TSupp
	TCust
	TOrder
	TLine
	TOrderLine
	TPartPsupp
	TNation
	TRegion
)

type Loader interface {
	Load(item interface{}) error
	Flush() error
}
