package dbgen

type Loader interface {
	loader() error
}

const (
	NONE       table = -1
	PART       table = iota
	PSUPP            = 1
	SUPP             = 2
	CUST             = 3
	ORDER            = 4
	LINE             = 5
	ORDER_LINE       = 6
	PART_PSUPP       = 7
	NATION           = 8
	REGION           = 9
	UPDATE           = 10
	MAX_TABLE        = 1
)

