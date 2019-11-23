package dbgen

var (
	nations      *distribution
	nations2     *distribution
	regions      *distribution
	oPrioritySet *distribution
	lInstructSet *distribution
	lSmodeSet    *distribution
	lCategorySet *distribution
	lRflagSet    *distribution
	cMsegSet     *distribution
	colors       *distribution
	pTypesSet    *distribution
	pCntrSet     *distribution
)

type setMember struct {
	weight long
	text   string
}

type distribution struct {
	count   int
	max     int
	members []setMember
	permute []long
}

func init() {

}
