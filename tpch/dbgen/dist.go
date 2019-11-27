package dbgen

import "github.com/pingcap/go-tpc/tpch/dbgen/dist"

var (
	nations      distribution
	nations2     distribution
	regions      distribution
	oPrioritySet distribution
	lInstructSet distribution
	lSmodeSet    distribution
	lCategorySet distribution
	lRflagSet    distribution
	cMsegSet     distribution
	colors       distribution
	pTypesSet    distribution
	pCntrSet     distribution
	articles     distribution
	nouns        distribution
	adjectives   distribution
	adverbs      distribution
	prepositions distribution
	verbs        distribution
	terminators  distribution
	auxillaries  distribution
	np           distribution
	vp           distribution
	grammar      distribution
)

type setMember struct {
	weight long
	text   string
}

type distribution struct {
	count   int
	max     int32
	members []setMember
	permute []long
}

func readDist(name string, d *distribution) {
	dist := dist.Maps[name]
	d.count = len(dist)
	for _, item := range dist {
		d.max += item.Weight
		d.members = append(d.members, setMember{text: item.Text, weight: long(d.max)})
	}
}

func permute(permute []long, count int, stream long) {
	for i := 0; i < count; i++ {
		source := random(dssHuge(i), dssHuge(count-1), stream)
		permute[source], permute[i] = permute[i], permute[source]
	}
}

func permuteDist(dist *distribution, stream long) {
	if len(dist.permute) == 0 {
		dist.permute = make([]long, dist.count)
	}
	for i := 0; i < dist.count; i++ {
		dist.permute[i] = long(i)
	}
	permute(dist.permute, dist.count, stream)
}

func initDists() {
	readDist("p_cntr", &pCntrSet)
	readDist("colors", &colors)
	readDist("p_types", &pTypesSet)
	readDist("nations", &nations)
	readDist("regions", &regions)
	readDist("o_oprio", &oPrioritySet)
	readDist("instruct", &lInstructSet)
	readDist("smode", &lSmodeSet)
	readDist("category", &lCategorySet)
	readDist("rflag", &lRflagSet)
	readDist("msegmnt", &cMsegSet)
	readDist("nouns", &nouns)
	readDist("verbs", &verbs)
	readDist("adjectives", &adjectives)
	readDist("adverbs", &adverbs)
	readDist("auxillaries", &auxillaries)
	readDist("terminators", &terminators)
	readDist("articles", &articles)
	readDist("prepositions", &prepositions)
	readDist("grammar", &grammar)
	readDist("np", &np)
	readDist("vp", &vp)
}
