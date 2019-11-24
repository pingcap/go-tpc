package dbgen

import "math"

const (
	maxStream             = 47
	RNG_A         dssHuge = 6364136223846793005
	RNG_C         dssHuge = 1
	MAX_LONG              = math.MaxInt32
	BITS_PER_LONG         = 32
	dM                    = 2147483647.0
)

var (
	multiplier = dssHuge(16807)
	modulus    = dssHuge(2147483647)
)

type Seed struct {
	table
	value    dssHuge
	usage    dssHuge
	boundary dssHuge
}

var seeds [maxStream + 1]Seed

func nextRand(nSeed dssHuge) dssHuge {
	return (nSeed * 16807) % 2147483647
}

func nextRand64(nSeed dssHuge) dssHuge {
	a := RNG_A
	c := RNG_C
	return nSeed*a + c
}

func unifInt(nLow dssHuge, nHigh dssHuge, nStream int) dssHuge {
	var dRange float64
	var nTemp dssHuge
	nLow32 := int32(nLow)
	nHigh32 := int32(nHigh)

	if nStream < 0 || nStream > maxStream {
		nStream = 0
	}
	if (nHigh == MAX_LONG) && (nLow == 0) {
		dRange = float64(nHigh32 - nLow32 + 1)
		_ = dssHuge(nHigh32 - nLow32 + 1)
	} else {
		dRange = float64(nHigh - nLow + 1)
		_ = nHigh - nLow + 1
	}
	seeds[nStream].value = nextRand(seeds[nStream].value)
	nTemp = dssHuge(float64(seeds[nStream].value) / dM * dRange)
	return nLow + nTemp
}

func random64(lower, upper dssHuge, nStream long) dssHuge {

	if nStream < 0 || nStream > maxStream {
		nStream = 0
	}
	if lower > upper {
		lower, upper = upper, lower
	}
	seeds[nStream].value = nextRand64(seeds[nStream].value)

	nTemp := seeds[nStream].value
	if nTemp < 0 {
		nTemp = -nTemp
	}
	nTemp %= upper - lower + 1
	seeds[nStream].usage += 1
	return lower + nTemp
}

func random(lower, upper dssHuge, nStream int) dssHuge {
	seeds[nStream].usage += 1
	return unifInt(lower, upper, nStream)
}

func advanceRand64(nSeed, nCount dssHuge) dssHuge {
	a := RNG_A
	c := RNG_C
	var nBit int
	aPow := a
	dSum := c
	if nCount == 0 {
		return nSeed
	}

	for nBit = 0; (nCount >> nBit) != RNG_C; nBit++ {
	}
	for {
		nBit -= 1
		if nBit < 0 {
			break
		}
		dSum *= aPow + 1
		aPow = aPow * aPow
		if (nCount>>nBit)%2 == 1 {
			dSum += aPow
			aPow *= a
		}
	}
	nSeed = nSeed*aPow + dSum*c
	return nSeed
}

func nthElement(n dssHuge, startSeed *dssHuge) {
	var z, mult dssHuge

	mult = multiplier
	z = *startSeed
	for n > 0 {
		if n%2 != 0 {
			z = (mult * z) % modulus
		}
		n = n / 2
		mult = (mult * mult) % modulus
	}
	*startSeed = z
}

func advanceStream(nStream int, nCalls dssHuge, bUse64Bit bool) {
	if bUse64Bit {
		seeds[nStream].value = advanceRand64(seeds[nStream].value, nCalls)
	} else {
		nthElement(nCalls, &seeds[nStream].value)
	}
}

func init() {
	seeds = [maxStream + 1]Seed{
		{PART, 1, 0, 1},
		{PART, 46831694, 0, 1},
		{PART, 1841581359, 0, 1},
		{PART, 1193163244, 0, 1},
		{PART, 727633698, 0, 1},
		{NONE, 933588178, 0, 1},
		{PART, 804159733, 0, 2},
		{PSUPP, 1671059989, 0, SUPP_PER_PART},
		{PSUPP, 1051288424, 0, SUPP_PER_PART},
		{PSUPP, 1961692154, 0, SUPP_PER_PART * 2},
		{ORDER, 1227283347, 0, 1},
		{ORDER, 1171034773, 0, 1},
		{ORDER, 276090261, 0, 2},
		{ORDER, 1066728069, 0, 1},
		{LINE, 209208115, 0, O_LCNT_MAX},
		{LINE, 554590007, 0, O_LCNT_MAX},
		{LINE, 721958466, 0, O_LCNT_MAX},
		{LINE, 1371272478, 0, O_LCNT_MAX},
		{LINE, 675466456, 0, O_LCNT_MAX},
		{LINE, 1808217256, 0, O_LCNT_MAX},
		{LINE, 2095021727, 0, O_LCNT_MAX},
		{LINE, 1769349045, 0, O_LCNT_MAX},
		{LINE, 904914315, 0, O_LCNT_MAX},
		{LINE, 373135028, 0, O_LCNT_MAX},
		{LINE, 717419739, 0, O_LCNT_MAX},
		{LINE, 1095462486, 0, O_LCNT_MAX * 2},
		{CUST, 881155353, 0, 9},
		{CUST, 1489529863, 0, 1},
		{CUST, 1521138112, 0, 3},
		{CUST, 298370230, 0, 1},
		{CUST, 1140279430, 0, 1},
		{CUST, 1335826707, 0, 2},
		{SUPP, 706178559, 0, 9},
		{SUPP, 110356601, 0, 1},
		{SUPP, 884434366, 0, 3},
		{SUPP, 962338209, 0, 1},
		{SUPP, 1341315363, 0, 2},
		{PART, 709314158, 0, 92},
		{ORDER, 591449447, 0, 1},
		{LINE, 431918286, 0, 1},
		{ORDER, 851767375, 0, 1},
		{NATION, 606179079, 0, 2},
		{REGION, 1500869201, 0, 2},
		{ORDER, 1434868289, 0, 1},
		{SUPP, 263032577, 0, 1},
		{SUPP, 753643799, 0, 1},
		{SUPP, 202794285, 0, 1},
		{SUPP, 715851524, 0, 1},
	}
}
