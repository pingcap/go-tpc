package dbgen

import (
	"testing"
)

func TestTextPool(t *testing.T) {
	expect := "furiously special foxes haggle furiously blithely ironic deposits. slyly final theodolites boost slyly even asymptotes. carefully final foxes wake furiously around the furiously bold deposits. foxes around the pending, special theodolites believe about the special accounts. furiously special packages wake about the slyly ironic accounts. ironic accounts sleep. blithely pending ideas sleep blithely. carefully bold attainments unwind along the even foxes. blithely regular accounts haggle blithely above the quick pinto beans. requests cajole slyly across the slyly pending ideas. evenly even deposits hinder bold deposits. quick, careful packages could have to use slyly ideas. instructions about the foxes detect across the quickly regular requests. furiously final orbits across the fluffily special dependencies boost slyly about the express theodolites. evenly bold excuses need to wake. slyly even pinto beans use blithely according to the special packages. quickly regular dependencies sleep"

	if string(szTextPool[:1000]) != expect {
		t.Errorf("expect %s", expect)
	}
}
