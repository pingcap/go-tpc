package dbgen

import (
	"testing"
)

func TestPickClerk(t *testing.T) {
	if pickClerk() != "Clerk#000000951" {
		t.Errorf("expect Clerk#000000951")
	}
	if pickClerk() != "Clerk#000000880" {
		t.Errorf("expect Clerk#000000880")
	}
	if pickClerk() != "Clerk#000000955" {
		t.Errorf("expect Clerk#000000955")
	}
}

func TestPickStr(t *testing.T) {
	var target string
	pickStr(&oPrioritySet, O_PRIO_SD, &target)
	if target != "5-LOW" {
		t.Errorf("expect 5-LOW")
	}
	pickStr(&oPrioritySet, O_PRIO_SD, &target)
	if target != "1-URGENT" {
		t.Errorf("expect 1-URGENT")
	}
	pickStr(&oPrioritySet, O_PRIO_SD, &target)
	if target != "5-LOW" {
		t.Errorf("expect 5-LOW")
	}
	pickStr(&oPrioritySet, O_PRIO_SD, &target)
	if target != "5-LOW" {
		t.Errorf("expect 5-LOW")
	}
	pickStr(&oPrioritySet, O_PRIO_SD, &target)
	if target != "5-LOW" {
		t.Errorf("expect 5-LOW")
	}
	pickStr(&oPrioritySet, O_PRIO_SD, &target)
	if target != "4-NOT SPECIFIED" {
		t.Errorf("expect 4-NOT SPECIFIED")
	}
	pickStr(&oPrioritySet, O_PRIO_SD, &target)
	if target != "2-HIGH" {
		t.Errorf("2-HIGH")
	}
}
