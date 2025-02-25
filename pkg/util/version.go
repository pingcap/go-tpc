package util

import (
	"strconv"
	"strings"
)

type SemVersion struct {
	Major int
	Minor int
	Patch int
}

// @version is the `SELECT VERSION()` output of TiDB
func NewTiDBSemVersion(version string) (SemVersion, bool) {
	isTiDB := strings.Contains(strings.ToLower(version), "tidb")
	if !isTiDB {
		return SemVersion{}, false
	}

	verItems := strings.Split(version, "-v")
	if len(verItems) < 2 {
		return SemVersion{}, false
	}
	verStr := strings.Split(verItems[1], "-")[0]

	parts := strings.Split(verStr, ".")
	if len(parts) < 3 {
		return SemVersion{}, false
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return SemVersion{}, false
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return SemVersion{}, false
	}

	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return SemVersion{}, false
	}

	return SemVersion{
		Major: major,
		Minor: minor,
		Patch: patch,
	}, true

}
