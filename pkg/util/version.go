package util

import (
	"strconv"
	"strings"

	"github.com/coreos/go-semver/semver"
)

// @version is the `SELECT VERSION()` output of TiDB
func NewTiDBSemVersion(version string) (*semver.Version, bool) {
	isTiDB := strings.Contains(strings.ToLower(version), "tidb")
	if !isTiDB {
		return nil, false
	}

	verItems := strings.Split(version, "-v")
	if len(verItems) < 2 {
		return nil, false
	}
	verParts := strings.Split(verItems[1], "-")
	verStr := verParts[0]
	var preReleaseStr string
	if len(verParts) > 1 {
		preReleaseStr = verParts[1]
	}

	parts := strings.Split(verStr, ".")
	if len(parts) < 3 {
		return nil, false
	}

	major, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, false
	}
	minor, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, false
	}

	patch, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, false
	}

	return &semver.Version{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: semver.PreRelease(preReleaseStr),
	}, true
}
