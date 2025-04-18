package util

import (
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/assert"
)

func TestNewTiDBSemVersion(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected *semver.Version
		ok       bool
	}{
		{
			name:     "normal case with addition",
			input:    "5.7.25-TiDB-v7.1.0-alpha",
			expected: &semver.Version{Major: 7, Minor: 1, Patch: 0, PreRelease: "alpha"},
			ok:       true,
		},
		{
			name:     "version without addition",
			input:    "5.7.25-TiDB-v7.4.1",
			expected: &semver.Version{Major: 7, Minor: 4, Patch: 1, PreRelease: ""},
			ok:       true,
		},
		{
			name:     "multi-part addition",
			input:    "5.7.25-TiDB-v6.5.3-beta.2",
			expected: &semver.Version{Major: 6, Minor: 5, Patch: 3, PreRelease: "beta.2"},
			ok:       true,
		},
		{
			name:     "empty addition due to trailing hyphen",
			input:    "5.7.25-TiDB-v7.1.0-",
			expected: &semver.Version{Major: 7, Minor: 1, Patch: 0, PreRelease: ""},
			ok:       true,
		},
		{
			input:    "8.0.11-TiDB-v9.0.0-beta.1.pre-547-g4d34cac",
			expected: &semver.Version{Major: 9, Minor: 0, Patch: 0, PreRelease: "beta.1.pre"},
			ok:       true,
		},
		{
			name:     "non-tidb database",
			input:    "MySQL 8.0.35",
			expected: nil,
			ok:       false,
		},
		{
			name:     "missing version prefix",
			input:    "TiDB-7.2.0",
			expected: nil,
			ok:       false,
		},
		{
			name:     "invalid patch version",
			input:    "5.7.25-TiDB-v7.1.x",
			expected: nil,
			ok:       false,
		},
		{
			name:     "insufficient version parts",
			input:    "5.7.25-TiDB-v7.1",
			expected: nil,
			ok:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, ok := NewTiDBSemVersion(tc.input)
			assert.Equal(t, tc.ok, ok, "ok mismatch")
			if tc.ok {
				assert.Equal(t, tc.expected, actual, "version mismatch")
			}
		})
	}
}
