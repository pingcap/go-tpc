package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTiDBSemVersion(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected SemVersion
		ok       bool
	}{
		{
			name:     "normal case with addition",
			input:    "5.7.25-TiDB-v7.1.0-alpha",
			expected: SemVersion{Major: 7, Minor: 1, Patch: 0},
			ok:       true,
		},
		{
			name:     "version without addition",
			input:    "5.7.25-TiDB-v7.4.1",
			expected: SemVersion{Major: 7, Minor: 4, Patch: 1},
			ok:       true,
		},
		{
			name:     "multi-part addition",
			input:    "5.7.25-TiDB-v6.5.3-beta.2",
			expected: SemVersion{Major: 6, Minor: 5, Patch: 3},
			ok:       true,
		},
		{
			name:     "empty addition due to trailing hyphen",
			input:    "5.7.25-TiDB-v7.1.0-",
			expected: SemVersion{Major: 7, Minor: 1, Patch: 0},
			ok:       true,
		},
		{
			name:     "non-tidb database",
			input:    "MySQL 8.0.35",
			expected: SemVersion{},
			ok:       false,
		},
		{
			name:     "missing version prefix",
			input:    "TiDB-7.2.0",
			expected: SemVersion{},
			ok:       false,
		},
		{
			name:     "invalid patch version",
			input:    "5.7.25-TiDB-v7.1.x",
			expected: SemVersion{},
			ok:       false,
		},
		{
			name:     "insufficient version parts",
			input:    "5.7.25-TiDB-v7.1",
			expected: SemVersion{},
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
