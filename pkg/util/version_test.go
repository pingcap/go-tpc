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

func TestSemVersionCompare(t *testing.T) {
	testCases := []struct {
		name     string
		version1 SemVersion
		version2 SemVersion
		expected int
	}{
		{
			name:     "major version greater",
			version1: SemVersion{Major: 8, Minor: 0, Patch: 0},
			version2: SemVersion{Major: 7, Minor: 5, Patch: 10},
			expected: 1,
		},
		{
			name:     "major version less",
			version1: SemVersion{Major: 6, Minor: 9, Patch: 9},
			version2: SemVersion{Major: 7, Minor: 0, Patch: 0},
			expected: -1,
		},
		{
			name:     "major same, minor greater",
			version1: SemVersion{Major: 7, Minor: 2, Patch: 0},
			version2: SemVersion{Major: 7, Minor: 1, Patch: 5},
			expected: 1,
		},
		{
			name:     "major same, minor less",
			version1: SemVersion{Major: 7, Minor: 1, Patch: 10},
			version2: SemVersion{Major: 7, Minor: 2, Patch: 0},
			expected: -1,
		},
		{
			name:     "major and minor same, patch greater",
			version1: SemVersion{Major: 7, Minor: 1, Patch: 5},
			version2: SemVersion{Major: 7, Minor: 1, Patch: 0},
			expected: 1,
		},
		{
			name:     "major and minor same, patch less",
			version1: SemVersion{Major: 7, Minor: 1, Patch: 0},
			version2: SemVersion{Major: 7, Minor: 1, Patch: 1},
			expected: -1,
		},
		{
			name:     "identical versions",
			version1: SemVersion{Major: 7, Minor: 1, Patch: 0},
			version2: SemVersion{Major: 7, Minor: 1, Patch: 0},
			expected: 0,
		},
		{
			name:     "extreme version differences",
			version1: SemVersion{Major: 10, Minor: 0, Patch: 0},
			version2: SemVersion{Major: 1, Minor: 99, Patch: 99},
			expected: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.version1.Compare(tc.version2)
			if result != tc.expected {
				t.Errorf("Expected %v.Compare(%v) = %v, got %v",
					tc.version1, tc.version2, tc.expected, result)
			}

			reverseResult := tc.version2.Compare(tc.version1)
			expectedReverse := -tc.expected
			if reverseResult != expectedReverse {
				t.Errorf("Expected %v.Compare(%v) = %v, got %v",
					tc.version2, tc.version1, expectedReverse, reverseResult)
			}
		})
	}
}
