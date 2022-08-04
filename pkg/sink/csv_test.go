package sink

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildColumns(t *testing.T) {
	v := buildColumns([]interface{}{nil, "a", 123, 456.123})
	require.Equal(t, []string{"NULL", "a", "123", "456.123000"}, v)

	v = buildColumns([]interface{}{sql.NullInt64{}})
	require.Equal(t, []string{"NULL"}, v)

	v = buildColumns([]interface{}{sql.NullInt64{Valid: true}})
	require.Equal(t, []string{"0"}, v)

	type dssHuge int

	v = buildColumns([]interface{}{dssHuge(5)})
	require.Equal(t, []string{"5"}, v)
}
