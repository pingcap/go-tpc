package sink

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildSQLRow(t *testing.T) {
	v := buildSQLRow([]interface{}{nil, "a", 123, 456.123})
	require.Equal(t, `(NULL,'a',123,456.123000)`, v)

	v = buildSQLRow([]interface{}{sql.NullInt64{}})
	require.Equal(t, `(NULL)`, v)

	v = buildSQLRow([]interface{}{sql.NullInt64{Valid: true}})
	require.Equal(t, `(0)`, v)

	type dssHuge int

	v = buildSQLRow([]interface{}{dssHuge(5)})
	require.Equal(t, `(5)`, v)

	v = buildSQLRow([]interface{}{})
	require.Equal(t, `()`, v)
}
