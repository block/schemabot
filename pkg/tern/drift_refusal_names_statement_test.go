package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// A statement the drift comparison cannot classify stays refused, and the
// refusal names the statement, so the recovery that pauses on it says which
// planned step it could not verify.
func TestCanonicalDDLForDrift_RefusalNamesTheStatement(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	_, err = canonicalDDLForDrift(parser, `COMMENT ON TABLE app.orders IS 'orders'`)
	require.Error(t, err)
	assert.ErrorContains(t, err, `statement "COMMENT ON TABLE app.orders IS 'orders'" classified outside the shared DDL vocabulary`)
}
