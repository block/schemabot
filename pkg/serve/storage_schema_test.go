package serve

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

// A request may widen the deployment's destructive-statement policy and can
// never narrow it.
//
// The asymmetry is the point. A deployment that configured
// allow_destructive_schema_changes has already decided for every boot, so a
// convergence that ignored it would run less than the next boot runs — and
// "apply is what a boot does", the property that makes this usable as a
// pre-deploy step, would quietly stop holding there. In the other direction, a
// request opting in is the explicit operator consent required before surplus
// storage state is destroyed.
func TestStorageSchemaAdapter_EffectiveAllowDestructive(t *testing.T) {
	tests := []struct {
		name          string
		configAllows  bool
		requestAllows bool
		want          bool
	}{
		{"neither", false, false, false},
		{"request opts in", false, true, true},
		{"config already opted in", true, false, true},
		{"both", true, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &storageSchemaAdapter{configAllowsDestructive: tc.configAllows}
			assert.Equal(t, tc.want, adapter.effectiveAllowDestructive(tc.requestAllows))
		})
	}
}

// The adapter's storage is fixed at construction and nothing on the wire moves
// it: a request carries no target, so the DSN comes from the server's own
// config on every call.
func TestStorageSchemaAdapter_TargetResolvesTheServersOwnDSN(t *testing.T) {
	const dsn = "root@tcp(127.0.0.1:3306)/schemabot"
	adapter := &storageSchemaAdapter{
		resolveDSN: func() (string, error) { return dsn, nil },
		dialect:    schema.DialectPostgres,
		logger:     slog.New(slog.DiscardHandler),
	}

	resolved, opts, err := adapter.target(false)
	require.NoError(t, err)
	assert.Equal(t, dsn, resolved)
	assert.Len(t, opts, 3, "dialect, destructive policy and the PostgreSQL statement budget")
}

// The DSN is re-resolved per call rather than captured once, so a credential
// rotated since startup is picked up the same way the storage pool picks it up.
func TestStorageSchemaAdapter_TargetRereadsTheDSN(t *testing.T) {
	calls := 0
	adapter := &storageSchemaAdapter{
		resolveDSN: func() (string, error) {
			calls++
			return fmt.Sprintf("root:rotated%d@tcp(127.0.0.1:3306)/schemabot", calls), nil
		},
		dialect: schema.DialectMySQL,
		logger:  slog.New(slog.DiscardHandler),
	}

	first, _, err := adapter.target(false)
	require.NoError(t, err)
	second, _, err := adapter.target(false)
	require.NoError(t, err)
	assert.NotEqual(t, first, second, "each call resolves the DSN again")
	assert.Equal(t, 2, calls)
}

// A server with no storage DSN configured refuses to answer rather than
// reporting on a database it guessed at, and says which dialect it was trying
// to read.
func TestStorageSchemaAdapter_RefusesWithoutStorageDSN(t *testing.T) {
	adapter := &storageSchemaAdapter{
		resolveDSN: func() (string, error) { return "", nil },
		dialect:    schema.DialectMySQL,
		logger:     slog.New(slog.DiscardHandler),
	}

	_, _, err := adapter.target(false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage DSN not configured")
	assert.Contains(t, err.Error(), string(schema.DialectMySQL))

	_, err = adapter.StorageSchemaDiff(t.Context(), &ternv1.StorageSchemaDiffRequest{})
	require.Error(t, err, "a diff must not proceed without a database to read")

	_, err = adapter.StorageSchemaApply(t.Context(), &ternv1.StorageSchemaApplyRequest{})
	require.Error(t, err, "a convergence must not proceed without a database to converge")
}

// A diff with no schema on it is answered against this binary's own embedded
// schema, attributed to this binary's version — which is what a boot would
// converge to, and the answer an operator gets when they ask nothing else.
func TestStorageSchemaAdapter_DesiredSchemaDefaultsToThisBinary(t *testing.T) {
	adapter := &storageSchemaAdapter{version: "v1.2.3", logger: slog.New(slog.DiscardHandler)}

	desired, err := adapter.desiredSchema(&ternv1.StorageSchemaDiffRequest{})
	require.NoError(t, err)
	assert.Equal(t, "the schema embedded in v1.2.3", desired.Description)
	assert.Empty(t, desired.Files, "the answering binary's own files are read here, not sent to it")
}

// A schema on the request replaces the files the comparison reads, so an
// operator can ask what this storage needs in order to match a release this
// binary is not running. The attribution is the caller's, because the answer
// came from the caller's files.
func TestStorageSchemaAdapter_DesiredSchemaAcceptsASuppliedSchema(t *testing.T) {
	adapter := &storageSchemaAdapter{version: "v1.2.3", logger: slog.New(slog.DiscardHandler)}

	desired, err := adapter.desiredSchema(&ternv1.StorageSchemaDiffRequest{
		SchemaSource: "the schema files of release v1.4.0",
		SchemaFiles:  map[string]string{"applies.sql": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED PRIMARY KEY)"},
	})
	require.NoError(t, err)
	assert.Equal(t, "the schema files of release v1.4.0", desired.Description)
	assert.Len(t, desired.Files, 1)
}

// An unusable supplied schema is refused before anything reads a database. A
// file set that cannot be read as one .sql file per table would otherwise diff
// as a storage database full of surplus tables.
func TestStorageSchemaAdapter_DesiredSchemaRefusesAnUnusableSchema(t *testing.T) {
	adapter := &storageSchemaAdapter{version: "v1.2.3", logger: slog.New(slog.DiscardHandler)}

	_, err := adapter.desiredSchema(&ternv1.StorageSchemaDiffRequest{
		SchemaFiles: map[string]string{"applies.sql": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED PRIMARY KEY)"},
	})
	require.Error(t, err, "files with no source leave the report unable to attribute its answer")
	assert.Contains(t, err.Error(), "needs a description")
}

// A DSN the server cannot resolve — an unreadable credential file, say —
// surfaces as an error naming what was being resolved, not as a report about
// an empty database.
func TestStorageSchemaAdapter_SurfacesDSNResolutionFailure(t *testing.T) {
	adapter := &storageSchemaAdapter{
		resolveDSN: func() (string, error) { return "", assert.AnError },
		dialect:    schema.DialectPostgres,
		logger:     slog.New(slog.DiscardHandler),
	}

	_, _, err := adapter.target(false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve storage DSN")
	assert.ErrorIs(t, err, assert.AnError)
}
