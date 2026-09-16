package serve

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
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
	const dsn = "postgres://schemabot@db.example:5432/schemabot"
	adapter := &storageSchemaAdapter{
		resolveDSN: func() (string, error) { return dsn, nil },
		bootTarget: bootTargetFor(t, schema.DialectPostgres, dsn),
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
	const bootDSN = "root:original@tcp(127.0.0.1:3306)/schemabot"
	calls := 0
	adapter := &storageSchemaAdapter{
		resolveDSN: func() (string, error) {
			calls++
			return fmt.Sprintf("root:rotated%d@tcp(127.0.0.1:3306)/schemabot", calls), nil
		},
		bootTarget: bootTargetFor(t, schema.DialectMySQL, bootDSN),
		dialect:    schema.DialectMySQL,
		logger:     slog.New(slog.DiscardHandler),
	}

	first, _, err := adapter.target(false)
	require.NoError(t, err)
	second, _, err := adapter.target(false)
	require.NoError(t, err)
	assert.NotEqual(t, first, second, "each call resolves the DSN again")
	assert.Equal(t, 2, calls)
}

// A credential may rotate under a running instance; the database may not. A DSN
// that has come to name a different server or a different database is refused,
// because answering it would report another instance's storage as this one's
// and converge this binary's embedded schema onto a database it never booted
// against.
func TestStorageSchemaAdapter_TargetRefusesAMovedDatabase(t *testing.T) {
	tests := []struct {
		name    string
		dialect schema.Dialect
		boot    string
		moved   string
	}{
		{
			name:    "mysql database renamed",
			dialect: schema.DialectMySQL,
			boot:    "root:pw@tcp(127.0.0.1:3306)/schemabot",
			moved:   "root:pw@tcp(127.0.0.1:3306)/schemabot_staging",
		},
		{
			name:    "mysql server moved",
			dialect: schema.DialectMySQL,
			boot:    "root:pw@tcp(127.0.0.1:3306)/schemabot",
			moved:   "root:pw@tcp(db.example:3306)/schemabot",
		},
		{
			name:    "postgres database renamed",
			dialect: schema.DialectPostgres,
			boot:    "postgres://schemabot@db.example:5432/schemabot",
			moved:   "postgres://schemabot@db.example:5432/schemabot_staging",
		},
		{
			name:    "postgres server moved",
			dialect: schema.DialectPostgres,
			boot:    "postgres://schemabot@db.example:5432/schemabot",
			moved:   "postgres://schemabot@other.example:5432/schemabot",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &storageSchemaAdapter{
				resolveDSN: func() (string, error) { return tc.moved, nil },
				bootTarget: bootTargetFor(t, tc.dialect, tc.boot),
				dialect:    tc.dialect,
				logger:     slog.New(slog.DiscardHandler),
			}

			_, _, err := adapter.target(false)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "this instance booted against")
			assert.Contains(t, err.Error(), "restart it to adopt the new storage")

			_, err = adapter.StorageSchemaPlan(t.Context(), &ternv1.StorageSchemaPlanRequest{})
			require.Error(t, err, "a plan must not read a database this instance did not boot on")

			_, err = adapter.StorageSchemaApply(t.Context(), &ternv1.StorageSchemaApplyRequest{})
			require.Error(t, err, "a convergence must not write to a database this instance did not boot on")
		})
	}
}

// Building a server registers one storage-schema adapter on both surfaces that
// answer for its storage: the HTTP routes an operator reaches directly, and the
// field the gRPC endpoint registers from. Either surface left unregistered
// refuses every request as unsupported, which an operator reads as a data plane
// too old to serve them.
//
// The adapter is bound to the DSN the storage pool was opened with, so the
// configured storage naming a different database than this server booted
// against is refused by both surfaces rather than answered from either.
func TestRegisterStorageSchemaBindsBothSurfacesToTheBootStorage(t *testing.T) {
	const bootDSN = "root:pw@tcp(127.0.0.1:3306)/schemabot"
	logger := slog.New(slog.DiscardHandler)
	cfg := &api.ServerConfig{Storage: api.StorageConfig{DSN: "root:pw@tcp(127.0.0.1:3306)/schemabot_staging"}}
	svc := api.New(mysqlstore.New(nil), cfg, nil, logger)
	srv := &Server{cfg: cfg, svc: svc, dialect: schema.DialectMySQL, storageDSN: bootDSN, version: "v1.2.3", logger: logger}

	require.NoError(t, srv.registerStorageSchema(svc))

	require.NotNil(t, srv.storageSchema, "the gRPC endpoint registers the service from this field")
	_, err := srv.storageSchema.StorageSchemaPlan(t.Context(), &ternv1.StorageSchemaPlanRequest{})
	require.Error(t, err, "the configured storage names a database this server did not boot on")
	assert.Contains(t, err.Error(), "booted against", "the adapter is bound to the DSN the storage pool was opened with")

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/storage/schema/plan", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.NotContains(t, rec.Body.String(), "does not expose its own storage schema",
		"the HTTP routes answer from the registered adapter, not as an unregistered surface")
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
}

func bootTargetFor(t *testing.T, dialect schema.Dialect, dsn string) storageTarget {
	t.Helper()
	target, err := storageTargetFor(dialect, dsn)
	require.NoError(t, err)
	return target
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

	_, err = adapter.StorageSchemaPlan(t.Context(), &ternv1.StorageSchemaPlanRequest{})
	require.Error(t, err, "a diff must not proceed without a database to read")

	_, err = adapter.StorageSchemaApply(t.Context(), &ternv1.StorageSchemaApplyRequest{})
	require.Error(t, err, "a convergence must not proceed without a database to converge")
}

// A diff with no schema on it is answered against this binary's own embedded
// schema, attributed to this binary's version — which is what a boot would
// converge to, and the answer an operator gets when they ask nothing else.
func TestStorageSchemaAdapter_DesiredSchemaDefaultsToThisBinary(t *testing.T) {
	adapter := &storageSchemaAdapter{version: "v1.2.3", logger: slog.New(slog.DiscardHandler)}

	desired, err := adapter.desiredSchema(&ternv1.StorageSchemaPlanRequest{})
	require.NoError(t, err)
	assert.Equal(t, "the schema embedded in v1.2.3", desired.Description)
	assert.Empty(t, desired.Files, "the answering binary's own files are read here, not sent to it")
}

// A schema on the request replaces the files the comparison reads, so an
// operator can ask what this storage needs in order to match a release this
// binary is not running. The attribution is the caller's, because the answer
// came from the caller's files.
func TestStorageSchemaAdapter_DesiredSchemaAcceptsASuppliedSchema(t *testing.T) {
	adapter := &storageSchemaAdapter{version: "v1.2.3", dialect: schema.DialectMySQL, logger: slog.New(slog.DiscardHandler)}

	desired, err := adapter.desiredSchema(&ternv1.StorageSchemaPlanRequest{
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
//
// The refusal is marked as the caller's, because the caller wrote the files: it
// reaches them as an InvalidArgument carrying the reason, rather than as an
// Internal pointing at logs on a data plane they may not be able to read.
func TestStorageSchemaAdapter_DesiredSchemaRefusesAnUnusableSchema(t *testing.T) {
	adapter := &storageSchemaAdapter{version: "v1.2.3", logger: slog.New(slog.DiscardHandler)}

	_, err := adapter.desiredSchema(&ternv1.StorageSchemaPlanRequest{
		SchemaFiles: map[string]string{"applies.sql": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED PRIMARY KEY)"},
	})
	require.Error(t, err, "files with no source leave the report unable to attribute its answer")
	assert.Contains(t, err.Error(), "needs a description")
	assert.ErrorIs(t, err, tern.ErrInvalidStorageSchemaRequest)
}

// Supplied content is held to the dialect's real parser at the door, and a file
// that is not valid SQL for it is named. The diff would reach a parser several
// layers down and fail there too, but that failure is indistinguishable from
// the storage database being unreachable: it reaches the caller as this
// instance's fault, and sends an operator who mistyped a file looking at the
// server.
func TestStorageSchemaAdapter_DesiredSchemaRefusesUnparseableContent(t *testing.T) {
	tests := []struct {
		name    string
		dialect schema.Dialect
		files   map[string]string
	}{
		{
			name:    "mysql",
			dialect: schema.DialectMySQL,
			files:   map[string]string{"applies.sql": "CREATE TABLE `applies` ("},
		},
		{
			name:    "postgres",
			dialect: schema.DialectPostgres,
			files:   map[string]string{"applies.sql": "CREATE TABLE applies ("},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &storageSchemaAdapter{version: "v1.2.3", dialect: tc.dialect, logger: slog.New(slog.DiscardHandler)}

			_, err := adapter.desiredSchema(&ternv1.StorageSchemaPlanRequest{
				SchemaSource: "the schema files in ./schema",
				SchemaFiles:  tc.files,
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, tern.ErrInvalidStorageSchemaRequest)
			assert.Contains(t, err.Error(), `schema file "applies.sql"`, "the refusal has to name the file the caller must fix")
			assert.Contains(t, err.Error(), "the schema files in ./schema")
		})
	}
}

// A dialect with no parser of ours is this instance's configuration, not the
// caller's request, so it is not blamed on the caller: an InvalidArgument would
// tell an operator to fix files that are fine.
func TestStorageSchemaAdapter_DesiredSchemaBlamesAnUnparseableDialectOnTheServer(t *testing.T) {
	adapter := &storageSchemaAdapter{version: "v1.2.3", dialect: schema.Dialect("cockroach"), logger: slog.New(slog.DiscardHandler)}

	_, err := adapter.desiredSchema(&ternv1.StorageSchemaPlanRequest{
		SchemaSource: "the schema files in ./schema",
		SchemaFiles:  map[string]string{"applies.sql": "CREATE TABLE applies (id BIGINT)"},
	})
	require.Error(t, err)
	assert.NotErrorIs(t, err, tern.ErrInvalidStorageSchemaRequest)
	assert.Contains(t, err.Error(), "no statement parser registered")
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
