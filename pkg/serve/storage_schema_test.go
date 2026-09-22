package serve

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// A request naming no budget runs under the boot's, and one naming a budget
// runs under exactly that (AV-11). The control plane always names the budget
// it resolved, so a zero here is a caller reaching the data plane directly
// that could not say how long it can wait — and the boot budget is the one
// such a caller has always waited out, where the operator default would hold
// the bootstrap lock for an hour after it had given up.
func TestConvergenceBudget_UnnamedRunsUnderTheBootBudget(t *testing.T) {
	t.Parallel()

	unnamed, err := convergenceBudget(&ternv1.StorageSchemaApplyRequest{})
	require.NoError(t, err)
	assert.Equal(t, api.EnsureSchemaTimeout, unnamed)

	named, err := convergenceBudget(&ternv1.StorageSchemaApplyRequest{TimeoutSeconds: 1200})
	require.NoError(t, err)
	assert.Equal(t, 20*time.Minute, named)
}

// An out-of-range budget on the wire is the caller's mistake, refused before
// the convergence is dispatched and before the storage database is dialed, so
// nothing runs under a ceiling nobody chose.
func TestStorageSchemaAdapter_ApplyRefusesAnOutOfRangeBudget(t *testing.T) {
	t.Parallel()

	const dsn = "root:pw@tcp(127.0.0.1:1)/schemabot"
	adapter := &storageSchemaAdapter{
		resolveDSN: func() (string, error) { return dsn, nil },
		bootTarget: bootTargetFor(t, schema.DialectMySQL, dsn),
		dialect:    schema.DialectMySQL,
		logger:     slog.New(slog.DiscardHandler),
	}

	_, err := adapter.StorageSchemaApply(t.Context(), &ternv1.StorageSchemaApplyRequest{TimeoutSeconds: -1})
	require.Error(t, err)
	assert.ErrorIs(t, err, tern.ErrInvalidStorageSchemaRequest)
	assert.Contains(t, err.Error(), "must be positive")
}

// A locally hosted server has no route to a destructive storage bootstrap, so
// the per-request opt-in that a deployed server honors is refused here instead
// of applied (AZ-6).
//
// It refuses rather than silently running the safe remainder: an operator who
// asked for the destructive statements needs to learn their opt-in did not
// apply, not read a convergence report that looks as though it did. The local
// runtime cannot say who issued the command, and a local host can be pointed at
// a real deployment's storage, so consent arriving this way is not the consent
// the widening is granted for.
func TestStorageSchemaAdapter_RefusesTheDestructiveOptInWhenLocallyHosted(t *testing.T) {
	adapter := &storageSchemaAdapter{
		localHosted: true,
		logger:      slog.New(slog.DiscardHandler),
	}

	err := adapter.checkDestructiveOptIn(true)
	require.Error(t, err)
	assert.ErrorIs(t, err, tern.ErrInvalidStorageSchemaRequest, "the request is what is wrong, not this server")
	assert.Contains(t, err.Error(), "locally hosted")
	assert.Contains(t, err.Error(), "re-run without the destructive opt-in",
		"the refusal names the way forward that does converge everything else")
}

// Local hosting refuses the destructive opt-in without refusing the request
// that never asked for it: an ordinary local convergence still runs.
func TestStorageSchemaAdapter_AllowsAnOrdinaryLocalConvergence(t *testing.T) {
	adapter := &storageSchemaAdapter{
		localHosted: true,
		logger:      slog.New(slog.DiscardHandler),
	}

	require.NoError(t, adapter.checkDestructiveOptIn(false))
}

// The server carries its local hosting into the adapter it builds, so the
// refusal is the whole server's and not a property of an adapter a test
// assembled by hand.
func TestNewStorageSchemaServiceCarriesLocalHosting(t *testing.T) {
	srv := &Server{
		cfg:         &api.ServerConfig{Storage: api.StorageConfig{DSN: "schemabot:pw@tcp(db.example:3306)/schemabot"}},
		dialect:     schema.DialectMySQL,
		storageDSN:  "schemabot:pw@tcp(db.example:3306)/schemabot",
		localHosted: true,
		logger:      slog.New(slog.DiscardHandler),
	}

	adapter, err := srv.newStorageSchemaService()
	require.NoError(t, err)

	require.ErrorIs(t, adapter.checkDestructiveOptIn(true), tern.ErrInvalidStorageSchemaRequest)
}

// The refusal is reached through the path every RPC takes, not only by calling
// the policy helper directly: target resolves the bootstrap options, so a
// destructive opt-in on a locally hosted server never reaches a convergence.
func TestStorageSchemaAdapter_TargetRefusesTheDestructiveOptInWhenLocallyHosted(t *testing.T) {
	const dsn = "root:secret@tcp(127.0.0.1:3306)/schemabot"
	adapter := &storageSchemaAdapter{
		resolveDSN:  func() (string, error) { return dsn, nil },
		bootTarget:  bootTargetFor(t, schema.DialectMySQL, dsn),
		dialect:     schema.DialectMySQL,
		localHosted: true,
		logger:      slog.New(slog.DiscardHandler),
	}

	_, _, err := adapter.target(true)
	require.Error(t, err)
	assert.ErrorIs(t, err, tern.ErrInvalidStorageSchemaRequest)

	_, opts, err := adapter.target(false)
	require.NoError(t, err, "the same server still converges what is safe")
	assert.Len(t, opts, 3)
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

// A caller that went away does not stop a convergence (AV-13). The adapter
// answers requests, and a request's context is cancelled by a dropped
// connection — which is not a decision anyone made about the storage every
// instance depends on. ApplyStorageSchema observes its context so an operator
// at a terminal can stop a run they are watching, so the authority to do that
// has to be withheld here rather than absent everywhere.
//
// The seam is visible without a database: a convergence handed an already
// cancelled context fails on the context before it reaches the storage, while
// one handed a context stripped of cancellation gets as far as trying to
// connect. Reaching the connection attempt is what says the cancellation did
// not travel.
func TestStorageSchemaAdapter_ADroppedConnectionDoesNotStopTheConvergence(t *testing.T) {
	const bootDSN = "root:pw@tcp(127.0.0.1:1)/schemabot"
	adapter := &storageSchemaAdapter{
		resolveDSN: func() (string, error) { return bootDSN, nil },
		bootTarget: bootTargetFor(t, schema.DialectMySQL, bootDSN),
		dialect:    schema.DialectMySQL,
		version:    "v1.2.3",
		logger:     slog.New(slog.DiscardHandler),
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := adapter.StorageSchemaApply(ctx, &ternv1.StorageSchemaApplyRequest{})

	// Nothing is listening on the DSN, so this always fails. Which way it
	// failed is the assertion.
	require.Error(t, err)
	assert.NotErrorIs(t, err, context.Canceled,
		"the request's cancellation must not reach the convergence; a lost connection cannot abandon a table copy")
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

	desired, err := adapter.desiredSchema(nil, "", "diff")
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

	desired, err := adapter.desiredSchema(
		map[string]string{"applies.sql": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED PRIMARY KEY)"},
		"the schema files of release v1.4.0", "diff")
	require.NoError(t, err)
	assert.Equal(t, "the schema files of release v1.4.0", desired.Description)
	assert.Len(t, desired.Files, 1)
}

// A schema named with no files to go with it is an invalid request, and it is
// refused in this adapter because the tern gRPC server reaches it without
// passing the HTTP API's validation. Defaulting to the embedded schema here
// would converge it successfully, so a caller that asked for one release's
// schema would have another's run against its storage and be told it worked.
func TestStorageSchemaAdapter_DesiredSchemaRefusesANameWithNoFiles(t *testing.T) {
	adapter := &storageSchemaAdapter{version: "v1.2.3", dialect: schema.DialectMySQL, logger: slog.New(slog.DiscardHandler)}

	_, err := adapter.desiredSchema(nil, "the schema files of release v1.4.0", "converge")
	require.ErrorIs(t, err, tern.ErrInvalidStorageSchemaRequest,
		"a source with no files must be an invalid request, not a silent convergence of the embedded schema")
	assert.Contains(t, err.Error(), "without schema_files")
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

	_, err := adapter.desiredSchema(
		map[string]string{"applies.sql": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED PRIMARY KEY)"},
		"", "diff")
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

			_, err := adapter.desiredSchema(tc.files, "the schema files in ./schema", "diff")
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

	_, err := adapter.desiredSchema(
		map[string]string{"applies.sql": "CREATE TABLE applies (id BIGINT)"},
		"the schema files in ./schema",
		"diff",
	)
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
