package testutil

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PgBouncerPoolMode selects how PgBouncer binds a client connection to a
// PostgreSQL backend.
type PgBouncerPoolMode string

const (
	// PgBouncerTransactionPooling rebinds a backend per transaction, so a
	// client connection has no stable server session. This is the mode hosted
	// PostgreSQL platforms put on their default connection string.
	PgBouncerTransactionPooling PgBouncerPoolMode = "transaction"
	// PgBouncerSessionPooling holds one backend for a client connection's
	// lifetime, which preserves session-scoped state.
	PgBouncerSessionPooling PgBouncerPoolMode = "session"
)

const (
	pgBouncerImage         = "edoburu/pgbouncer:v1.25.2-p0"
	pgBouncerPort          = "6432"
	pgBouncerUpstreamAlias = "upstream-postgres"
	pgBouncerUser          = "schemabot"
	pgBouncerPassword      = "test"
)

// StartPostgresBehindPgBouncer starts a PostgreSQL container with a PgBouncer
// container in front of it on a shared Docker network, and returns the DSN
// through PgBouncer alongside the DSN straight to PostgreSQL. Both reach the
// same database, so a test can compare behavior across the pooler against the
// same server.
//
// The pool is deliberately larger than one backend: a single-backend pool
// would serialize the two connections a caller needs rather than let them
// share or rotate backends, which is the behavior under test.
func StartPostgresBehindPgBouncer(t *testing.T, database string, mode PgBouncerPoolMode) (pooledDSN, directDSN string) {
	t.Helper()
	ctx := t.Context()

	nw, err := network.New(ctx)
	require.NoError(t, err, "failed to create docker network")
	t.Cleanup(func() {
		// The test's context is already cancelled by the time cleanup runs, so
		// the removal needs one that outlives it or the network is left behind.
		if err := nw.Remove(context.WithoutCancel(ctx)); err != nil {
			t.Logf("failed to remove docker network: %v", err)
		}
	})

	pg, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase(database),
		postgres.WithUsername(pgBouncerUser),
		postgres.WithPassword(pgBouncerPassword),
		postgres.BasicWaitStrategies(),
		network.WithNetwork([]string{pgBouncerUpstreamAlias}, nw),
	)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(pg); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	})
	require.NoError(t, err, "failed to start postgres")

	directDSN, err = ContainerConnectionString(ctx, pg, "sslmode=disable")
	require.NoError(t, err, "failed to get postgres connection string")

	bouncer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: pgBouncerImage,
			Env: map[string]string{
				"DB_HOST":     pgBouncerUpstreamAlias,
				"DB_PORT":     "5432",
				"DB_USER":     pgBouncerUser,
				"DB_PASSWORD": pgBouncerPassword,
				"DB_NAME":     database,
				"POOL_MODE":   string(mode),
				"LISTEN_PORT": pgBouncerPort,
				"AUTH_TYPE":   "scram-sha-256",
				// pgx prepares statements by default, which a transaction-mode
				// pooler can only carry across backends when it tracks them
				// itself. Without this the pooler fails the driver's queries
				// outright instead of serving them from the wrong session,
				// which is not the configuration operators actually run.
				"MAX_PREPARED_STATEMENTS": "100",
				"MAX_CLIENT_CONN":         "50",
				"DEFAULT_POOL_SIZE":       "5",
			},
			ExposedPorts: []string{pgBouncerPort + "/tcp"},
			Networks:     []string{nw.Name},
			WaitingFor:   wait.ForListeningPort(pgBouncerPort + "/tcp"),
		},
		Started: true,
	})
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(bouncer); err != nil {
			t.Logf("failed to terminate pgbouncer container: %v", err)
		}
	})
	require.NoError(t, err, "failed to start pgbouncer")

	host, err := ContainerHost(ctx, bouncer)
	require.NoError(t, err, "failed to get pgbouncer host")
	port, err := ContainerPort(ctx, bouncer, pgBouncerPort+"/tcp")
	require.NoError(t, err, "failed to get pgbouncer port")

	pooled := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(pgBouncerUser, pgBouncerPassword),
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     "/" + database,
		RawQuery: "sslmode=disable",
	}
	return pooled.String(), directDSN
}
