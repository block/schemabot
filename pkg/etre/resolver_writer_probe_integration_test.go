//go:build integration

package etre

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/square/etre"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/testutil"
)

// replicationDeadline bounds waiting for a replica to connect to its source.
const replicationDeadline = 30 * time.Second

// pairServer is one MySQL server of a replicated pair, reachable from the test
// at addr and from the other server at alias.
type pairServer struct {
	alias    string
	addr     string
	password string
	db       *sql.DB
}

func startPairServer(t *testing.T, nw *testcontainers.DockerNetwork, alias string, serverID int) *pairServer {
	t.Helper()
	ctx := t.Context()

	req := testutil.MySQLContainerRequest("mysql:8.0", "app")
	req.Cmd = []string{fmt.Sprintf("--server-id=%d", serverID)}
	req.Networks = []string{nw.Name}
	req.NetworkAliases = map[string][]string{nw.Name: {alias}}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate %s container: %v", alias, err)
		}
	})
	require.NoError(t, err, "start %s container", alias)

	dsn, err := testutil.MySQLDSN(ctx, container, "")
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)

	db, err := mysqlconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, testutil.PingMySQL(ctx, db))

	return &pairServer{alias: alias, addr: cfg.Addr, password: cfg.Passwd, db: db}
}

func (s *pairServer) exec(t *testing.T, query string) {
	t.Helper()
	_, err := s.db.ExecContext(t.Context(), query)
	require.NoError(t, err, "%s: %s", s.alias, query)
}

func (s *pairServer) serverUUID(t *testing.T) string {
	t.Helper()
	var uuid string
	require.NoError(t, s.db.QueryRowContext(t.Context(), "SELECT @@global.server_uuid").Scan(&uuid))
	return uuid
}

// dsn connects to s from the test, the way the resolver's probe will.
func (s *pairServer) dsn() string {
	cfg := mysql.NewConfig()
	cfg.User = "root"
	cfg.Passwd = s.password
	cfg.Net = "tcp"
	cfg.Addr = s.addr
	return cfg.FormatDSN()
}

// replicateFrom makes s a read-only replica of source and waits until the
// writer probe sees s replicating from source's identity.
func (s *pairServer) replicateFrom(t *testing.T, source *pairServer) {
	t.Helper()
	s.exec(t, fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=3306, SOURCE_USER='root', SOURCE_PASSWORD='%s', GET_SOURCE_PUBLIC_KEY=1", source.alias, source.password))
	s.exec(t, "START REPLICA")
	s.exec(t, "SET GLOBAL read_only = 1")

	want := source.serverUUID(t)
	ctx, cancel := context.WithTimeout(t.Context(), replicationDeadline)
	defer cancel()
	for {
		status, err := pairProbe.ProbeWriter(ctx, s.dsn())
		require.NoError(t, err, "probe %s", s.alias)
		if len(status.SourceIDs) == 1 && status.SourceIDs[0] == want {
			return
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "replica did not connect to its source", "%s waiting for %s (%s), saw %v", s.alias, source.alias, want, status.SourceIDs)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// promote stops s replicating and makes it writable.
func (s *pairServer) promote(t *testing.T) {
	t.Helper()
	s.exec(t, "STOP REPLICA")
	s.exec(t, "RESET REPLICA ALL")
	s.exec(t, "SET GLOBAL read_only = 0")
}

var pairProbe = inventory.MySQLWriterProbe{ConnectTimeout: 5 * time.Second}

func pairResolver(t *testing.T, entities []etre.Entity, password string) *EtreResolver {
	t.Helper()
	return newEtreResolverForTest(t, nil, entities, EtreResolverConfig{
		TargetLabel: "dsid",
		EnvLabel:    "env",
		HostField:   "writer_endpoint",
		Credentials: inventory.SecretRefCredentialResolver{Username: "root", PasswordRef: password},
		Assembler:   inventory.MySQLConnectionAssembler{},
		WriterProbe: pairProbe,
	})
}

func resolvePair(t *testing.T, r *EtreResolver) (*inventory.Target, error) {
	t.Helper()
	return r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid", DatabaseType: "mysql", Environment: "staging"})
}

// An inventory records both sides of a replicated pair under one target. The
// target resolves to whichever side accepts writes: the original before a
// switchover, the standby after it, once the original replicates back from it.
// When the read-only side stops replicating, nothing proves the two are copies
// of one database, so the target does not resolve at all.
func TestEtreResolverWriterProbeAgainstReplicatedPair(t *testing.T) {
	nw, err := tcnetwork.New(t.Context())
	require.NoError(t, err, "create docker network")
	t.Cleanup(func() {
		if err := nw.Remove(t.Context()); err != nil {
			t.Logf("remove docker network: %v", err)
		}
	})

	blue := startPairServer(t, nw, "blue", 1)
	green := startPairServer(t, nw, "green", 2)
	green.replicateFrom(t, blue)

	entities := []etre.Entity{
		{"_id": "id-green", "writer_endpoint": green.addr},
		{"_id": "id-blue", "writer_endpoint": blue.addr},
	}
	r := pairResolver(t, entities, blue.password)

	// The probe reports each side as it is.
	blueStatus, err := pairProbe.ProbeWriter(t.Context(), blue.dsn())
	require.NoError(t, err)
	assert.Equal(t, inventory.WriterStatus{Writable: true, ServerID: blue.serverUUID(t)}, blueStatus)
	greenStatus, err := pairProbe.ProbeWriter(t.Context(), green.dsn())
	require.NoError(t, err)
	assert.Equal(t, inventory.WriterStatus{ReadOnlyReason: "read_only=1", ServerID: green.serverUUID(t), SourceIDs: []string{blue.serverUUID(t)}}, greenStatus)

	t.Run("before a switchover the original side is the writer", func(t *testing.T) {
		target, err := resolvePair(t, r)
		require.NoError(t, err)
		assert.Equal(t, blue.addr, resolvedAddr(t, target))
	})

	t.Run("after a switchover the standby is the writer", func(t *testing.T) {
		green.promote(t)
		blue.replicateFrom(t, green)

		target, err := resolvePair(t, r)
		require.NoError(t, err)
		assert.Equal(t, green.addr, resolvedAddr(t, target))
	})

	t.Run("a read-only side that does not replicate is refused", func(t *testing.T) {
		blue.exec(t, "STOP REPLICA")
		blue.exec(t, "RESET REPLICA ALL")

		target, err := resolvePair(t, r)
		require.Error(t, err)
		assert.Nil(t, target)
		assert.Contains(t, err.Error(), "read-only candidate id-blue does not replicate from writable candidate id-green")
	})

	t.Run("two writable sides are refused", func(t *testing.T) {
		blue.exec(t, "SET GLOBAL read_only = 0")

		target, err := resolvePair(t, r)
		require.Error(t, err)
		assert.Nil(t, target)
		assert.Contains(t, err.Error(), "2 candidates accept writes")
	})
}
