//go:build integration

package localscale

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/testutil"
)

// metadataOpTimeout bounds each database operation these tests run, so a
// blocked database fails the test instead of consuming the package timeout.
const metadataOpTimeout = 30 * time.Second

const stateWriteKeyspace = "testapp"

// stateWriteFixture is a Server backed by a fresh metadata database, plus a
// backend whose branch databases live on the same MySQL, so a test can drive
// the processor's state writes and its branch cleanup directly.
type stateWriteFixture struct {
	s       *Server
	backend *databaseBackend
}

func newStateWriteFixture(t *testing.T) *stateWriteFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), metadataOpTimeout)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", "localscale"),
		Started:          true,
	})
	testcontainers.CleanupContainer(t, container)
	require.NoError(t, err, "start metadata MySQL")
	dsn, err := testutil.MySQLDSN(ctx, container, "localscale", "parseTime=true")
	require.NoError(t, err)
	db, err := sql.Open(mysqlDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, testutil.PingMySQL(ctx, db))

	s := &Server{metadataDB: db, logger: quietLogger(), revertWindowDuration: time.Minute}
	require.NoError(t, s.initMetadata(ctx), "create the metadata schema")
	backend := &databaseBackend{
		mysqlDSNBase: dsn,
		vtgateDBs:    map[string]*sql.DB{stateWriteKeyspace: nil},
	}
	return &stateWriteFixture{s: s, backend: backend}
}

func opContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), metadataOpTimeout)
	t.Cleanup(cancel)
	return ctx
}

func (f *stateWriteFixture) insertDeployRequest(t *testing.T, ref deployRequest, branch, deployState string) {
	t.Helper()
	_, err := f.s.metadataDB.ExecContext(opContext(t),
		`INSERT INTO localscale_deploy_requests (number, org, database_name, branch, deployment_state, deployed)
		 VALUES (?, ?, ?, ?, ?, TRUE)`,
		ref.number, ref.org, ref.database, branch, deployState)
	require.NoError(t, err)
}

func (f *stateWriteFixture) deployState(t *testing.T, ref deployRequest) string {
	t.Helper()
	var deployState string
	require.NoError(t, f.s.metadataDB.QueryRowContext(opContext(t),
		`SELECT deployment_state FROM localscale_deploy_requests WHERE org = ? AND database_name = ? AND number = ?`,
		ref.org, ref.database, ref.number).Scan(&deployState))
	return deployState
}

func (f *stateWriteFixture) createBranchDatabase(t *testing.T, branch string) {
	t.Helper()
	_, err := f.s.metadataDB.ExecContext(opContext(t),
		fmt.Sprintf("CREATE DATABASE %s", quoteIdentifier(branchDBName(branch, stateWriteKeyspace))))
	require.NoError(t, err)
}

func (f *stateWriteFixture) branchDatabaseExists(t *testing.T, branch string) bool {
	t.Helper()
	var count int
	require.NoError(t, f.s.metadataDB.QueryRowContext(opContext(t),
		`SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?`,
		branchDBName(branch, stateWriteKeyspace)).Scan(&count))
	return count == 1
}

func TestProcessorStateWrites(t *testing.T) {
	f := newStateWriteFixture(t)

	// The processor reads a deploy request in in_progress and derives
	// pending_cutover from its schema changes, but the operator's cancel
	// recorded in_progress_cancel before the processor wrote. The processor's
	// write leaves the cancel in place, so the request goes on to
	// complete_cancel instead of returning to pending_cutover for good.
	t.Run("keeps a cancel recorded after the read", func(t *testing.T) {
		ref := deployRequest{org: "org", database: "db", number: 1}
		f.insertDeployRequest(t, ref, "cancel-race", dr.InProgress)
		require.NoError(t, f.s.updateDeployState(opContext(t), ref, dr.InProgressCancel), "the cancel handler records the cancel")

		applied, err := f.s.advanceDeployState(opContext(t), ref, dr.InProgress, dr.PendingCutover)
		require.NoError(t, err)
		assert.False(t, applied, "the derived state is not written over the cancel")
		assert.Equal(t, dr.InProgressCancel, f.deployState(t, ref))
	})

	// With nothing recorded since the processor read the request, its derived
	// state is written, and entering the revert window starts the window's
	// clock.
	t.Run("writes the derived state", func(t *testing.T) {
		ref := deployRequest{org: "org", database: "db", number: 2}
		f.insertDeployRequest(t, ref, "derived", dr.InProgressCutover)

		applied, err := f.s.advanceDeployState(opContext(t), ref, dr.InProgressCutover, dr.CompletePendingRevert)
		require.NoError(t, err)
		assert.True(t, applied)
		assert.Equal(t, dr.CompletePendingRevert, f.deployState(t, ref))
		var expires sql.NullTime
		require.NoError(t, f.s.metadataDB.QueryRowContext(opContext(t),
			`SELECT revert_expires_at FROM localscale_deploy_requests WHERE number = ?`, ref.number).Scan(&expires))
		assert.True(t, expires.Valid, "the revert window's expiry is recorded")
	})

	// The processor reads a deploy request in complete_pending_revert after
	// its revert window expired, but the operator's revert recorded
	// in_progress_revert before the processor closed the window. The revert
	// keeps its state and the branch databases it reverts from.
	t.Run("expired revert window keeps a revert recorded after the read", func(t *testing.T) {
		ref := deployRequest{org: "org", database: "db", number: 3}
		const branch = "revert-race"
		f.insertDeployRequest(t, ref, branch, dr.CompletePendingRevert)
		f.createBranchDatabase(t, branch)
		require.NoError(t, f.s.updateDeployState(opContext(t), ref, dr.InProgressRevert), "the revert handler records the revert")

		require.NoError(t, f.s.completeExpiredRevertWindow(opContext(t), f.backend, ref, dr.CompletePendingRevert, branch))
		assert.Equal(t, dr.InProgressRevert, f.deployState(t, ref))
		assert.True(t, f.branchDatabaseExists(t, branch), "the revert's branch database is kept")
	})

	// With nothing recorded since the processor read the request, an expired
	// revert window completes the request and drops its branch databases.
	t.Run("expired revert window completes and drops the branch databases", func(t *testing.T) {
		ref := deployRequest{org: "org", database: "db", number: 4}
		const branch = "revert-expired"
		f.insertDeployRequest(t, ref, branch, dr.CompletePendingRevert)
		f.createBranchDatabase(t, branch)

		require.NoError(t, f.s.completeExpiredRevertWindow(opContext(t), f.backend, ref, dr.CompletePendingRevert, branch))
		assert.Equal(t, dr.Complete, f.deployState(t, ref))
		assert.False(t, f.branchDatabaseExists(t, branch), "the branch database is dropped")
	})
}
