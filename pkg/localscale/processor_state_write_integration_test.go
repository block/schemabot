//go:build integration

package localscale

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/block/schemabot/pkg/testutil"
)

// newMetadataOnlyServer is a Server backed by a fresh metadata database and
// nothing else, so a test can drive the processor's state writes directly.
func newMetadataOnlyServer(t *testing.T) *Server {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	container, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithDatabase("localscale"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
		testutil.MySQLTmpfsDatadir(),
	)
	testcontainers.CleanupContainer(t, container)
	require.NoError(t, err, "start metadata MySQL")
	dsn, err := container.ConnectionString(ctx, "parseTime=true")
	require.NoError(t, err)
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(ctx))

	s := &Server{metadataDB: db, logger: quietLogger(), revertWindowDuration: time.Minute}
	require.NoError(t, s.initMetadata(ctx), "create the metadata schema")
	return s
}

func insertDeployRequest(t *testing.T, s *Server, ref deployRequest, deployState string) {
	t.Helper()
	_, err := s.metadataDB.ExecContext(t.Context(),
		`INSERT INTO localscale_deploy_requests (number, org, database_name, branch, deployment_state, deployed)
		 VALUES (?, ?, ?, 'feature', ?, TRUE)`,
		ref.number, ref.org, ref.database, deployState)
	require.NoError(t, err)
}

func storedDeployState(t *testing.T, s *Server, ref deployRequest) string {
	t.Helper()
	var deployState string
	require.NoError(t, s.metadataDB.QueryRowContext(t.Context(),
		`SELECT deployment_state FROM localscale_deploy_requests WHERE org = ? AND database_name = ? AND number = ?`,
		ref.org, ref.database, ref.number).Scan(&deployState))
	return deployState
}

// The processor reads a deploy request in in_progress and derives
// pending_cutover from its schema changes, but the operator's cancel recorded
// in_progress_cancel before the processor wrote. The processor's write leaves
// the cancel in place, so the request goes on to complete_cancel instead of
// returning to pending_cutover for good.
func TestAdvanceDeployStateKeepsAStateRecordedAfterTheProcessorRead(t *testing.T) {
	s := newMetadataOnlyServer(t)
	ref := deployRequest{org: "org", database: "db", number: 6}
	insertDeployRequest(t, s, ref, dr.InProgress)
	require.NoError(t, s.updateDeployState(t.Context(), ref, dr.InProgressCancel), "the cancel handler records the cancel")

	applied, err := s.advanceDeployState(t.Context(), ref, dr.InProgress, dr.PendingCutover)
	require.NoError(t, err)
	assert.False(t, applied, "the derived state is not written over the cancel")
	assert.Equal(t, dr.InProgressCancel, storedDeployState(t, s, ref))
}

// With nothing recorded since the processor read the request, its derived
// state is written, and entering the revert window starts the window's clock.
func TestAdvanceDeployStateWritesTheDerivedState(t *testing.T) {
	s := newMetadataOnlyServer(t)
	ref := deployRequest{org: "org", database: "db", number: 7}
	insertDeployRequest(t, s, ref, dr.InProgressCutover)

	applied, err := s.advanceDeployState(t.Context(), ref, dr.InProgressCutover, dr.CompletePendingRevert)
	require.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, dr.CompletePendingRevert, storedDeployState(t, s, ref))
	var expires sql.NullTime
	require.NoError(t, s.metadataDB.QueryRowContext(t.Context(),
		`SELECT revert_expires_at FROM localscale_deploy_requests WHERE number = ?`, ref.number).Scan(&expires))
	assert.True(t, expires.Valid, "the revert window's expiry is recorded")
}
