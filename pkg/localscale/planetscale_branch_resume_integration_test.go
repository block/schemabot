//go:build integration

package localscale_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

const (
	resumeLandedTable  = "CREATE TABLE `resume_landed` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	resumeMissingTable = "CREATE TABLE `resume_missing` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	resumeStrayTable   = "CREATE TABLE `resume_stray` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
)

// A driver that dies after creating the branch and running part of the plan
// on it leaves the next driver to resume from that branch. The resume reads
// the branch's real schema and runs only the planned tables the branch still
// lacks: the table that already landed is not created a second time, which
// would fail the apply on a table-already-exists error.
//
// The plan creates two tables in an unsharded keyspace; the branch already
// carries one of them. The resume must create the other, leave the existing
// one alone, and reach the deploy request. PlanetScale's schema API can still
// report a branch that has only just become ready as having no keyspaces, so
// the engine here sees that answer from the API; the resume must read the
// branch's schema from the branch itself rather than diff from nothing.
func TestPlanetScaleResumeFromBranchRunsOnlyTheRemainingPlannedChanges(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	deferCleanupActiveDeployRequests(t)
	ctx := t.Context()
	const keyspace = "testapp"

	desired := desiredSchemaWith(t, ctx, keyspace, resumeLandedTable, resumeMissingTable)
	branch := createBranchWithDDL(t, ctx, "resume-partial", map[string][]string{keyspace: {resumeLandedTable}}, nil)

	eng := resumeTestEngineWith(&laggingSchemaAPIClient{PSClient: testClient})
	result, err := eng.Apply(ctx, resumeFromBranchRequest(t, ctx, branch, desired, keyspace))
	require.NoError(t, err, "a resume must run only the planned tables the branch still lacks")
	require.NotNil(t, result)
	assert.True(t, result.Accepted)
	t.Cleanup(func() { closeDeferredDeployRequest(t, eng, result.ResumeState) })

	tables := branchTableNames(t, ctx, branch, keyspace)
	assert.Contains(t, tables, "resume_landed")
	assert.Contains(t, tables, "resume_missing", "the planned table the branch lacked must be created on resume")
}

// A resumed branch that differs from the declared schema on a table the plan
// does not change is refused rather than repaired. The branch was cut to carry
// exactly the planned DDL, so the stray table means main moved or the branch
// was touched; creating, altering, or dropping it would apply a schema change
// nobody reviewed. The refusal is permanent, names the table, and runs none of
// the plan's DDL on the branch.
//
// The refusal ends the apply before any deploy request exists to tear the
// branch down, so a branch SchemaBot created is deleted rather than stranded
// against quota, and a delete that fails does not replace the refusal. An
// operator-supplied branch is the operator's and is left in place.
func TestPlanetScaleResumeFromBranchRefusesChangesOutsideThePlan(t *testing.T) {
	tests := []struct {
		name       string
		operator   bool
		deleteErr  error
		wantDelete bool
	}{
		{name: "a branch SchemaBot created is deleted even when the delete fails", deleteErr: errors.New("branch delete refused"), wantDelete: true},
		{name: "an operator-supplied branch is left in place", operator: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanupActiveDeployRequests(t, t.Context())
			deferCleanupActiveDeployRequests(t)
			ctx := t.Context()
			const keyspace = "testapp"

			desired := desiredSchemaWith(t, ctx, keyspace, resumeLandedTable, resumeMissingTable)
			branch := createBranchWithDDL(t, ctx, "resume-stray", map[string][]string{keyspace: {resumeLandedTable, resumeStrayTable}}, nil)
			req := resumeFromBranchRequest(t, ctx, branch, desired, keyspace)
			if tt.operator {
				req.Options["branch"] = branch
			}
			client := &branchDeleteRecorder{PSClient: testClient, deleteErr: tt.deleteErr}

			result, err := resumeTestEngineWith(client).Apply(ctx, req)
			require.Error(t, err)
			assert.Nil(t, result)
			var permanent *engine.PermanentError
			assert.True(t, errors.As(err, &permanent), "a branch that differs outside the plan cannot be fixed by retrying: %v", err)
			assert.Contains(t, err.Error(), keyspace+".resume_stray")
			assert.NotContains(t, err.Error(), "branch delete refused", "a failed delete must not replace the refusal")

			tables := branchTableNames(t, ctx, branch, keyspace)
			assert.NotContains(t, tables, "resume_missing", "a refused resume must run none of the plan's DDL")
			if tt.wantDelete {
				assert.Equal(t, []string{branch}, client.deletedBranches())
			} else {
				assert.Empty(t, client.deletedBranches())
			}
		})
	}
}

// A resumed branch whose remaining planned change is refused for good ends the
// apply before any deploy request exists. A branch SchemaBot created is
// deleted rather than stranded against quota; an operator-supplied branch is
// the operator's and is left in place.
func TestPlanetScaleResumeFromBranchReclaimsTheBranchWhenTheRemainingChangeIsRefused(t *testing.T) {
	tests := []struct {
		name       string
		operator   bool
		wantDelete bool
	}{
		{name: "a branch SchemaBot created is deleted", wantDelete: true},
		{name: "an operator-supplied branch is left in place", operator: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanupActiveDeployRequests(t, t.Context())
			deferCleanupActiveDeployRequests(t)
			ctx := t.Context()
			const keyspace = "testapp"

			desired := desiredSchemaWith(t, ctx, keyspace)
			desired[keyspace].Files["vschema.json"] = `{"tables": {}}`
			branch := createBranch(t, ctx, "resume-vschema")
			req := resumeFromBranchRequest(t, ctx, branch, desired, keyspace)
			req.Changes = []engine.SchemaChange{{
				Namespace: keyspace,
				Metadata:  map[string]string{"vschema_changed": "true"},
			}}
			if tt.operator {
				req.Options["branch"] = branch
			}
			client := &branchDeleteRecorder{PSClient: &vschemaRejectingClient{PSClient: testClient}}

			result, err := resumeTestEngineWith(client).Apply(ctx, req)
			require.Error(t, err)
			assert.Nil(t, result)
			var permanent *engine.PermanentError
			assert.True(t, errors.As(err, &permanent), "a rejected VSchema cannot be fixed by retrying: %v", err)
			assert.Contains(t, err.Error(), "apply remaining changes on resume")

			if tt.wantDelete {
				assert.Equal(t, []string{branch}, client.deletedBranches())
			} else {
				assert.Empty(t, client.deletedBranches())
			}
		})
	}
}

// branchDeleteRecorder records each branch the engine deletes and serves
// everything else from the wrapped client. LocalScale serves no branch
// deletion, so the record is what shows the engine's teardown decision;
// deleteErr, when set, fails each delete after recording it.
type branchDeleteRecorder struct {
	psclient.PSClient
	deleteErr error

	mu      sync.Mutex
	deleted []string
}

func (c *branchDeleteRecorder) DeleteBranch(_ context.Context, req *ps.DeleteDatabaseBranchRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, req.Branch)
	return c.deleteErr
}

func (c *branchDeleteRecorder) deletedBranches() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deleted...)
}

// vschemaRejectingClient refuses every VSchema write the way PlanetScale
// refuses an invalid one, a rejection no retry can clear.
type vschemaRejectingClient struct {
	psclient.PSClient
}

func (c *vschemaRejectingClient) UpdateKeyspaceVSchema(context.Context, *ps.UpdateKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	return nil, &ps.Error{Code: ps.ErrInvalid}
}

// laggingSchemaAPIClient answers the branch schema API the way PlanetScale can
// for a branch that has only just become ready: as if its keyspaces did not
// exist yet. Everything else is served by LocalScale.
type laggingSchemaAPIClient struct {
	psclient.PSClient
}

func (c *laggingSchemaAPIClient) GetBranchSchema(context.Context, *ps.BranchSchemaRequest) ([]*ps.Diff, error) {
	return nil, &ps.Error{Code: ps.ErrNotFound}
}

func resumeTestEngineWith(client psclient.PSClient) *planetscale.Engine {
	return planetscale.NewWithClient(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		func(_, _ string) (psclient.PSClient, error) { return client, nil },
	)
}

// resumeFromBranchRequest is the request a driver resuming an apply builds when
// the stored resume state names a branch but no deploy request yet. The plan
// creates resume_landed and resume_missing; defer_deploy stops the resume at
// the deploy request so the test never changes main.
func resumeFromBranchRequest(t *testing.T, ctx context.Context, branch string, desired schema.SchemaFiles, keyspace string) *engine.ApplyRequest {
	t.Helper()
	rs, err := planetscale.BuildResumeState(planetscale.ResumeData{BranchName: branch})
	require.NoError(t, err, "build resume state")
	return &engine.ApplyRequest{
		Database:    testDB,
		Credentials: newRecoveryCredentials(t, ctx),
		SchemaFiles: desired,
		Changes: []engine.SchemaChange{{
			Namespace: keyspace,
			TableChanges: []engine.TableChange{
				{Table: "resume_landed", Operation: ddl.StatementCreateTable, DDL: resumeLandedTable},
				{Table: "resume_missing", Operation: ddl.StatementCreateTable, DDL: resumeMissingTable},
			},
		}},
		Options:     map[string]string{"defer_deploy": "true"},
		ResumeState: rs,
	}
}

// desiredSchemaWith declares main's current keyspace schema plus the given
// tables, the schema a plan that adds only those tables would carry.
func desiredSchemaWith(t *testing.T, ctx context.Context, keyspace string, extra ...string) schema.SchemaFiles {
	t.Helper()
	files := map[string]string{}
	for _, tbl := range loadBranchSchema(t, ctx, "main", keyspace) {
		files[tbl.Name+".sql"] = tbl.Schema + ";"
	}
	for i, stmt := range extra {
		files[fmt.Sprintf("resume_extra_%d.sql", i)] = stmt + ";"
	}
	return schema.SchemaFiles{keyspace: {Files: files}}
}

func branchTableNames(t *testing.T, ctx context.Context, branch, keyspace string) []string {
	t.Helper()
	var names []string
	for _, tbl := range loadBranchSchema(t, ctx, branch, keyspace) {
		names = append(names, tbl.Name)
	}
	return names
}

// loadBranchSchema reads a keyspace's tables on a branch over MySQL, the same
// read the engine's resume makes.
func loadBranchSchema(t *testing.T, ctx context.Context, branch, keyspace string) []table.TableSchema {
	t.Helper()
	pw, err := testClient.CreateBranchPassword(ctx, &ps.DatabaseBranchPasswordRequest{
		Organization: testOrg, Database: testDB, Branch: branch,
	})
	require.NoError(t, err, "CreateBranchPassword for %s", branch)
	db, err := sql.Open("block-mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", pw.Username, pw.PlainText, pw.Hostname, keyspace))
	require.NoError(t, err, "open %s MySQL for %s", branch, keyspace)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx), "ping %s MySQL for %s", branch, keyspace)
	tables, err := table.LoadSchemaFromDB(ctx, db, table.WithoutUnderscoreTables)
	require.NoError(t, err, "load %s schema for %s", branch, keyspace)
	return tables
}

// closeDeferredDeployRequest closes the undeployed deploy request a deferred
// resume created, so it never deploys to main.
func closeDeferredDeployRequest(t *testing.T, eng *planetscale.Engine, rs *engine.ResumeState) {
	t.Helper()
	ctx, cancel := testutil.CleanupContext(cleanupTimeout)
	defer cancel()
	_, err := eng.Cancel(ctx, &engine.ControlRequest{
		Database:    testDB,
		ResumeState: rs,
		Credentials: &engine.Credentials{Metadata: map[string]string{
			"organization": testOrg,
			"token_name":   "test",
			"token_value":  "test",
		}},
	})
	assert.NoError(t, err, "close the deferred deploy request")
}
