package planetscale

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// databaseCapturingClient records the database name of every PlanetScale API
// call so a test can assert which PlanetScale database the engine addressed.
type databaseCapturingClient struct {
	psclient.PSClient

	// branch and branchErr configure GetBranch for the entry points that read
	// a branch before doing anything else.
	branch    *ps.DatabaseBranch
	branchErr error

	mu        sync.Mutex
	databases []string
}

func (c *databaseCapturingClient) record(database string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.databases = append(c.databases, database)
}

func (c *databaseCapturingClient) recorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.databases...)
}

func (c *databaseCapturingClient) GetBranch(_ context.Context, req *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	c.record(req.Database)
	if c.branchErr != nil {
		return nil, c.branchErr
	}
	return c.branch, nil
}

func (c *databaseCapturingClient) ListKeyspaces(_ context.Context, req *ps.ListKeyspacesRequest) ([]*ps.Keyspace, error) {
	c.record(req.Database)
	return nil, nil
}

func (c *databaseCapturingClient) GetDeployRequest(_ context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.record(req.Database)
	return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.Complete}, nil
}

func (c *databaseCapturingClient) CancelDeployRequest(_ context.Context, req *ps.CancelDeployRequestRequest) (*ps.DeployRequest, error) {
	c.record(req.Database)
	return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.Cancelled}, nil
}

func (c *databaseCapturingClient) DeployDeployRequest(_ context.Context, req *ps.PerformDeployRequest) (*ps.DeployRequest, error) {
	c.record(req.Database)
	return &ps.DeployRequest{Number: req.Number}, nil
}

func (c *databaseCapturingClient) ApplyDeployRequest(_ context.Context, req *ps.ApplyDeployRequestRequest) (*ps.DeployRequest, error) {
	c.record(req.Database)
	return &ps.DeployRequest{Number: req.Number}, nil
}

func (c *databaseCapturingClient) RevertDeployRequest(_ context.Context, req *ps.RevertDeployRequestRequest) (*ps.DeployRequest, error) {
	c.record(req.Database)
	return &ps.DeployRequest{Number: req.Number}, nil
}

func (c *databaseCapturingClient) SkipRevertDeployRequest(_ context.Context, req *ps.SkipRevertDeployRequestRequest) (*ps.DeployRequest, error) {
	c.record(req.Database)
	return &ps.DeployRequest{Number: req.Number}, nil
}

// The database a target is registered under is an arbitrary routing key; when
// the credential metadata carries the PlanetScale database name, every
// PlanetScale API call made by every exported engine method must address that
// name. Without the metadata the registered identifier doubles as the
// PlanetScale name. Each entry point is exercised against a client that
// records the database of every API call it receives.
func TestEngine_AddressesPlanetScaleDatabaseFromCredentialMetadata(t *testing.T) {
	activeMeta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  7,
		DeployRequestURL: "https://example.test/deploys/7",
	})
	require.NoError(t, err)
	deferredMeta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  7,
		DeployRequestURL: "https://example.test/deploys/7",
		DeferredDeploy:   true,
	})
	require.NoError(t, err)

	activeResume := func() *engine.ResumeState { return &engine.ResumeState{Metadata: activeMeta} }
	deferredResume := func() *engine.ResumeState { return &engine.ResumeState{Metadata: deferredMeta} }

	entryPoints := []struct {
		name string
		prep func(c *databaseCapturingClient)
		call func(t *testing.T, e *Engine, creds *engine.Credentials) error
		// wantErr marks entry points whose fake halts the flow with an error
		// after the database name has been recorded; the recorded name is
		// still the assertion that matters.
		wantErr bool
	}{
		{
			name:    "Apply",
			prep:    func(c *databaseCapturingClient) { c.branchErr = errors.New("parent branch unavailable") },
			wantErr: true,
			call: func(t *testing.T, e *Engine, creds *engine.Credentials) error {
				_, err := e.Apply(t.Context(), &engine.ApplyRequest{Database: "mydb", PlanID: "plan-1", Credentials: creds})
				return err
			},
		},
		{
			name: "Plan",
			prep: func(c *databaseCapturingClient) { c.branch = &ps.DatabaseBranch{SafeMigrations: true} },
			call: func(t *testing.T, e *Engine, creds *engine.Credentials) error {
				_, err := e.Plan(t.Context(), &engine.PlanRequest{Database: "mydb", Credentials: creds})
				return err
			},
		},
		{
			name: "Progress",
			call: func(t *testing.T, e *Engine, creds *engine.Credentials) error {
				_, err := e.Progress(t.Context(), &engine.ProgressRequest{Database: "mydb", ResumeState: activeResume(), Credentials: creds})
				return err
			},
		},
		{
			name: "Stop",
			call: func(t *testing.T, e *Engine, creds *engine.Credentials) error {
				_, err := e.Stop(t.Context(), &engine.ControlRequest{Database: "mydb", ResumeState: activeResume(), Credentials: creds})
				return err
			},
		},
		{
			name: "Cancel",
			call: func(t *testing.T, e *Engine, creds *engine.Credentials) error {
				_, err := e.Cancel(t.Context(), &engine.ControlRequest{Database: "mydb", ResumeState: activeResume(), Credentials: creds})
				return err
			},
		},
		{
			name: "Start",
			call: func(t *testing.T, e *Engine, creds *engine.Credentials) error {
				_, err := e.Start(t.Context(), &engine.ControlRequest{Database: "mydb", ResumeState: deferredResume(), Credentials: creds})
				return err
			},
		},
		{
			name: "Cutover",
			call: func(t *testing.T, e *Engine, creds *engine.Credentials) error {
				_, err := e.Cutover(t.Context(), &engine.ControlRequest{Database: "mydb", ResumeState: activeResume(), Credentials: creds})
				return err
			},
		},
		{
			name: "Revert",
			call: func(t *testing.T, e *Engine, creds *engine.Credentials) error {
				_, err := e.Revert(t.Context(), &engine.ControlRequest{Database: "mydb", ResumeState: activeResume(), Credentials: creds})
				return err
			},
		},
		{
			name: "SkipRevert",
			call: func(t *testing.T, e *Engine, creds *engine.Credentials) error {
				_, err := e.SkipRevert(t.Context(), &engine.ControlRequest{Database: "mydb", ResumeState: activeResume(), Credentials: creds})
				return err
			},
		},
	}

	credCases := []struct {
		name     string
		metadata map[string]string
		want     string
	}{
		{
			name: "credential metadata database addresses the API",
			metadata: map[string]string{
				"organization": "org",
				"token_name":   "tn",
				"token_value":  "tv",
				"database":     "mydb_main",
			},
			want: "mydb_main",
		},
		{
			name: "without the metadata the registered identifier is the name",
			metadata: map[string]string{
				"organization": "org",
				"token_name":   "tn",
				"token_value":  "tv",
			},
			want: "mydb",
		},
	}

	for _, ep := range entryPoints {
		for _, cc := range credCases {
			t.Run(ep.name+"/"+cc.name, func(t *testing.T) {
				client := &databaseCapturingClient{}
				if ep.prep != nil {
					ep.prep(client)
				}
				e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
					func(_, _ string) (psclient.PSClient, error) { return client, nil })

				err := ep.call(t, e, &engine.Credentials{Metadata: cc.metadata})

				if ep.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				recorded := client.recorded()
				require.NotEmpty(t, recorded, "the entry point must reach at least one PlanetScale API call")
				for _, database := range recorded {
					assert.Equal(t, cc.want, database)
				}
			})
		}
	}
}
