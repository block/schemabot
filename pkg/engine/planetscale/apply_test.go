package planetscale

import (
	"context"
	"log/slog"
	"os"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

type permanentVSchemaErrorClient struct {
	psclient.PSClient
	updateCalls int
}

var _ psclient.PSClient = (*permanentVSchemaErrorClient)(nil)

func (c *permanentVSchemaErrorClient) UpdateKeyspaceVSchema(context.Context, *ps.UpdateKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	c.updateCalls++
	return nil, &ps.Error{Code: ps.ErrInvalid}
}

func TestApply_MainBranchReuseIsPermanent(t *testing.T) {
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)), func(_, _ string) (psclient.PSClient, error) {
		return nil, nil
	})

	_, err := e.Apply(t.Context(), &engine.ApplyRequest{
		Database: "testdb",
		Options: map[string]string{
			"branch": "main",
		},
		Credentials: &engine.Credentials{
			Metadata: map[string]string{
				"organization": "org",
				"token_name":   "token",
				"token_value":  "secret",
				"main_branch":  "main",
			},
		},
	})

	require.Error(t, err)
	assert.False(t, engine.IsRetryable(err))
	assert.Contains(t, err.Error(), "cannot reuse the main branch")
}

func TestApplyKeyspaceChanges_PermanentVSchemaErrorIsPermanent(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &permanentVSchemaErrorClient{}

	err := e.applyKeyspaceChanges(t.Context(),
		engine.SchemaChange{
			Namespace: "testapp",
			Metadata:  map[string]string{"vschema_changed": "true"},
		},
		schema.SchemaFiles{
			"testapp": &schema.Namespace{Files: map[string]string{"vschema.json": "{}"}},
		},
		&ps.DatabaseBranchPassword{},
		client,
		"org",
		"database",
		"branch",
	)

	require.Error(t, err)
	assert.False(t, engine.IsRetryable(err))
	assert.Equal(t, 1, client.updateCalls)
}
