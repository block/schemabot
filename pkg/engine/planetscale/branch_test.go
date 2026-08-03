package planetscale

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/spirit/pkg/table"
)

func TestDiffKeyspace_DetectsSchemaChanges(t *testing.T) {
	e := &Engine{
		linter: lint.New(),
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	t.Run("matching schemas produce no changes", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {
				{Name: "users", Schema: "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
			},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		assert.Empty(t, changes, "matching schemas should produce no changes")
	})

	t.Run("missing column detected as ALTER", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {
				{Name: "users", Schema: "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
			},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `phone` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		require.Len(t, changes, 1, "should detect one ALTER TABLE change")
		assert.Equal(t, "users", changes[0].Table)
		assert.Contains(t, changes[0].DDL, "phone")
	})

	t.Run("extra column on branch detected as ALTER DROP", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {
				{Name: "users", Schema: "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `stale_col` varchar(100) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
			},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		require.Len(t, changes, 1, "should detect DROP COLUMN for stale column")
		assert.Equal(t, "users", changes[0].Table)
		assert.Contains(t, changes[0].DDL, "stale_col")
	})

	t.Run("missing table detected as CREATE", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		require.Len(t, changes, 1, "should detect CREATE TABLE")
		assert.Equal(t, "users", changes[0].Table)
		assert.Equal(t, ddl.StatementCreateTable, changes[0].Operation)
	})
}

// vschemaFetchStubClient serves a fixed response for keyspace VSchema reads so
// tests can drive the VSchema half of diffKeyspace without a live API.
type vschemaFetchStubClient struct {
	psclient.PSClient
	vschema *ps.VSchema
	err     error
}

func (c *vschemaFetchStubClient) GetKeyspaceVSchema(_ context.Context, _ *ps.GetKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	return c.vschema, c.err
}

// A NotFound on the keyspace VSchema read means the keyspace has no VSchema on
// the branch yet — first-time VSchema creation, or the API lagging a recent
// write. The diff must treat that as an empty current VSchema so the desired
// VSchema surfaces as a change (plan reports it will be created; post-apply
// validation reports a difference that the staleness retry re-polls) instead
// of failing the operation. Any other fetch error must still fail the diff.
func TestDiffKeyspace_VSchemaFetchErrors(t *testing.T) {
	e := &Engine{
		linter: lint.New(),
		logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	desiredVSchema := `{"sharded": false, "tables": {"users_seq": {"type": "sequence"}}}`
	desired := &schema.Namespace{
		Files: map[string]string{
			"vschema.json": desiredVSchema,
		},
	}
	currentSchema := map[string][]table.TableSchema{"myapp": {}}

	t.Run("NotFound treated as empty current VSchema", func(t *testing.T) {
		client := &vschemaFetchStubClient{err: &ps.Error{Code: ps.ErrNotFound}}
		changes, vschemaChanged, currentRaw, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "myapp", desired, currentSchema)
		require.NoError(t, err)
		assert.Empty(t, changes)
		assert.True(t, vschemaChanged, "desired VSchema must surface as a change against a missing one")
		assert.Empty(t, currentRaw)
	})

	t.Run("non-NotFound API error fails the diff", func(t *testing.T) {
		client := &vschemaFetchStubClient{err: &ps.Error{Code: ps.ErrInternal}}
		_, _, _, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "myapp", desired, currentSchema)
		require.ErrorContains(t, err, "fetch VSchema for keyspace myapp")
	})

	t.Run("non-API error fails the diff", func(t *testing.T) {
		client := &vschemaFetchStubClient{err: errors.New("dial tcp: connection refused")}
		_, _, _, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "myapp", desired, currentSchema)
		require.ErrorContains(t, err, "fetch VSchema for keyspace myapp")
	})

	t.Run("matching VSchema reports no change", func(t *testing.T) {
		client := &vschemaFetchStubClient{vschema: &ps.VSchema{Raw: desiredVSchema}}
		changes, vschemaChanged, currentRaw, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "myapp", desired, currentSchema)
		require.NoError(t, err)
		assert.Empty(t, changes)
		assert.False(t, vschemaChanged)
		assert.Equal(t, desiredVSchema, currentRaw)
	})
}

// deployRequestRecorderClient captures the deploy-request creation payload so
// tests can assert on the options the engine sends to the PlanetScale API.
type deployRequestRecorderClient struct {
	psclient.PSClient
	created *ps.CreateDeployRequestRequest
}

func (c *deployRequestRecorderClient) CreateDeployRequest(_ context.Context, req *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error) {
	c.created = req
	return &ps.DeployRequest{Number: 1}, nil
}

// SchemaBot is the sole cutover actor: deploy requests are created with
// PlanetScale's auto-cutover disabled so they park at pending_cutover until
// the driver (or a deferred-cutover operator) completes the cutover. If
// PlanetScale cut over on its own, the schema could move without SchemaBot's
// involvement or caller attribution.
func TestCreateDeployRequest_DisablesPlanetScaleAutoCutover(t *testing.T) {
	e := &Engine{logger: slog.New(slog.NewTextHandler(os.Stdout, nil))}
	client := &deployRequestRecorderClient{}

	_, err := e.createDeployRequest(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "main", true)

	require.NoError(t, err)
	require.NotNil(t, client.created)
	assert.False(t, client.created.AutoCutover)
	assert.True(t, client.created.AutoDeleteBranch)
	assert.Equal(t, "schemabot-mydb-abc", client.created.Branch)
	assert.Equal(t, "main", client.created.IntoBranch)
}
