package planetscale

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
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

// autoCutoverReportingClient answers what the backend holds for a deploy
// request. When err is set it is returned instead of an answer: for the first
// failures reads when failures is positive, and for every read otherwise. reads
// counts every read it was asked for.
type autoCutoverReportingClient struct {
	psclient.PSClient
	autoCutover bool
	err         error
	failures    int
	reads       int
}

func (c *autoCutoverReportingClient) DeployRequestAutoCutover(_ context.Context, _, _ string, _ uint64) (bool, error) {
	c.reads++
	if c.err != nil && (c.failures <= 0 || c.reads <= c.failures) {
		return false, c.err
	}
	return c.autoCutover, nil
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

// Cutover ownership is settled when the deploy request is created and can never
// be changed afterwards. The operator's timeline states it, because the apply's
// log surface carries lifecycle events and not the engine's own log lines — so
// a decision left in the engine logger cannot be read from the schema change.
//
// The created deploy request echoes back no cutover setting, so the event states
// what SchemaBot requested and says so: an operator who reads it as a confirmed
// fact would take a deploy request that quietly kept auto-cutover for one that
// holds it.
func TestDeployRequestCreatedEventStatesRequestedCutoverOwnership(t *testing.T) {
	event := deployRequestCreatedEvent(&ps.DeployRequest{Number: 132, HtmlURL: "https://example.test/deploy-requests/132"}, "schema-change-branch")

	assert.Contains(t, event.Message, "#132")
	assert.Contains(t, event.Message, "requesting SchemaBot hold the cutover")
	assert.Equal(t, "false", event.Metadata["requested_auto_cutover"])
	assert.NotContains(t, event.Metadata, "auto_cutover")
	assert.Equal(t, "132", event.Metadata["deploy_request_id"])
	assert.Equal(t, "https://example.test/deploy-requests/132", event.Metadata["deploy_request_url"])
	assert.Equal(t, "schema-change-branch", event.Metadata["branch"])
	assert.Equal(t, state.Apply.ValidatingDeployRequest, event.NewState)
}

// A deferred cutover is only held if the deploy request the backend holds says
// so, and that setting is settled at creation and cannot be changed afterwards.
// Reading it back before deploying is the last point at which a schema change
// that would swap on its own can still be stopped, so an unheld — or unreadable
// — cutover stops the deploy rather than starting one that cuts over without
// the operator.
func TestVerifyCutoverHeld(t *testing.T) {
	e := &Engine{logger: slog.New(slog.NewTextHandler(os.Stdout, nil))}

	t.Run("a held cutover deploys", func(t *testing.T) {
		err := e.verifyCutoverHeld(t.Context(), &autoCutoverReportingClient{autoCutover: false}, "org", "mydb", 132)
		require.NoError(t, err)
	})

	t.Run("a backend that would cut over on its own stops the deploy", func(t *testing.T) {
		client := &autoCutoverReportingClient{autoCutover: true}

		err := e.verifyCutoverHeld(t.Context(), client, "org", "mydb", 132)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "#132 was not deployed")
		assert.Contains(t, err.Error(), "swap the schema on its own")
		assert.Contains(t, err.Error(), "re-run the schema change")
		assert.Equal(t, 1, client.reads, "auto-cutover on is an answer, so it is not read again")
	})

	t.Run("an unreadable setting stops the deploy", func(t *testing.T) {
		client := &autoCutoverReportingClient{err: errors.New("deploy request read timed out")}

		err := e.verifyCutoverHeld(t.Context(), client, "org", "mydb", 132)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "#132 was not deployed")
		assert.Contains(t, err.Error(), "could not confirm the cutover is held")
		assert.Contains(t, err.Error(), "deploy request read timed out")
		assert.Greater(t, client.reads, 1, "a read that does not answer is read again before it refuses")
	})

	// A read that answers on a later attempt is an answer. Refusing on the first
	// one would fail an apply whose cutover was held all along, and a gate that
	// refuses at random is one operators learn to re-run past.
	t.Run("a setting that reads on a retry deploys", func(t *testing.T) {
		client := &autoCutoverReportingClient{
			autoCutover: false,
			err:         psclient.ErrDeploymentNotReported,
			failures:    1,
		}

		err := e.verifyCutoverHeld(t.Context(), client, "org", "mydb", 132)

		require.NoError(t, err)
		assert.Equal(t, 2, client.reads)
	})

	t.Run("a cancelled context stops the deploy", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := e.verifyCutoverHeld(ctx, &autoCutoverReportingClient{err: errors.New("deploy request read timed out")}, "org", "mydb", 132)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "#132 was not deployed")
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// Instant DDL swaps the schema in the same step that runs the deploy: there is
// no pending_cutover for a deferred cutover to park at, and no revert window to
// undo an unsafe change. An operator who held the cutover kept that decision
// for themselves, and an unsafe change must stay revertible — so in both cases
// an eligible change is deployed with a row copy instead: slower, but it stops
// at the gate and keeps the safety net.
func TestUseInstantDDL(t *testing.T) {
	eligible := &ps.DeployRequest{Deployment: &ps.Deployment{InstantDDLEligible: true}}
	ineligible := &ps.DeployRequest{Deployment: &ps.Deployment{InstantDDLEligible: false}}

	assert.True(t, useInstantDDL(eligible, false, false), "an eligible safe change with no held cutover deploys instantly")
	assert.False(t, useInstantDDL(eligible, true, false), "a held cutover needs a gate, so instant DDL is declined")
	assert.False(t, useInstantDDL(eligible, false, true), "an unsafe change needs a revert window, so instant DDL is declined")
	assert.False(t, useInstantDDL(eligible, true, true), "a held cutover and an unsafe change each decline instant DDL on their own")
	assert.False(t, useInstantDDL(ineligible, false, false))
	assert.False(t, useInstantDDL(&ps.DeployRequest{}, false, false), "a deploy request with no deployment is not eligible")
}

// An unsafe change — one that destroys existing data, in Spirit's unsafe
// vocabulary — must keep the revert window that only the row-copy path
// provides. Safe additive changes carry no such risk and stay eligible for
// instant DDL. A statement that cannot be classified fails closed to the
// row-copy path: the cost of a misclassification must only ever be time,
// never data.
func TestChangesContainUnsafe(t *testing.T) {
	e := &Engine{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	changes := func(ddls ...string) []engine.SchemaChange {
		tcs := make([]engine.TableChange, 0, len(ddls))
		for _, d := range ddls {
			tcs = append(tcs, engine.TableChange{DDL: d})
		}
		return []engine.SchemaChange{{Namespace: "orders", TableChanges: tcs}}
	}

	t.Run("DROP TABLE is unsafe", func(t *testing.T) {
		unsafe, reason := e.changesContainUnsafe(changes("DROP TABLE `users`"), "mydb")
		assert.True(t, unsafe)
		assert.Contains(t, reason, "DROP TABLE")
	})

	t.Run("ALTER TABLE DROP COLUMN is unsafe", func(t *testing.T) {
		unsafe, reason := e.changesContainUnsafe(changes("ALTER TABLE `users` DROP COLUMN `email`"), "mydb")
		assert.True(t, unsafe)
		assert.Contains(t, reason, "DROP COLUMN")
		assert.Contains(t, reason, "email")
	})

	t.Run("an unsafe statement among additive changes is unsafe", func(t *testing.T) {
		unsafe, reason := e.changesContainUnsafe(changes(
			"ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)",
			"ALTER TABLE `users` DROP COLUMN `fax`",
		), "mydb")
		assert.True(t, unsafe)
		assert.Contains(t, reason, "fax")
	})

	t.Run("additive changes are safe", func(t *testing.T) {
		unsafe, reason := e.changesContainUnsafe(changes(
			"CREATE TABLE `audit` (`id` BIGINT UNSIGNED AUTO_INCREMENT, PRIMARY KEY (`id`))",
			"ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)",
		), "mydb")
		assert.False(t, unsafe)
		assert.Empty(t, reason)
	})

	t.Run("an unclassifiable statement fails closed", func(t *testing.T) {
		unsafe, reason := e.changesContainUnsafe(changes("THIS IS NOT SQL"), "mydb")
		assert.True(t, unsafe)
		assert.NotEmpty(t, reason)
	})

	t.Run("no changes are safe", func(t *testing.T) {
		unsafe, reason := e.changesContainUnsafe(nil, "mydb")
		assert.False(t, unsafe)
		assert.Empty(t, reason)
	})
}
