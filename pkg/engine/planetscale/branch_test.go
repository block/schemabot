package planetscale

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
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
		diff, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.NoError(t, err)
		assert.Empty(t, diff.tableChanges, "matching schemas should produce no changes")
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
		diff, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.NoError(t, err)
		require.Len(t, diff.tableChanges, 1, "should detect one ALTER TABLE change")
		assert.Equal(t, "users", diff.tableChanges[0].Table)
		assert.Contains(t, diff.tableChanges[0].DDL, "phone")
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
		diff, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.NoError(t, err)
		require.Len(t, diff.tableChanges, 1, "should detect DROP COLUMN for stale column")
		assert.Equal(t, "users", diff.tableChanges[0].Table)
		assert.Contains(t, diff.tableChanges[0].DDL, "stale_col")
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
		diff, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.NoError(t, err)
		require.Len(t, diff.tableChanges, 1, "should detect CREATE TABLE")
		assert.Equal(t, "users", diff.tableChanges[0].Table)
		assert.Equal(t, ddl.StatementCreateTable, diff.tableChanges[0].Operation)
	})
}

// Vitess will not accept a foreign key, so a plan that offered to apply one
// would be offering a guaranteed failure — and the author would learn that only
// after the apply had run. The plan refuses it up front instead. Dropping a
// foreign key is still applicable: that is how a legacy constraint gets removed.
func TestDiffKeyspace_RefusesForeignKeys(t *testing.T) {
	e := &Engine{
		linter: lint.New(),
		logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	usersTable := "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	t.Run("a new table declaring a foreign key is refused", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{"myapp": {{Name: "users", Schema: usersTable}}}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql":  usersTable + ";",
				"orders.sql": "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `user_id` bigint NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_user` (`user_id`),\n  CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}

		diff, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.NoError(t, err)
		require.Len(t, diff.tableChanges, 1)
		assert.Equal(t, "orders", diff.tableChanges[0].Table)
		assert.Equal(t, engine.ExecutionModeBlocked, diff.tableChanges[0].ExecutionMode)
		assert.Equal(t, foreignKeyRefusalReason, diff.tableChanges[0].ModeReason)
	})

	t.Run("an unrelated change to a table that has one is applicable", func(t *testing.T) {
		ordersWithFK := "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `user_id` bigint NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_user` (`user_id`),\n  CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
		currentSchema := map[string][]table.TableSchema{"myapp": {{Name: "orders", Schema: ordersWithFK}}}
		desired := &schema.Namespace{
			Files: map[string]string{
				"orders.sql": "CREATE TABLE `orders` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `user_id` bigint NOT NULL,\n  `note` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_user` (`user_id`),\n  CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}

		diff, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.NoError(t, err)
		require.Len(t, diff.tableChanges, 1)
		assert.Contains(t, diff.tableChanges[0].DDL, "note")
		assert.Empty(t, diff.tableChanges[0].ExecutionMode, "adding a column is not the constraint Vitess refuses")
	})
}

// Two schema files in one keyspace both declare `orders`: orders.sql keeps the
// live `email` column and orders_extras.sql adds `note` instead. The differ can
// keep only one definition per table, so planning either definition would
// silently discard the other and could drop `email`. The plan is refused with
// an error naming the table and both files, on every run.
func TestDiffKeyspace_RefusesTableDeclaredTwice(t *testing.T) {
	e := &Engine{
		linter: lint.New(),
		logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	liveOrders := "CREATE TABLE `orders` (\n  `id` bigint NOT NULL,\n  `email` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	withNote := "CREATE TABLE `orders` (\n  `id` bigint NOT NULL,\n  `note` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	currentSchema := map[string][]table.TableSchema{"myapp": {{Name: "orders", Schema: liveOrders}}}

	desired := &schema.Namespace{Files: map[string]string{
		"orders.sql":        liveOrders + ";",
		"orders_extras.sql": withNote,
	}}

	// Map iteration order varies between runs, so plan repeatedly: every run
	// must refuse with the same, fully named error.
	for range 20 {
		diff, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.EqualError(t, err, `table "orders" is declared by both schema files "myapp/orders.sql" and "myapp/orders_extras.sql". Declare each table in exactly one schema file`)
		assert.Empty(t, diff.tableChanges)
	}
}

// vschemaFetchStubClient serves a fixed response for keyspace VSchema reads so
// tests can drive the VSchema half of diffKeyspace without a live API. The
// branch has no keyspaces: reading one by name is not-found, unless lookupErr
// is set, in which case the read fails with it.
type vschemaFetchStubClient struct {
	psclient.PSClient
	vschema   *ps.VSchema
	err       error
	lookupErr error
}

func (c *vschemaFetchStubClient) GetKeyspaceVSchema(_ context.Context, _ *ps.GetKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	return c.vschema, c.err
}

func (c *vschemaFetchStubClient) GetKeyspace(context.Context, *ps.GetKeyspaceRequest) (*ps.Keyspace, error) {
	if c.lookupErr != nil {
		return nil, c.lookupErr
	}
	return nil, &ps.Error{Code: ps.ErrNotFound}
}

// A NotFound on the VSchema read of a keyspace the branch does not list yet
// means the keyspace has no VSchema: the desired document is its first, so it
// surfaces as a change instead of failing the plan. A keyspace read that fails
// cannot confirm the absence, so it fails the diff, and so does any other
// VSchema fetch error.
func TestDiffKeyspace_VSchemaFetchErrors(t *testing.T) {
	shortenVSchemaRereadInterval(t)
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

	t.Run("NotFound for a keyspace the branch does not have is an empty current VSchema", func(t *testing.T) {
		client := &vschemaFetchStubClient{err: &ps.Error{Code: ps.ErrNotFound}}
		diff, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.NoError(t, err)
		assert.Empty(t, diff.tableChanges)
		assert.True(t, diff.vschemaChanged, "desired VSchema must surface as a change against a missing one")
		assert.Empty(t, diff.currentVSchemaRaw)
	})

	t.Run("NotFound whose keyspace read fails fails the diff", func(t *testing.T) {
		client := &vschemaFetchStubClient{err: &ps.Error{Code: ps.ErrNotFound}, lookupErr: &ps.Error{Code: ps.ErrInternal}}
		_, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.ErrorContains(t, err, "read keyspace myapp on branch schemabot-mydb-abc of database mydb to confirm it has no VSchema")
	})

	t.Run("non-NotFound API error fails the diff", func(t *testing.T) {
		client := &vschemaFetchStubClient{err: &ps.Error{Code: ps.ErrInternal}}
		_, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.ErrorContains(t, err, "fetch VSchema for keyspace myapp")
	})

	t.Run("non-API error fails the diff", func(t *testing.T) {
		client := &vschemaFetchStubClient{err: errors.New("dial tcp: connection refused")}
		_, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.ErrorContains(t, err, "fetch VSchema for keyspace myapp")
	})

	t.Run("matching VSchema reports no change", func(t *testing.T) {
		client := &vschemaFetchStubClient{vschema: &ps.VSchema{Raw: desiredVSchema}}
		diff, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", "myapp", desired, currentSchema, rereadMissingVSchema)
		require.NoError(t, err)
		assert.Empty(t, diff.tableChanges)
		assert.False(t, diff.vschemaChanged)
		assert.Equal(t, desiredVSchema, diff.currentVSchemaRaw)
	})
}

// laggingVSchemaClient answers the first notFoundReads VSchema reads with
// not-found and every later one with vschema, the way the API answers while it
// converges after a recent VSchema write. reads counts every read. The branch
// has the keyspaces in existing; lookups counts every keyspace read by name.
// Its keyspace listing returns only listed, so a test can model a listing
// whose pages shifted and left out a keyspace that exists.
type laggingVSchemaClient struct {
	psclient.PSClient
	notFoundReads int
	vschema       *ps.VSchema
	existing      []string
	listed        []string
	reads         int
	lookups       int
}

func (c *laggingVSchemaClient) GetKeyspace(_ context.Context, req *ps.GetKeyspaceRequest) (*ps.Keyspace, error) {
	c.lookups++
	if slices.Contains(c.existing, req.Keyspace) {
		return &ps.Keyspace{Name: req.Keyspace}, nil
	}
	return nil, &ps.Error{Code: ps.ErrNotFound}
}

func (c *laggingVSchemaClient) ListKeyspaces(context.Context, *ps.ListKeyspacesRequest) ([]*ps.Keyspace, error) {
	keyspaces := make([]*ps.Keyspace, 0, len(c.listed))
	for _, name := range c.listed {
		keyspaces = append(keyspaces, &ps.Keyspace{Name: name})
	}
	return keyspaces, nil
}

func (c *laggingVSchemaClient) GetKeyspaceVSchema(context.Context, *ps.GetKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	c.reads++
	if c.reads <= c.notFoundReads {
		return nil, &ps.Error{Code: ps.ErrNotFound}
	}
	return c.vschema, nil
}

// shortenVSchemaRereadInterval makes a test's VSchema re-reads back to back,
// so a test that exercises the bounded re-read runs without the wait.
func shortenVSchemaRereadInterval(t *testing.T) {
	t.Helper()
	origInterval := vschemaAbsenceRereadInterval
	vschemaAbsenceRereadInterval = time.Millisecond
	t.Cleanup(func() { vschemaAbsenceRereadInterval = origInterval })
}

// A keyspace's VSchema has a lookup vindex, and the API briefly reports it
// missing after a recent write. Taken at its word, the desired document would
// be the keyspace's first and the vindex removal it makes would go unflagged.
// A diff that acts on its answer reads again a bounded number of times for a
// keyspace the branch has, so it compares against the VSchema that exists,
// and refuses once the re-reads run out rather than planning against an
// absence nothing confirms. A keyspace the branch does not have has no VSchema
// to lag, so its absence is believed at once. A diff whose caller retries on a
// VSchema difference believes the first not-found, so the two waits do not
// stack.
func TestDiffKeyspace_VSchemaReportedMissingIsReadAgain(t *testing.T) {
	shortenVSchemaRereadInterval(t)

	e := &Engine{linter: lint.New(), logger: slog.New(slog.DiscardHandler)}
	live := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}, "users_email": {"type": "consistent_lookup_unique"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	desired := &schema.Namespace{Files: map[string]string{
		"vschema.json": `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`,
	}}
	// The keyspace has no physical tables yet; its VSchema still exists to be
	// lagging, so the diff cannot use the table list to decide whether to read
	// again.
	noTables := map[string][]table.TableSchema{"commerce": nil}

	t.Run("a lagging read finds the VSchema", func(t *testing.T) {
		client := &laggingVSchemaClient{notFoundReads: 2, vschema: &ps.VSchema{Raw: live}, existing: []string{"billing", "commerce"}}
		diff, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "main", "commerce", desired, noTables, rereadMissingVSchema)
		require.NoError(t, err)
		assert.Equal(t, 3, client.reads)
		assert.Equal(t, live, diff.currentVSchemaRaw, "the plan compares against the VSchema that exists")
	})

	t.Run("a VSchema that stays missing on a keyspace the branch has refuses the plan after the re-reads", func(t *testing.T) {
		client := &laggingVSchemaClient{notFoundReads: 100, existing: []string{"commerce"}}
		diff, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "main", "commerce", desired, noTables, rereadMissingVSchema)
		var refusal *unconfirmedVSchemaAbsenceError
		require.ErrorAs(t, err, &refusal)
		assert.Equal(t, 1+vschemaAbsenceRereads, client.reads)
		assert.Equal(t, 1, client.lookups)
		assert.Equal(t, "PlanetScale has keyspace commerce on branch main of database mydb but reported no VSchema for it on 4 reads, "+
			"so the plan cannot tell an absent VSchema from one the API has not caught up on. "+
			"Retry once PlanetScale shows the keyspace's VSchema. "+
			"If the keyspace has never had one, set an empty VSchema ({}) on it in PlanetScale, then retry", err.Error())
		assert.Equal(t, keyspaceDiff{}, diff)
	})

	t.Run("a keyspace the branch does not have plans its first VSchema without re-reading it", func(t *testing.T) {
		client := &laggingVSchemaClient{notFoundReads: 100, existing: []string{"billing"}}
		diff, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "main", "commerce", desired, noTables, rereadMissingVSchema)
		require.NoError(t, err)
		assert.Equal(t, 1, client.reads)
		assert.Equal(t, 1+keyspaceAbsenceRereads, client.lookups, "the keyspace's absence is read twice before it is believed")
		assert.Empty(t, diff.currentVSchemaRaw)
		assert.True(t, diff.vschemaChanged, "the desired document is the keyspace's first")
	})

	t.Run("a caller that retries on a VSchema difference believes the first not-found", func(t *testing.T) {
		client := &laggingVSchemaClient{notFoundReads: 2, vschema: &ps.VSchema{Raw: live}}
		diff, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "main", "commerce", desired, noTables, acceptMissingVSchema)
		require.NoError(t, err)
		assert.Equal(t, 1, client.reads, "the caller's own retry reads again")
		assert.Equal(t, 0, client.lookups, "the caller's retry needs no confirmation of the absence")
		assert.Empty(t, diff.currentVSchemaRaw)
		assert.True(t, diff.vschemaChanged, "the absence surfaces as the difference the caller retries on")
	})

	t.Run("a keyspace a shifted listing leaves out is still read by name and refuses the plan", func(t *testing.T) {
		// Another keyspace was deleted between the listing's pages, so the
		// listing omits commerce although the branch has it.
		client := &laggingVSchemaClient{notFoundReads: 100, existing: []string{"billing", "commerce"}, listed: []string{"billing"}}
		_, err := e.diffKeyspace(t.Context(), client, "org", "mydb", "main", "commerce", desired, noTables, rereadMissingVSchema)
		var refusal *unconfirmedVSchemaAbsenceError
		require.ErrorAs(t, err, &refusal, "a keyspace that exists is never taken as having no VSchema")
		assert.Equal(t, 1+vschemaAbsenceRereads, client.reads)
		assert.Equal(t, 1, client.lookups)
	})
}

// A plan's answer is acted on as read: the removal gate compares the desired
// VSchema against the current one, and a lagging not-found would leave it
// nothing to compare against. The plan reads again, so a vindex the desired
// document drops is recorded as a deletion for the reviewer to consent to.
func TestPlanKeyspace_LaggingVSchemaKeepsRemovalGate(t *testing.T) {
	shortenVSchemaRereadInterval(t)

	e := &Engine{linter: lint.New(), logger: slog.New(slog.DiscardHandler)}
	live := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}, "users_email": {"type": "consistent_lookup_unique"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	desired := &schema.Namespace{Files: map[string]string{
		"vschema.json": `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`,
	}}
	client := &laggingVSchemaClient{notFoundReads: 2, vschema: &ps.VSchema{Raw: live}, existing: []string{"commerce"}}

	res, err := e.planKeyspace(t.Context(), client, "org", "mydb", "main", "commerce", desired, map[string][]table.TableSchema{"commerce": nil})
	require.NoError(t, err)
	assert.Equal(t, 3, client.reads)
	deletions, err := apitypes.ParseVSchemaDeletions(res.change.Metadata)
	require.NoError(t, err)
	require.Len(t, deletions, 1, "the vindex removal must be recorded for consent")
	assert.Equal(t, "vindex", deletions[0].Kind)
	assert.Equal(t, "users_email", deletions[0].Name)
	assert.Equal(t, live, res.change.OriginalFiles["vschema.json"], "the pre-image is the VSchema that exists")
}

// A keyspace the branch has whose VSchema the API keeps reporting missing is
// not planned: a plan against the absence would drop the vindex removal from
// the consent gate and record an empty VSchema as the rollback pre-image, which
// a rollback would write over the keyspace's real VSchema.
func TestPlanKeyspace_UnconfirmedVSchemaAbsenceRefusesThePlan(t *testing.T) {
	shortenVSchemaRereadInterval(t)

	e := &Engine{linter: lint.New(), logger: slog.New(slog.DiscardHandler)}
	desired := &schema.Namespace{Files: map[string]string{
		"vschema.json": `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`,
	}}
	client := &laggingVSchemaClient{notFoundReads: 100, existing: []string{"commerce"}}

	res, err := e.planKeyspace(t.Context(), client, "org", "mydb", "main", "commerce", desired, map[string][]table.TableSchema{"commerce": nil})
	var refusal *unconfirmedVSchemaAbsenceError
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, "commerce", refusal.keyspace)
	assert.Nil(t, res, "no plan, and so no empty rollback pre-image, is produced")
}

// keyspaceLagClient is a laggingVSchemaClient whose keyspace read by name
// answers not-found for the first keyspaceNotFoundReads reads and finds the
// keyspace after that, the way the API answers while it catches up on a
// keyspace created moments before.
type keyspaceLagClient struct {
	laggingVSchemaClient
	keyspaceNotFoundReads int
}

func (c *keyspaceLagClient) GetKeyspace(_ context.Context, req *ps.GetKeyspaceRequest) (*ps.Keyspace, error) {
	c.lookups++
	if c.lookups <= c.keyspaceNotFoundReads {
		return nil, &ps.Error{Code: ps.ErrNotFound}
	}
	return &ps.Keyspace{Name: req.Keyspace}, nil
}

// A keyspace was created moments before the plan and a VSchema with a lookup
// vindex already written to it, and the API still answers not-found for both
// on the first read. Believing the keyspace's absence would make the desired
// document its first: the vindex removal would go ungated and an empty
// VSchema would be the rollback pre-image. The keyspace read is repeated once,
// so the plan finds the keyspace, re-reads its VSchema, and compares against
// the VSchema that exists.
func TestPlanKeyspace_KeyspaceReportedMissingOnceIsReadAgain(t *testing.T) {
	shortenVSchemaRereadInterval(t)

	e := &Engine{linter: lint.New(), logger: slog.New(slog.DiscardHandler)}
	live := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}, "customers_email_lookup": {"type": "lookup_unique"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	desired := &schema.Namespace{Files: map[string]string{
		"vschema.json": `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`,
	}}
	client := &keyspaceLagClient{keyspaceNotFoundReads: 1}
	client.vschema = &ps.VSchema{Raw: live}
	client.notFoundReads = 1

	res, err := e.planKeyspace(t.Context(), client, "org", "mydb", "main", "commerce", desired, map[string][]table.TableSchema{"commerce": nil})
	require.NoError(t, err)
	assert.Equal(t, 2, client.lookups, "the keyspace's not-found is read again")
	assert.Equal(t, 2, client.reads, "the VSchema is read again once the keyspace is found")
	assert.Equal(t, live, res.change.OriginalFiles["vschema.json"], "the pre-image is the VSchema that exists")
	deletions, err := apitypes.ParseVSchemaDeletions(res.change.Metadata)
	require.NoError(t, err)
	require.Len(t, deletions, 1, "the lookup vindex removal stays gated")
	assert.Equal(t, "vindex", deletions[0].Kind)
	assert.Equal(t, "customers_email_lookup", deletions[0].Name)
}

// Post-apply validation runs inside a retry loop that reads the branch again
// whenever the VSchema differs, under a documented overall bound. Each
// validation therefore reads the VSchema once and takes a not-found as the
// difference that loop retries on: re-reading inside the validation as well
// would nest one wait inside the other and overrun the bound.
func TestVerifyBranchMatchesDesired_ReadsVSchemaOncePerValidation(t *testing.T) {
	shortenVSchemaRereadInterval(t)

	desiredVSchema := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}}`
	e := &Engine{
		linter: lint.New(),
		logger: slog.New(slog.DiscardHandler),
		validationBranchSchema: func(context.Context, *ps.DatabaseBranchPassword, []string) (map[string][]table.TableSchema, error) {
			return map[string][]table.TableSchema{"commerce": nil}, nil
		},
	}
	client := &laggingVSchemaClient{notFoundReads: 1, vschema: &ps.VSchema{Raw: desiredVSchema}, existing: []string{"commerce"}}
	files := schema.SchemaFiles{"commerce": &schema.Namespace{Files: map[string]string{"vschema.json": desiredVSchema}}}
	validate := func() error {
		return e.verifyBranchMatchesDesired(t.Context(), client, "org", "mydb", "schemabot-mydb-abc", []string{"commerce"}, files, engine.IgnoredTables{}, &ps.DatabaseBranchPassword{})
	}

	err := validate()
	require.ErrorContains(t, err, "keyspace commerce has unexpected VSchema difference after apply")
	assert.Equal(t, 1, client.reads, "one VSchema read per validation")
	assert.Equal(t, 0, client.lookups)

	require.NoError(t, validate(), "the next validation reads the converged VSchema")
	assert.Equal(t, 2, client.reads)
}

// A cancelled context ends the wait between VSchema re-reads with the
// cancellation, rather than reading again or believing the absence.
func TestFetchKeyspaceVSchema_CancelledWhileWaitingToReread(t *testing.T) {
	origInterval := vschemaAbsenceRereadInterval
	vschemaAbsenceRereadInterval = time.Hour
	t.Cleanup(func() { vschemaAbsenceRereadInterval = origInterval })

	e := &Engine{logger: slog.New(slog.DiscardHandler)}
	client := &laggingVSchemaClient{notFoundReads: 100, existing: []string{"commerce"}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	vs, err := e.fetchKeyspaceVSchema(ctx, client, "org", "mydb", "main", "commerce", rereadMissingVSchema)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, vs)
	assert.Equal(t, 1, client.reads, "cancellation does not read again")
}

// A cancelled context likewise ends the wait between keyspace reads with the
// cancellation, rather than reading again or believing the keyspace absent.
func TestFetchKeyspaceVSchema_CancelledWhileWaitingToRereadKeyspace(t *testing.T) {
	origInterval := vschemaAbsenceRereadInterval
	vschemaAbsenceRereadInterval = time.Hour
	t.Cleanup(func() { vschemaAbsenceRereadInterval = origInterval })

	e := &Engine{logger: slog.New(slog.DiscardHandler)}
	client := &laggingVSchemaClient{notFoundReads: 100}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	vs, err := e.fetchKeyspaceVSchema(ctx, client, "org", "mydb", "main", "commerce", rereadMissingVSchema)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "re-read keyspace commerce on branch main of database mydb")
	assert.Nil(t, vs)
	assert.Equal(t, 1, client.lookups, "cancellation does not read the keyspace again")
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
