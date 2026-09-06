package tern

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// alterUsersEmailShardPlan is the recomputed plan for a sharded engine: the same
// reviewed ALTER on a single named shard, used by the shard-scoped drift tests.
func alterUsersEmailShardPlan(shard string) *engine.PlanResult {
	return &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			Shard:     engine.Shard{Name: shard},
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: ddl.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			}},
		}},
	}
}

// A non-primary deployment whose recomputed local plan exactly matches the
// reviewed DDL materializes the plan: there is no drift to block.
func TestDriftGuard_MatchMaterializes(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 5}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_ok",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(5), got.ID)
}

// Whitespace and quoting differences between the recomputed DDL and the reviewed
// DDL are normalized away by canonicalization, so they are not drift.
func TestDriftGuard_CanonicalizationTolerant(t *testing.T) {
	recomputed := &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace: "testapp",
		TableChanges: []engine.TableChange{{
			Table:     "users",
			Operation: ddl.StatementAlterTable,
			DDL:       "ALTER TABLE users ADD COLUMN email varchar(255)",
		}},
	}}}
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 6}
	c := newPlanMaterializeClientWithPlan(store, recomputed)

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_canon",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.NoError(t, err)
}

// A reviewed change this deployment would not plan (local schema already has the
// column) fails closed rather than replaying unreviewed DDL.
func TestDriftGuard_MissingReviewedChangeFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, &engine.PlanResult{}) // recomputes no changes

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_drift",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted")
	assert.Contains(t, err.Error(), "re-plan against the current live schema", "the error must tell a blocked operator how to recover")
	assert.Nil(t, store.created, "must not materialize a drifted plan")
}

// A change this deployment would plan that was never reviewed (local schema is
// behind the desired files in a way the primary did not see) fails closed.
func TestDriftGuard_UnexpectedLocalChangeFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_extra",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "orders", Ddl: "CREATE TABLE `orders` (`id` bigint)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted")
}

// Different DDL for the same table/operation is drift even though the
// namespace/table/action triple matches.
func TestDriftGuard_DifferentDDLSameTableFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_diff_ddl",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `phone` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted")
}

// A shard-scoped apply is drift-checked against this deployment's re-plan
// restricted to the dispatch's shard. When the reviewed DDL matches that shard's
// recomputed change, the plan materializes.
func TestDriftGuard_ShardScopedMatchMaterializes(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 8}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailShardPlan("-80"))

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:       "plan_shard_ok",
		TargetShards: []string{"-80"},
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(8), got.ID)
}

// A shard-scoped apply whose reviewed DDL targets one shard but whose live
// re-plan only needs the change on a different shard fails closed: the targeted
// shard has drifted from the reviewed plan.
func TestDriftGuard_ShardScopedWrongShardFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailShardPlan("80-"))

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:       "plan_shard_drift",
		TargetShards: []string{"-80"},
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted")
	assert.Nil(t, store.created, "must not materialize a drifted shard plan")
}

// More than one target shard is a malformed dispatch (the per-shard fan-out
// emits one shard per operation), so the guard fails closed.
func TestDriftGuard_MultipleTargetShardsFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailShardPlan("-80"))

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:       "plan_shard_multi",
		TargetShards: []string{"-80", "80-"},
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard")
}

// A vschema change the reviewed plan carries but this deployment would not plan
// is drift, even when the table DDL matches exactly.
func TestDriftGuard_VSchemaParityFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	// Recomputed plan has the table change but no vschema change.
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_vschema",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
			{TableName: "VSchema: testapp", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "vschema")
}

// A matching vschema change on both sides is not drift.
func TestDriftGuard_VSchemaParityMatches(t *testing.T) {
	recomputed := &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace: "testapp",
		Metadata:  map[string]string{"vschema_changed": "true"},
	}}}
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 9}
	c := newPlanMaterializeClientWithPlan(store, recomputed)

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_vschema_ok",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "VSchema: testapp", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, Namespace: "testapp"},
		},
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testapp": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
		},
	})

	require.NoError(t, err)
}

// An engine failure during recompute surfaces as an error: the guard never
// fails open when it cannot recompute.
func TestDriftGuard_RecomputeErrorFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClient(store)
	c.config.TargetDSN = "user:pass@tcp(127.0.0.1:3306)/testapp"
	c.spiritEngine = fakePlanEngine{
		planFn: func(ctx context.Context, _ *engine.PlanRequest) (*engine.PlanResult, error) {
			return nil, errors.New("engine boom")
		},
	}

	_, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId: "plan_engine_err",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "recompute local plan")
}

// driftParserForDialect resolves the statement parser drift tests classify and
// canonicalize DDL with, failing the test if the dialect is unregistered.
func driftParserForDialect(t *testing.T, dialect schema.Dialect) ddl.StatementParser {
	t.Helper()
	parser, err := ddl.ParserForDialect(dialect)
	require.NoError(t, err)
	return parser
}

// canonicalDDLForDrift must fail closed on DDL it cannot parse: the parser's
// Canonicalize returns its input unchanged on a parse failure, so without this
// guard an unparseable statement would silently compare by raw text and could
// mask drift.
func TestCanonicalDDLForDrift_FailsClosed(t *testing.T) {
	parser := driftParserForDialect(t, schema.DialectMySQL)

	t.Run("unparseable DDL is rejected", func(t *testing.T) {
		_, err := canonicalDDLForDrift(parser, "this is not valid sql")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DDL rejected by the statement parser")
	})

	t.Run("empty DDL is rejected", func(t *testing.T) {
		_, err := canonicalDDLForDrift(parser, "   ")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty DDL")
	})

	t.Run("multi-statement DDL is rejected", func(t *testing.T) {
		// The parser rejects multi-statement input, so a destructive trailing
		// statement cannot hide behind the classification of the first one and
		// mask drift. It must fail closed instead.
		_, err := canonicalDDLForDrift(parser, "ALTER TABLE `users` ADD COLUMN `email` varchar(255); ALTER TABLE `users` ADD COLUMN `phone` varchar(255)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsed as 2 statements")
	})

	t.Run("DML is rejected", func(t *testing.T) {
		// DML has no place in a schema change drift comparison. It must fail
		// closed instead of canonicalizing it as if it were DDL, and the error
		// names the remedy's cause: the statement should not be in the change.
		_, err := canonicalDDLForDrift(parser, "INSERT INTO `users` (`id`) VALUES (1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected a DDL statement")
	})

	t.Run("a statement outside the shared vocabulary is rejected", func(t *testing.T) {
		// A statement that parses cleanly but classifies to no shared
		// StatementType is unverifiable rather than malformed, so the error
		// distinguishes it from the DML rejection.
		_, err := canonicalDDLForDrift(parser, "SELECT 1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "outside the shared DDL vocabulary")
	})

	t.Run("parseable DDL is canonicalized", func(t *testing.T) {
		// Whitespace and unquoted identifiers normalize to the same canonical form
		// regardless of incidental formatting, so equivalent DDL compares equal.
		spaced, err := canonicalDDLForDrift(parser, "ALTER TABLE   users   ADD COLUMN email varchar(255)")
		require.NoError(t, err)
		quoted, err := canonicalDDLForDrift(parser, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		require.NoError(t, err)
		assert.Equal(t, quoted, spaced)
		assert.NotEmpty(t, spaced)
	})
}

// PostgreSQL DDL must be classified and canonicalized with the PostgreSQL
// grammar: constructs like uuid/timestamptz columns, IDENTITY, and CREATE
// INDEX CONCURRENTLY are not MySQL and would fail the MySQL parser, blocking
// every PostgreSQL apply behind a false drift error.
func TestCanonicalDDLForDrift_PostgresDialect(t *testing.T) {
	parser := driftParserForDialect(t, schema.DialectPostgres)

	t.Run("CREATE TABLE with PostgreSQL types is canonicalized", func(t *testing.T) {
		spaced, err := canonicalDDLForDrift(parser, `CREATE TABLE  users (
			id uuid PRIMARY KEY,
			seq bigint GENERATED ALWAYS AS IDENTITY,
			created_at timestamptz NOT NULL DEFAULT now()
		)`)
		require.NoError(t, err)
		compact, err := canonicalDDLForDrift(parser, "CREATE TABLE users (id uuid PRIMARY KEY, seq bigint GENERATED ALWAYS AS IDENTITY, created_at timestamptz NOT NULL DEFAULT now())")
		require.NoError(t, err)
		assert.Equal(t, compact, spaced, "formatting-only differences canonicalize to the same form")
	})

	t.Run("greenfield create set is canonicalized statement by statement", func(t *testing.T) {
		spaced, err := canonicalDDLForDrift(parser, `CREATE TABLE  users (id bigint, email text);
			CREATE INDEX users_email_idx ON users (email);
			CREATE UNIQUE INDEX users_id_idx ON users (id);`)
		require.NoError(t, err)
		compact, err := canonicalDDLForDrift(parser, "create table users(id bigint,email text); create index users_email_idx on users using btree(email); create unique index users_id_idx on users using btree(id)")
		require.NoError(t, err)

		assert.Equal(t, compact, spaced)
		assert.Equal(t, strings.Join([]string{
			parser.Canonicalize("CREATE TABLE users (id bigint, email text)"),
			parser.Canonicalize("CREATE INDEX users_email_idx ON users (email)"),
			parser.Canonicalize("CREATE UNIQUE INDEX users_id_idx ON users (id)"),
		}, ";\n"), spaced)
	})

	t.Run("greenfield create set rejects ALTER TABLE", func(t *testing.T) {
		_, err := canonicalDDLForDrift(parser, "CREATE TABLE users (id bigint); ALTER TABLE users ADD COLUMN email text")
		require.Error(t, err)
		assert.ErrorContains(t, err, "statement 2 is ALTER TABLE")
	})

	t.Run("CREATE INDEX CONCURRENTLY is canonicalized", func(t *testing.T) {
		// Two spellings that differ only in keyword case and the implicit
		// btree access method canonicalize to the same form, proving the
		// statement round-trips through the parser rather than passing
		// through as raw text.
		implicit, err := canonicalDDLForDrift(parser, "CREATE INDEX CONCURRENTLY idx_users_email ON users (email)")
		require.NoError(t, err)
		explicit, err := canonicalDDLForDrift(parser, "create index concurrently idx_users_email on users using btree (email)")
		require.NoError(t, err)
		assert.Equal(t, explicit, implicit)
		assert.Contains(t, implicit, "CONCURRENTLY")
	})

	t.Run("ALTER TABLE is canonicalized", func(t *testing.T) {
		spaced, err := canonicalDDLForDrift(parser, "ALTER TABLE   users   ADD COLUMN email varchar(255)")
		require.NoError(t, err)
		quoted, err := canonicalDDLForDrift(parser, `ALTER TABLE "users" ADD COLUMN "email" varchar(255)`)
		require.NoError(t, err)
		assert.Equal(t, quoted, spaced)
	})

	t.Run("MySQL-quoted DDL is rejected by the PostgreSQL grammar", func(t *testing.T) {
		_, err := canonicalDDLForDrift(parser, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DDL rejected by the statement parser")
	})

	t.Run("DML is rejected", func(t *testing.T) {
		_, err := canonicalDDLForDrift(parser, "INSERT INTO users (id) VALUES (1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected a DDL statement")
	})

	t.Run("a statement outside the shared vocabulary is rejected", func(t *testing.T) {
		// PostgreSQL classifies statement kinds the shared vocabulary has no
		// name for (SELECT among them) as unknown; those are unverifiable
		// rather than malformed, and the error says so.
		_, err := canonicalDDLForDrift(parser, "SELECT 1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "outside the shared DDL vocabulary")
	})
}

// A deployment whose database type maps to no registered dialect has no grammar
// to classify its DDL with, so the drift guard must fail closed rather than
// judge it by another family's parser.
func TestStatementParser_UnregisteredDialectFailsClosed(t *testing.T) {
	c := &LocalClient{config: LocalConfig{Type: "oracle"}}
	_, err := c.statementParser()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `database type "oracle"`)
}

// The drift guard selects its grammar from the client's configured database
// type: a PostgreSQL deployment's plan is judged by the PostgreSQL parser and
// a MySQL deployment's by the MySQL parser. The same statement proves the
// selection both ways — CREATE INDEX CONCURRENTLY is valid PostgreSQL that the
// MySQL grammar cannot parse.
func TestDriftMultisetFromPlanResult_SelectsParserByDatabaseType(t *testing.T) {
	planResult := func() *engine.PlanResult {
		return &engine.PlanResult{Changes: []engine.SchemaChange{{
			Namespace: "public",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: ddl.StatementCreateIndex,
				DDL:       "CREATE INDEX CONCURRENTLY idx_users_email ON users (email)",
			}},
		}}}
	}

	pg := &LocalClient{config: LocalConfig{Type: storage.DatabaseTypePostgres}}
	ms, err := pg.driftMultisetFromPlanResult(planResult(), false, "")
	require.NoError(t, err)
	require.Len(t, ms, 1)
	for key := range ms {
		assert.Equal(t, "public", key.namespace)
		assert.Equal(t, "users", key.table)
		assert.Contains(t, key.ddl, "CONCURRENTLY")
	}

	my := &LocalClient{config: LocalConfig{Type: storage.DatabaseTypeMySQL}}
	_, err = my.driftMultisetFromPlanResult(planResult(), false, "")
	require.Error(t, err, "PostgreSQL-only DDL must not parse under a MySQL-typed client")
	assert.Contains(t, err.Error(), "DDL rejected by the statement parser")
}
