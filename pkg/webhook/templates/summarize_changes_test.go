package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SummarizeChanges powers the aggregate check's Change column. It must never
// return an empty summary for a plan that has changes — an em dash there would
// read as "no changes" on a safety-critical surface — and its create/alter/drop
// and vschema counts must match the plan comment's summary.
func TestSummarizeChanges(t *testing.T) {
	t.Run("counts create, alter, drop", func(t *testing.T) {
		data := PlanCommentData{
			DatabaseType: "mysql",
			IsMySQL:      true,
			Changes: []KeyspaceChangeData{{
				Keyspace: "orders",
				Statements: []string{
					"CREATE TABLE a (id INT)",
					"CREATE TABLE b (id INT)",
					"ALTER TABLE c ADD COLUMN d INT",
					"DROP TABLE e",
				},
			}},
		}
		assert.Equal(t, "2 creates, 1 alter, 1 drop", SummarizeChanges(data))
	})

	t.Run("deduplicates alters by table", func(t *testing.T) {
		data := PlanCommentData{
			Database:     "testapp",
			DatabaseType: "mysql",
			IsMySQL:      true,
			Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{
				"ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY(`id`, `tenant_id`)",
				"ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`)",
				"ALTER TABLE `orders` ADD COLUMN `notes` text",
			}}},
		}
		require.Equal(t, "2 alters", SummarizeChanges(data))
	})

	t.Run("counts rejected statements as other", func(t *testing.T) {
		data := PlanCommentData{Database: "app", DatabaseType: "mysql", IsMySQL: true,
			Changes: []KeyspaceChangeData{{Keyspace: "app", Statements: []string{
				"ALTER TABLE `orders` ADD COLUMN `a` text", "THIS IS NOT SQL AT ALL",
			}}}}
		require.Contains(t, RenderPlanComment(data), "**1** table to alter, 1 other DDL statement")
		require.Equal(t, "1 alter, 1 other DDL statement", SummarizeChanges(data))
	})

	t.Run("counts drop and create separately for one table", func(t *testing.T) {
		data := PlanCommentData{DatabaseType: "mysql", IsMySQL: true, Changes: []KeyspaceChangeData{{
			Keyspace: "app", Statements: []string{"DROP TABLE `orders`", "CREATE TABLE `orders` (`id` int) ENGINE=InnoDB"},
		}}}
		require.Equal(t, "1 create, 1 drop", SummarizeChanges(data))
	})

	t.Run("counts the same table in separate keyspaces", func(t *testing.T) {
		data := PlanCommentData{DatabaseType: "vitess", Changes: []KeyspaceChangeData{
			{Keyspace: "ks1", Statements: []string{"ALTER TABLE `orders` ADD COLUMN `a` text"}},
			{Keyspace: "ks2", Statements: []string{"ALTER TABLE `orders` ADD COLUMN `a` text"}},
		}}
		require.Equal(t, "2 alters", SummarizeChanges(data))
	})

	t.Run("names unbucketed DDL alongside a vschema update", func(t *testing.T) {
		data := PlanCommentData{DatabaseType: "vitess", Changes: []KeyspaceChangeData{{
			Keyspace: "app", VSchemaChanged: true, VSchemaDiff: "+ x",
			Statements: []string{"CREATE VIEW v AS SELECT 1", "TRUNCATE TABLE t"},
		}}}
		require.Contains(t, RenderPlanComment(data), "2 DDL statements, **1** vschema update")
		require.Equal(t, "2 DDL statements · 1 vschema update", SummarizeChanges(data))
	})

	t.Run("counts PostgreSQL DDL under its own grammar", func(t *testing.T) {
		// PostgreSQL column types the MySQL family's parser rejects must still
		// classify, so the summary counts them instead of undercounting the plan.
		data := PlanCommentData{
			DatabaseType: "postgres",
			Changes: []KeyspaceChangeData{{
				Keyspace: "orders",
				Statements: []string{
					"CREATE TABLE a (id uuid, payload jsonb, created_at timestamptz)",
					"ALTER TABLE b ADD COLUMN raw bytea",
				},
			}},
		}
		assert.Equal(t, "1 create, 1 alter", SummarizeChanges(data))
	})

	t.Run("counts a PostgreSQL create set as one create", func(t *testing.T) {
		data := PlanCommentData{
			DatabaseType: "postgres",
			Changes: []KeyspaceChangeData{{
				Keyspace: "orders",
				Statements: []string{
					"CREATE TABLE t (id bigint, v text); CREATE INDEX t_v_idx ON t (v)",
				},
			}},
		}
		counts := countStatementTypes(data.Changes, data.DatabaseType)
		assert.Equal(t, 1, counts.Created)
		assert.Zero(t, counts.Altered)
		assert.Zero(t, counts.Dropped)
		assert.Zero(t, counts.Other)
	})

	t.Run("counts a MySQL create index as an alter of its table", func(t *testing.T) {
		changes := []KeyspaceChangeData{{
			Keyspace:   "orders",
			Statements: []string{"CREATE INDEX `i` ON `t` (`v`)"},
		}}
		counts := countStatementTypes(changes, "mysql")
		assert.Zero(t, counts.Created)
		assert.Equal(t, 1, counts.Altered)
		assert.Zero(t, counts.Dropped)
		assert.Zero(t, counts.Other)
	})

	t.Run("counts a PostgreSQL index on an existing table beside a create set", func(t *testing.T) {
		// A greenfield create set is one table to create however many indexes
		// it carries; a concurrent index build on a table the plan does not
		// otherwise touch is that table to alter, and an ALTER plus an index
		// on one table is still one table to alter.
		data := PlanCommentData{
			DatabaseType: "postgres",
			Changes: []KeyspaceChangeData{{
				Keyspace: "orders",
				Statements: []string{
					`ALTER TABLE "app"."settings" ADD COLUMN file_prefix text`,
					`ALTER TABLE "app"."payments" ALTER COLUMN amount TYPE numeric`,
					`CREATE TABLE "app"."recalls" (id bigint PRIMARY KEY, payment_id bigint); CREATE INDEX recalls_payment_idx ON "app"."recalls" (payment_id)`,
					`CREATE INDEX CONCURRENTLY payments_settled_idx ON "app"."payments" (settled_at)`,
					`CREATE INDEX CONCURRENTLY referrals_referred_idx ON "app"."referrals" (referred_at)`,
				},
			}},
		}
		assert.Equal(t, "1 create, 3 alters", SummarizeChanges(data))
		var summary strings.Builder
		writePlanSummary(&summary, data, 5, 0)
		assert.Equal(t, "📋 **Plan**: **1** table to create, **3** tables to alter\n\n", summary.String())
	})

	tests := []struct {
		name            string
		statements      []string
		wantSummary     string
		wantPlanSummary string
	}{
		{
			name: "mixed table and other statements",
			statements: []string{
				"ALTER TABLE orders ADD COLUMN state text",
				"CREATE TYPE order_state AS ENUM ('new', 'paid')",
				"COMMENT ON TABLE orders IS 'customer orders'",
			},
			wantSummary:     "1 alter, 2 other DDL statements",
			wantPlanSummary: "📋 **Plan**: **1** table to alter, 2 other DDL statements\n\n",
		},
		{
			name:            "one table change and one other statement",
			statements:      []string{"ALTER TABLE orders ADD COLUMN state text", "CREATE TYPE order_state AS ENUM ('new', 'paid')"},
			wantSummary:     "1 alter, 1 other DDL statement",
			wantPlanSummary: "📋 **Plan**: **1** table to alter, 1 other DDL statement\n\n",
		},
		{
			name:            "only unbucketed statements keep the raw total",
			statements:      []string{"CREATE TYPE order_state AS ENUM ('new', 'paid')", "COMMENT ON TABLE orders IS 'customer orders'"},
			wantSummary:     "2 DDL statements",
			wantPlanSummary: "📋 **Plan**: 2 DDL statements\n\n",
		},
		{
			name:            "bucketed statements stay unchanged",
			statements:      []string{"ALTER TABLE orders ADD COLUMN state text"},
			wantSummary:     "1 alter",
			wantPlanSummary: "📋 **Plan**: **1** table to alter\n\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := PlanCommentData{
				DatabaseType: "postgres",
				Changes: []KeyspaceChangeData{{
					Keyspace:   "orders",
					Statements: tt.statements,
				}},
			}

			assert.Equal(t, tt.wantSummary, SummarizeChanges(data))
			var summary strings.Builder
			writePlanSummary(&summary, data, len(tt.statements), 0)
			assert.Equal(t, tt.wantPlanSummary, summary.String())
		})
	}

	t.Run("appends vschema updates for non-MySQL", func(t *testing.T) {
		data := PlanCommentData{
			DatabaseType: "vitess",
			IsMySQL:      false,
			Changes: []KeyspaceChangeData{{
				Keyspace:       "orders",
				Statements:     []string{"CREATE TABLE a (id INT)"},
				VSchemaChanged: true,
			}},
		}
		assert.Equal(t, "1 create · 1 vschema update", SummarizeChanges(data))
	})

	t.Run("vschema-only change for non-MySQL", func(t *testing.T) {
		data := PlanCommentData{
			IsMySQL: false,
			Changes: []KeyspaceChangeData{{Keyspace: "orders", VSchemaChanged: true}},
		}
		assert.Equal(t, "1 vschema update", SummarizeChanges(data))
	})

	t.Run("per-shard-only DDL is classified once per distinct statement", func(t *testing.T) {
		data := PlanCommentData{
			IsMySQL:      false,
			DatabaseType: "vitess",
			Changes: []KeyspaceChangeData{{
				Keyspace: "orders",
				Shards: []KeyspaceShardChange{
					{Shard: "-80", Statements: []string{"ALTER TABLE a ADD COLUMN b INT"}},
					{Shard: "80-", Statements: []string{"ALTER TABLE a ADD COLUMN b INT"}},
				},
			}},
		}
		assert.Equal(t, "1 alter", SummarizeChanges(data))
	})

	t.Run("a statement confined to one shard is counted", func(t *testing.T) {
		// The collapsed namespace-level Statements carry only the change every
		// shard shares; the CREATE INDEX on a second table exists on one drifted
		// shard alone. The comment renders the per-shard DDL, so the summary
		// counts from it and names that table instead of reporting the
		// collapsed view.
		data := PlanCommentData{
			IsMySQL:      false,
			DatabaseType: "vitess",
			Changes: []KeyspaceChangeData{{
				Keyspace:   "orders",
				Statements: []string{"ALTER TABLE a ADD COLUMN b INT"},
				Shards: []KeyspaceShardChange{
					{Shard: "-80", Statements: []string{"ALTER TABLE a ADD COLUMN b INT", "CREATE INDEX `i` ON `c` (`d`)"}},
					{Shard: "80-", Statements: []string{"ALTER TABLE a ADD COLUMN b INT"}},
				},
			}},
		}
		assert.Equal(t, "2 alters", SummarizeChanges(data))
		totalStatements, _ := countChanges(data.Changes)
		assert.Equal(t, 2, totalStatements)
	})

	t.Run("a table whose shards diverge is one table to alter", func(t *testing.T) {
		// Two shard groups render two different ALTER statements against the
		// same table; the summary counts tables, so it reports one alter while
		// the raw statement total still sees both.
		data := PlanCommentData{
			IsMySQL:      false,
			DatabaseType: "vitess",
			Changes: []KeyspaceChangeData{{
				Keyspace:   "orders",
				Statements: []string{"ALTER TABLE a ADD COLUMN b INT"},
				Shards: []KeyspaceShardChange{
					{Shard: "-80", Statements: []string{"ALTER TABLE a ADD COLUMN b INT"}},
					{Shard: "80-", Statements: []string{"ALTER TABLE a ADD COLUMN b INT, ADD COLUMN c INT"}},
				},
			}},
		}
		assert.Equal(t, "1 alter", SummarizeChanges(data))
		totalStatements, _ := countChanges(data.Changes)
		assert.Equal(t, 2, totalStatements)
	})

	t.Run("no changes returns empty", func(t *testing.T) {
		assert.Empty(t, SummarizeChanges(PlanCommentData{IsMySQL: true}))
	})
}
