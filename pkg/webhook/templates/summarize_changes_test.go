package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
		creates, alters, drops, other := countStatementTypes(data.Changes, data.DatabaseType)
		assert.Equal(t, 1, creates)
		assert.Zero(t, alters)
		assert.Zero(t, drops)
		assert.Zero(t, other)
	})

	t.Run("does not count a MySQL create index as a table change", func(t *testing.T) {
		changes := []KeyspaceChangeData{{
			Keyspace:   "orders",
			Statements: []string{"CREATE INDEX `i` ON `t` (`v`)"},
		}}
		creates, alters, drops, other := countStatementTypes(changes, "mysql")
		assert.Zero(t, creates)
		assert.Zero(t, alters)
		assert.Zero(t, drops)
		assert.Equal(t, 1, other)
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

	t.Run("per-shard-only DDL falls back to a statement count", func(t *testing.T) {
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
		// countStatementTypes does not walk per-shard statements, so the
		// create/alter/drop tally is zero; the fallback reports the deduped total
		// rather than implying "no changes".
		assert.Equal(t, "1 DDL statement", SummarizeChanges(data))
	})

	t.Run("no changes returns empty", func(t *testing.T) {
		assert.Empty(t, SummarizeChanges(PlanCommentData{IsMySQL: true}))
	})
}
