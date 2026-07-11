package templates

import (
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
			IsMySQL: true,
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

	t.Run("appends vschema updates for non-MySQL", func(t *testing.T) {
		data := PlanCommentData{
			IsMySQL: false,
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
			IsMySQL: false,
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
