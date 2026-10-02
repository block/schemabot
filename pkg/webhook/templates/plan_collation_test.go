package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// Collation rendering in the plan comment: a section under the table sizes
// names each column the plan moves onto another collation, and says whether
// letter case and trailing spaces stop or start comparing equal. A move the
// plan cannot read is called out, never left out.

func collationPlanData(changes ...KeyspaceChangeData) PlanCommentData {
	return PlanCommentData{
		Database:    "testapp",
		Environment: "staging",
		IsMySQL:     true,
		Changes:     changes,
	}
}

func collationKeyspace(keyspace string, changes ...CollationChangeData) KeyspaceChangeData {
	return KeyspaceChangeData{
		Keyspace:         keyspace,
		Statements:       []string{"ALTER TABLE `notes` MODIFY COLUMN `body` text COLLATE utf8mb4_bin"},
		CollationChanges: changes,
	}
}

func collationSection(t *testing.T, out string) string {
	t.Helper()
	start := strings.Index(out, "🔤 **Collation changes**")
	require.NotEqual(t, -1, start, "plan comment renders the collation section:\n%s", out)
	end := strings.Index(out[start:], "\n\n")
	require.NotEqual(t, -1, end)
	return out[start : start+end+1]
}

func TestRenderPlanComment_CollationChanges(t *testing.T) {
	out := RenderPlanComment(collationPlanData(collationKeyspace("testapp",
		CollationChangeData{
			Table: "notes", Column: "body", From: "utf8mb4_general_ci", To: "utf8mb4_bin",
			Case: engine.ComparisonBecomesSensitive, TrailingSpaces: engine.ComparisonUnchanged,
		},
		CollationChangeData{
			Table: "notes", Column: "slug", From: "utf8mb4_bin", To: "utf8mb4_0900_ai_ci",
			Case: engine.ComparisonBecomesInsensitive, TrailingSpaces: engine.ComparisonBecomesSensitive,
			UniqueIndexes: []string{"uk_slug", "uk_owner_slug"},
		},
	)))

	assert.Equal(t, "🔤 **Collation changes**: these columns sort and compare under a new collation after the apply.\n"+
		"- `body` on `notes`: `utf8mb4_general_ci` → `utf8mb4_bin`\n"+
		"  - ⚠️ Comparisons become case-sensitive: `'abc'` and `'ABC'` stop comparing equal.\n"+
		"- `slug` on `notes`: `utf8mb4_bin` → `utf8mb4_0900_ai_ci`\n"+
		"  - ⚠️ Comparisons become case-insensitive: `'abc'` and `'ABC'` start comparing equal.\n"+
		"  - ⚠️ Trailing spaces become significant (NO PAD): `'abc'` and `'abc '` stop comparing equal.\n"+
		"  - ⚠️ `slug` is in unique indexes `uk_slug`, `uk_owner_slug`: the apply fails if two existing values compare equal under the new collation.\n",
		collationSection(t, out))
}

// A move that changes neither letter case nor trailing spaces still says that
// other characters can compare differently, so a quiet line never reads as no
// change at all.
func TestRenderPlanComment_CollationChangeKeepsCaseAndSpaces(t *testing.T) {
	out := RenderPlanComment(collationPlanData(collationKeyspace("testapp", CollationChangeData{
		Table: "notes", Column: "body", From: "utf8mb4_general_ci", To: "utf8mb4_unicode_ci",
		Case: engine.ComparisonUnchanged, TrailingSpaces: engine.ComparisonUnchanged,
	})))

	assert.Contains(t, collationSection(t, out),
		"  - Letter case and trailing spaces compare as before. Accented and other characters can still sort and compare differently.\n")
}

// What the plan cannot read is a warning, never silence: a collation left to
// the server default, and properties of a collation the plan does not know.
func TestRenderPlanComment_CollationChangeUnknown(t *testing.T) {
	out := RenderPlanComment(collationPlanData(collationKeyspace("testapp",
		CollationChangeData{
			Table: "notes", Column: "body", From: "utf8mb4_general_ci",
			Case: engine.ComparisonUnknown, TrailingSpaces: engine.ComparisonUnknown,
		},
		CollationChangeData{
			Table: "notes", Column: "title", From: "utf8mb4_general_ci", To: "utf8mb4_custom_ci",
			Case: engine.ComparisonUnknown, TrailingSpaces: engine.ComparisonUnknown,
		},
	)))

	section := collationSection(t, out)
	assert.Contains(t, section, "- `body` on `notes`: `utf8mb4_general_ci` → the server's default collation\n"+
		"  - ⚠️ The new collation is not in the plan, so it cannot say how letter case and trailing spaces will compare.\n")
	assert.Contains(t, section, "- `title` on `notes`: `utf8mb4_general_ci` → `utf8mb4_custom_ci`\n"+
		"  - ⚠️ The plan cannot read whether letter case is significant under the new collation.\n"+
		"  - ⚠️ The plan cannot read whether trailing spaces are significant under the new collation.\n")
}

// Columns of one table making the same move share a line, which lists a
// bounded number of them so a converted wide table stays readable; tables
// carry their keyspace when more than one keyspace changes collations.
func TestRenderPlanComment_CollationChangesGroupAndQualify(t *testing.T) {
	var wide []CollationChangeData
	for _, column := range []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"} {
		wide = append(wide, CollationChangeData{
			Table: "notes", Column: column, From: "latin1_swedish_ci", To: "utf8mb4_0900_ai_ci",
			Case: engine.ComparisonUnchanged, TrailingSpaces: engine.ComparisonBecomesSensitive,
		})
	}
	out := RenderPlanComment(collationPlanData(
		collationKeyspace("commerce", wide...),
		collationKeyspace("billing", CollationChangeData{
			Table: "notes", Column: "memo", From: "utf8mb4_general_ci", To: "utf8mb4_bin",
			Case: engine.ComparisonBecomesSensitive, TrailingSpaces: engine.ComparisonUnchanged,
		}),
	))

	section := collationSection(t, out)
	assert.Contains(t, section, "- `c1`, `c2`, `c3`, `c4`, `c5`, `c6`, `c7`, `c8` and 2 more on `commerce.notes`: `latin1_swedish_ci` → `utf8mb4_0900_ai_ci`\n")
	assert.Contains(t, section, "- `memo` on `billing.notes`: `utf8mb4_general_ci` → `utf8mb4_bin`\n")
}

// The section sits under the table sizes, describing the DDL above it, and a
// plan that re-collates nothing renders none.
func TestRenderPlanComment_CollationChangesPlacement(t *testing.T) {
	data := collationPlanData(collationKeyspace("testapp", CollationChangeData{
		Table: "notes", Column: "body", From: "utf8mb4_general_ci", To: "utf8mb4_bin",
		Case: engine.ComparisonBecomesSensitive, TrailingSpaces: engine.ComparisonUnchanged,
	}))
	data.Changes[0].TableSizes = []TableSizeData{{Table: "notes", EstimatedBytes: previewBytes(1_130_000_000)}}
	out := RenderPlanComment(data)

	sizesAt := strings.Index(out, "📊 **Table sizes**")
	collationAt := strings.Index(out, "🔤 **Collation changes**")
	require.NotEqual(t, -1, sizesAt)
	assert.Greater(t, collationAt, sizesAt, "collation section renders under the table sizes")

	data.Changes[0].CollationChanges = nil
	assert.NotContains(t, RenderPlanComment(data), "Collation changes")
}

// A locked comment that applies without confirmation renders no section: the
// operator saw it on the plan they chose to apply. One paused for
// apply-confirm keeps it, since its re-plan can differ from the reviewed one.
func TestRenderPlanComment_CollationChangesOnLockedComment(t *testing.T) {
	data := collationPlanData(collationKeyspace("testapp", CollationChangeData{
		Table: "notes", Column: "body", From: "utf8mb4_general_ci", To: "utf8mb4_bin",
		Case: engine.ComparisonBecomesSensitive, TrailingSpaces: engine.ComparisonUnchanged,
	}))
	data.IsLocked = true
	assert.NotContains(t, RenderPlanComment(data), "Collation changes")

	data.PendingManualConfirmation = true
	assert.Contains(t, RenderPlanComment(data), "🔤 **Collation changes**")
}
