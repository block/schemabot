package templates

import (
	"fmt"
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
			CanMergeValues: true, UniqueIndexes: []string{"uk_slug", "uk_owner_slug"},
		},
	)))

	assert.Equal(t, "🔤 **Collation changes**: these columns sort and compare under a new collation after the apply.\n"+
		"- `body` on `notes`: `utf8mb4_general_ci` → `utf8mb4_bin`\n"+
		"  - Comparisons become case-sensitive: `'abc'` and `'ABC'` stop comparing equal.\n"+
		"- `slug` on `notes`: `utf8mb4_bin` → `utf8mb4_0900_ai_ci`\n"+
		"  - Comparisons become case-insensitive: `'abc'` and `'ABC'` start comparing equal.\n"+
		"  - Trailing spaces become significant (NO PAD): `'abc'` and `'abc '` stop comparing equal.\n"+
		"  - Values that differ only by accents or other characters can start comparing equal.\n"+
		"  - `slug` is in unique indexes `uk_slug`, `uk_owner_slug`: the apply fails if two existing rows collide in one of them under the new collation.\n",
		collationSection(t, out))
}

// A move that can merge values says so whatever the case and trailing space
// lines say, since collations also weigh accents and other characters
// differently. That is what tells a reviewer a column outside any unique
// index still changes: under the new collation `title` matches, groups, and
// deduplicates values it kept apart before.
func TestRenderPlanComment_CollationChangeCanMergeValues(t *testing.T) {
	out := RenderPlanComment(collationPlanData(collationKeyspace("testapp", CollationChangeData{
		Table: "products", Column: "title", From: "utf8mb4_general_ci", To: "utf8mb4_0900_ai_ci",
		Case: engine.ComparisonUnchanged, TrailingSpaces: engine.ComparisonBecomesSensitive,
		CanMergeValues: true,
	})))

	assert.Equal(t, "🔤 **Collation changes**: these columns sort and compare under a new collation after the apply.\n"+
		"- `title` on `products`: `utf8mb4_general_ci` → `utf8mb4_0900_ai_ci`\n"+
		"  - Trailing spaces become significant (NO PAD): `'abc'` and `'abc '` stop comparing equal.\n"+
		"  - Values that differ only by accents or other characters can start comparing equal.\n",
		collationSection(t, out))
}

// A move that changes neither letter case nor trailing spaces still says what
// it does to other values, so a quiet line never reads as no change at all.
func TestRenderPlanComment_CollationChangeKeepsCaseAndSpaces(t *testing.T) {
	merges := RenderPlanComment(collationPlanData(collationKeyspace("testapp", CollationChangeData{
		Table: "notes", Column: "body", From: "utf8mb4_general_ci", To: "utf8mb4_unicode_ci",
		Case: engine.ComparisonUnchanged, TrailingSpaces: engine.ComparisonUnchanged,
		CanMergeValues: true,
	})))
	assert.Equal(t, "- `body` on `notes`: `utf8mb4_general_ci` → `utf8mb4_unicode_ci`\n"+
		"  - Values that differ only by accents or other characters can start comparing equal.\n",
		strings.SplitN(collationSection(t, merges), "\n", 2)[1])

	keeps := RenderPlanComment(collationPlanData(collationKeyspace("testapp", CollationChangeData{
		Table: "notes", Column: "body", From: "utf8mb4_0900_as_cs", To: "utf8mb4_0900_bin",
		Case: engine.ComparisonUnchanged, TrailingSpaces: engine.ComparisonUnchanged,
	})))
	assert.Equal(t, "- `body` on `notes`: `utf8mb4_0900_as_cs` → `utf8mb4_0900_bin`\n"+
		"  - Letter case and trailing spaces compare as before, and no values that compare unequal now start comparing equal.\n",
		strings.SplitN(collationSection(t, keeps), "\n", 2)[1])
}

// What the plan cannot read is a warning, never silence: a collation left to
// the server default, and properties of a collation the plan does not know.
func TestRenderPlanComment_CollationChangeUnknown(t *testing.T) {
	out := RenderPlanComment(collationPlanData(collationKeyspace("testapp",
		CollationChangeData{
			Table: "notes", Column: "body", From: "utf8mb4_general_ci",
			Case: engine.ComparisonUnknown, TrailingSpaces: engine.ComparisonUnknown,
			CanMergeValues: true,
		},
		CollationChangeData{
			Table: "notes", Column: "title", From: "utf8mb4_general_ci", To: "utf8mb4_custom_ci",
			Case: engine.ComparisonUnknown, TrailingSpaces: engine.ComparisonUnknown,
			CanMergeValues: true,
		},
	)))

	section := collationSection(t, out)
	assert.Contains(t, section, "- `body` on `notes`: `utf8mb4_general_ci` → the server's default collation\n"+
		"  - The statement leaves the collation to the server's default, which the plan could not read, so it cannot say how values will compare.\n"+
		"- `title`")
	assert.Contains(t, section, "- `title` on `notes`: `utf8mb4_general_ci` → `utf8mb4_custom_ci`\n"+
		"  - The plan cannot read whether letter case is significant under the new collation.\n"+
		"  - The plan cannot read whether trailing spaces are significant under the new collation.\n"+
		"  - Values that differ only by accents or other characters can start comparing equal.\n")
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
			CanMergeValues: true,
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

// A table that converts many unique columns at once lists a bounded number of
// unique index lines and counts the rest, so the cap on the column list is
// not undone by the lines under it.
func TestRenderPlanComment_CollationUniqueLinesAreBounded(t *testing.T) {
	var wide []CollationChangeData
	for i := range collationColumnsShown + 2 {
		column := fmt.Sprintf("c%d", i+1)
		wide = append(wide, CollationChangeData{
			Table: "notes", Column: column, From: "latin1_swedish_ci", To: "utf8mb4_0900_ai_ci",
			Case: engine.ComparisonUnchanged, TrailingSpaces: engine.ComparisonBecomesSensitive,
			CanMergeValues: true, UniqueIndexes: []string{"uk_" + column},
		})
	}
	section := collationSection(t, RenderPlanComment(collationPlanData(collationKeyspace("testapp", wide...))))

	assert.Contains(t, section, "  - `c8` is in unique index `uk_c8`: the apply fails if two existing rows collide in that index under the new collation.\n"+
		"  - …and 2 more columns in unique indexes, where the apply fails if two existing rows collide under the new collation.\n")
	assert.NotContains(t, section, "`c9` is in unique index")
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

func collationMove(table, column string) CollationChangeData {
	return CollationChangeData{
		Table: table, Column: column, From: "utf8mb4_general_ci", To: "utf8mb4_bin",
		Case: engine.ComparisonBecomesSensitive, TrailingSpaces: engine.ComparisonUnchanged,
	}
}

// A plan that moves many tables collapses the section and counts the moves in
// its summary. The moves a unique index covers are listed first, since they
// are the ones that can fail the apply, and past the listing cap the rest are
// counted rather than listed.
func TestRenderPlanComment_CollationChangesCollapse(t *testing.T) {
	var changes []CollationChangeData
	for i := range collationGroupsShown + 3 {
		changes = append(changes, collationMove(fmt.Sprintf("t%02d", i), "body"))
	}
	last := collationMove("t_last", "handle")
	last.To, last.Case, last.TrailingSpaces = "utf8mb4_0900_ai_ci", engine.ComparisonUnchanged, engine.ComparisonBecomesSensitive
	last.CanMergeValues, last.UniqueIndexes = true, []string{"uk_handle"}
	changes = append(changes, last)
	out := RenderPlanComment(collationPlanData(collationKeyspace("testapp", changes...)))

	assert.NotContains(t, out, "🔤 **Collation changes**")
	assert.Contains(t, out, "<details>\n<summary>🔤 <b>Collation changes</b>: 36 columns on 36 tables sort and compare under a new collation after the apply</summary>\n\n"+
		"- `handle` on `t_last`: `utf8mb4_general_ci` → `utf8mb4_0900_ai_ci`\n")
	assert.Contains(t, out, "  - `handle` is in unique index `uk_handle`: the apply fails if two existing rows collide in that index under the new collation.\n")
	assert.Contains(t, out, "- `body` on `t30`: ")
	assert.NotContains(t, out, "- `body` on `t31`: ")
	assert.Contains(t, out, "- …and 4 more columns on 4 tables\n\n</details>\n\n")

	inline := RenderPlanComment(collationPlanData(collationKeyspace("testapp", changes[:collationGroupsInlineLimit]...)))
	assert.Contains(t, inline, "🔤 **Collation changes**: ")
	assert.NotContains(t, inline, "<b>Collation changes</b>")
}

// Every comment that shows a plan shows its collation changes: each
// environment's section of a multi-environment plan, and the plan an apply
// rejection renders above the reason it refuses.
func TestCollationChangesRenderOnEveryPlanComment(t *testing.T) {
	plan := func(environment string) *PlanCommentData {
		data := collationPlanData(collationKeyspace("testapp", collationMove("notes", "body")))
		data.Environment = environment
		return &data
	}
	line := "- `body` on `notes`: `utf8mb4_general_ci` → `utf8mb4_bin`\n"

	staging, production := plan("staging"), plan("production")
	production.Changes[0].CollationChanges[0].Column = "memo"
	multi := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": staging, "production": production},
	})
	assert.Contains(t, multi, line, "staging section")
	assert.Contains(t, multi, "- `memo` on `notes`: `utf8mb4_general_ci` → `utf8mb4_bin`\n", "production section")

	unsafe := plan("staging")
	unsafe.UnsafeChanges = []UnsafeChangeData{{Table: "notes", Reason: "DROP COLUMN"}}
	assert.Contains(t, collationSection(t, RenderUnsafeChangesBlocked(*unsafe)), line, "unsafe changes blocked")

	blocked := plan("staging")
	blocked.BlockedChanges = []BlockedChangeData{{Table: "notes", Reason: "unsupported statement"}}
	assert.Contains(t, collationSection(t, RenderBlockedChangesApplyRejected(*blocked)), line, "apply rejected")
}
