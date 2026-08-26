package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
)

// A copy the apply will throw away is disclosed as a warning that names the
// tables, the namespace, how stale the copy is, why it cannot be resumed, and
// the remedy. This is the rendering that reaches an operator who still has the
// decision.
func TestRenderPlanComment_DiscardedCopyWarnsWhileTheDecisionIsTheOperators(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		DiscardedCopies: []ExistingCopyData{
			{
				Namespace: "testapp",
				Tables:    []string{"orders"},
				Reason:    engine.DiscardStatementDiffers,
				Age:       "3h 12m",
				Statement: "ALTER TABLE `orders` ADD INDEX `idx_user_created` (`user_id`, `created_at`)",
			},
		},
	}

	plan := RenderPlanComment(data)
	assert.Contains(t, plan, "⚠️ **Applying destroys work in progress**: **1** unfinished copy on the target\n")
	assert.Contains(t, plan, "- `orders` in `testapp` (last progress 3h 12m ago): the schema change differs from the one that started it, "+
		"which was `ALTER TABLE orders ADD INDEX idx_user_created (user_id, created_at)`",
		"a cause that is a comparison names the side the operator cannot see from the plan above")
	assert.NotContains(t, plan, "3h 12m of copying",
		"the copy's age dates its last checkpoint, so it can never be read as elapsed copying")
	assert.NotContains(t, plan, "3h 12m already spent",
		"a live copy checkpoints continuously, so its age understates what discarding costs")
	assert.Contains(t, plan, "Applying restarts the copy from zero rows.",
		"what confirming costs is the whole copy over again, named as work rather than as a duration")
	assert.Contains(t, plan, "To keep the work already done, apply the schema change that started it.")

	// A locked apply that stopped for confirmation is the same situation: the
	// copy is still there and confirming is what destroys it, so the warning
	// and its remedy belong on the comment the confirmation acts on.
	data.IsLocked = true
	data.AutoConfirmDowngradeReason = "Applying destroys work in progress on the target"
	paused := RenderPlanComment(data)
	assert.Contains(t, paused, "⚠️ **Applying destroys work in progress**")
	assert.Contains(t, paused, "apply the schema change that started it")
}

// An apply that is already running has nothing to ask and no move to offer: the
// copy is dropped as the comment is posted, and neither stopping the apply nor
// restoring the earlier schema change brings it back. So the section states what
// went and stops, under ℹ️ — a reader who cannot act on something is being
// informed, not warned.
func TestRenderPlanComment_DiscardedCopyReadsAsARecordOnceApplying(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, IsLocked: true,
		LockOwner: "block/schemabot#42",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers, Age: "3h 12m"},
		},
	})

	assert.Contains(t, out, "**Applying automatically**",
		"the fixture is the automatic path, which is what makes the section a record")
	assert.Contains(t, out, "ℹ️ **This apply destroys work in progress**: **1** unfinished copy on the target\n")
	assert.Contains(t, out, "- `orders` in `testapp` (last progress 3h 12m ago): the schema change differs from the one that started it")
	assert.NotContains(t, out, "⚠️ **This apply destroys work in progress**",
		"a reader with no move to make is being informed, not warned")
	assert.NotContains(t, out, "**Applying destroys work in progress**",
		"the hypothetical subject belongs to a comment where applying is still a choice")
	assert.NotContains(t, out, "apply the schema change that started it",
		"the remedy is out of reach once the copy is being destroyed")
	assert.NotContains(t, out, "restarts the copy from zero rows",
		"the entries are the whole record; a closing line here could only restate them")
}

// The names in a copy entry are read off a live target and the reason arrives
// from a server that may be a version ahead, so each is rendered through the
// shared inline-code sanitizer. A quoted identifier may legally carry a
// backtick or a newline, and either would end a code span early or split the
// entry across lines in the one section an operator reads to decide whether
// hours of copying are expendable. The sanitizer drops the backtick rather than
// escaping it, so a name that carries one is shown closed up; a readable entry
// that survives is worth more here than byte fidelity for a name no target
// realistically has.
func TestRenderPlanComment_DiscardedCopyKeepsHostileIdentifiersOnOneLine(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{
				Namespace: "app`\ndb",
				Tables:    []string{"ord`ers", "line\nitems"},
				Reason:    "future`reason\nvalue",
				Age:       "3h 12m",
			},
		},
	})

	assert.Contains(t, out, "- `orders`, `line items` in `app db` (last progress 3h 12m ago): `futurereason value`\n",
		"identifiers and an untranslated reason render as one entry on one line")
	assert.NotContains(t, out, "ord`ers", "a backtick in an identifier cannot end the code span early")
	assert.NotContains(t, out, "future`reason", "a backtick in a reason cannot end the code span early")
}

// The statement a copy was started for is read off a live target, so an entry
// has to survive one that is long or multi-line: it is folded onto one line and
// clamped, because a disclosure entry only does its job if it stays scannable
// next to the others in its section.
func TestRenderPlanComment_DiscardedCopyClampsTheStartingStatement(t *testing.T) {
	long := "ALTER TABLE `orders`\n  ADD INDEX `idx_" + strings.Repeat("x", 300) + "` (`user_id`)"

	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers, Statement: long},
		},
	})

	entry := entryLine(t, out, "- `orders` in `testapp`:")
	assert.Contains(t, entry, "which was `ALTER TABLE orders ADD INDEX idx_")
	assert.Contains(t, entry, "…`", "an over-long statement is truncated with an ellipsis inside the code span")
	assert.LessOrEqual(t, len([]rune(entry)), 300, "the entry stays scannable next to the others in its section")
}

// A copy the engine has no starting statement for still names the cause. The
// clause that would explain it is omitted rather than left dangling, since
// there is nothing to compare the plan against.
func TestRenderPlanComment_DiscardedCopyWithoutAStartingStatement(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers, Statement: "   "},
		},
	})

	assert.Contains(t, out, "- `orders` in `testapp`: the schema change differs from the one that started it\n")
	assert.NotContains(t, out, "which was")
}

// entryLine returns the single copy entry beginning with prefix, so an
// assertion can hold the whole entry to itself rather than matching a substring
// that may have wrapped onto a neighbouring line.
func entryLine(t *testing.T, out, prefix string) string {
	t.Helper()
	for line := range strings.SplitSeq(out, "\n") {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	t.Fatalf("no copy entry starting with %q in:\n%s", prefix, out)
	return ""
}

// A copy the apply will resume is disclosed as a continuation rather than a
// warning: nothing is destroyed, so an operator seeing the apply reappear
// knows it is not starting over.
func TestRenderPlanComment_AdoptedCopyReadsAsContinuation(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		AdoptedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders", "products"}, Age: "3h 12m"},
		},
	})

	assert.Contains(t, out, "♻️ **Resuming work in progress**: **1** unfinished copy on the target will be continued")
	assert.Contains(t, out, "- `orders`, `products` in `testapp` (last progress 3h 12m ago)")
	assert.Contains(t, out, "Applying picks up where the existing copy stopped")
	assert.NotContains(t, out, "destroys work in progress", "an adopted copy is not a discard warning")
}

// Both sections can land on one comment, and an apply already under way is not
// a decision anyone is being offered. Each then describes what the apply is
// doing rather than what applying would do, so the two read as one disclosure
// instead of a warning spliced onto a hypothetical. The plural follows the
// count: several resumed copies stopped, not one.
func TestRenderPlanComment_AdoptedCopyReadsAsAnEventOnceApplying(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, IsLocked: true,
		LockOwner: "block/schemabot#42",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "orders_a", Tables: []string{"orders"}, Reason: engine.DiscardCheckpointExpired, Age: "9d 4h"},
		},
		AdoptedCopies: []ExistingCopyData{
			{Namespace: "orders_b", Tables: []string{"orders"}, Age: "45s"},
			{Namespace: "orders_c", Tables: []string{"products"}, Age: "2m 30s"},
		},
	})

	assert.Contains(t, out, "**Applying automatically**",
		"the fixture is the automatic path, which is what makes both sections a record")
	assert.Contains(t, out, "ℹ️ **This apply destroys work in progress**")
	assert.Contains(t, out, "♻️ **Resuming work in progress**: **2** unfinished copies on the target will be continued")
	assert.Contains(t, out, "This apply picks up where the existing copies stopped rather than starting over.")
	assert.NotContains(t, out, "Applying picks up",
		"the hypothetical subject belongs to a comment where applying is still a choice")
}

// A copy still being made on the target is disclosed as work the apply joins,
// not work it picks back up. Nothing stopped, so nothing is resumed: the apply
// resolves the operator onto the copy already in flight and keeps every row of
// it. The entry says the copy is running instead of dating its last progress,
// because a live copy checkpoints continuously and "last progress 4s ago"
// reports healthy work as nearly stalled.
func TestRenderPlanComment_RunningCopyReadsAsJoiningWorkInFlight(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		RunningCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders", "products"}, Age: "4s", Running: true},
		},
	})

	assert.Contains(t, out, "♻️ **Work already in progress**: **1** unfinished copy still running on the target")
	assert.Equal(t, "- `orders`, `products` in `testapp` (still copying)", entryLine(t, out, "- `orders`"))
	assert.Contains(t, out, "Applying joins the copy already running rather than starting a new one: "+
		"every row copied so far is kept, and no second copy is made.")

	assert.NotContains(t, out, "4s", "a running copy's checkpoint interval is not its staleness")
	assert.NotContains(t, out, "stopped rather than starting over",
		"nothing stopped, so there is nothing to pick back up")
	assert.NotContains(t, out, "destroys work in progress", "a running copy is not a discard warning")
}

// Work that stopped and work still being made are separate promises, so a
// comment holding both discloses each in its own section rather than
// describing one as the other. On an apply already under way each states what
// the apply did rather than what applying would do, matching the discard
// section on the same comment.
func TestRenderPlanComment_RunningAndStoppedCopiesAreDisclosedApart(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true, IsLocked: true,
		LockOwner: "block/schemabot#42",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		AdoptedCopies: []ExistingCopyData{
			{Namespace: "orders_a", Tables: []string{"orders"}, Age: "3h 12m"},
		},
		RunningCopies: []ExistingCopyData{
			{Namespace: "orders_b", Tables: []string{"products"}, Age: "4s", Running: true},
			{Namespace: "orders_c", Tables: []string{"shipments"}, Age: "2s", Running: true},
		},
	})

	assert.Contains(t, out, "♻️ **Resuming work in progress**: **1** unfinished copy on the target will be continued")
	assert.Equal(t, "- `orders` in `orders_a` (last progress 3h 12m ago)", entryLine(t, out, "- `orders` in"),
		"a copy that stopped is still dated by how stale it is")

	assert.Contains(t, out, "♻️ **Work already in progress**: **2** unfinished copies still running on the target")
	assert.Equal(t, "- `products` in `orders_b` (still copying)", entryLine(t, out, "- `products`"))
	assert.Contains(t, out, "This apply joined the copies already running rather than starting new ones",
		"an apply under way describes what it did, not what applying would do")
	assert.Contains(t, out, "This apply picks up where the existing copy stopped",
		"the resumed section keeps its own promise on the same comment")
}

// A copy can be live and still be one the apply destroys: another PR's copy of
// the same table, progressing this second, is thrown away by a plan whose
// statement differs from the one that started it. The destructive warning keeps
// it — the work really is lost — but dates it as the running copy it is, since
// a live copy's checkpoint interval read as staleness would show the liveliest
// work on the target as the most abandoned.
func TestRenderPlanComment_RunningCopyKeepsItsHeartbeatOutOfTheDiscardWarning(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
		}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "orders_a", Tables: []string{"orders"}, Age: "3h 12m", Reason: "statement_differs"},
			{Namespace: "orders_b", Tables: []string{"products"}, Age: "4s", Reason: "statement_differs", Running: true},
		},
	})

	assert.Contains(t, out, "⚠️ **Applying destroys work in progress**: **2** unfinished copies on the target",
		"a running copy the apply throws away is still a discard")
	assert.Equal(t, "- `orders` in `orders_a` (last progress 3h 12m ago): the schema change differs from the one that started it",
		entryLine(t, out, "- `orders` in"),
		"a copy that stopped is dated by how stale it is")
	assert.Equal(t, "- `products` in `orders_b` (still copying): the schema change differs from the one that started it",
		entryLine(t, out, "- `products`"),
		"a copy still being made reports that rather than its checkpoint interval")
	assert.NotContains(t, out, "4s", "a running copy's checkpoint interval is not its staleness")
}

// An expired checkpoint is a different cause from a changed statement, and the
// remedy differs, so the section names which one applies. Its age is what
// expired it, so the entry reads as the staleness the cause describes rather
// than as time spent copying.
func TestRenderPlanComment_DiscardedCopyNamesExpiryCause(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardCheckpointExpired, Age: "9d 4h"},
		},
	})

	assert.Contains(t, out, "⚠️ **Applying destroys work in progress**: **1** unfinished copy on the target\n")
	assert.Contains(t, out, "- `orders` in `testapp` (last progress 9d 4h ago): it is too old to resume")
	assert.NotContains(t, out, "9d 4h of copying",
		"an expired checkpoint is stale by construction; naming it as copying time contradicts the cause below it")
}

// A copy covering only part of the schema change is discarded for a cause the
// operator did not create and cannot undo by restoring the batch, so the
// section says so plainly rather than implying the schema change drifted.
func TestRenderPlanComment_DiscardedCopyNamesPartialCoverage(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardCopyIncomplete, Age: "3h 12m"},
		},
	})

	assert.Contains(t, out, "- `orders` in `testapp` (last progress 3h 12m ago): it covers only some of the tables this schema change alters")
}

// Several discarded copies were started by several schema changes, so the
// section counts them as copies and its remedy speaks of changes and of them —
// telling an operator to re-apply "the schema change that started it" when two
// statements started two copies names nothing they can act on.
func TestRenderPlanComment_SeveralDiscardedCopiesReadAsPlural(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "orders_a", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers, Age: "3h 12m"},
			{Namespace: "orders_b", Tables: []string{"orders"}, Reason: engine.DiscardCheckpointExpired, Age: "9d 4h"},
		},
	})

	assert.Contains(t, out, "⚠️ **Applying destroys work in progress**: **2** unfinished copies on the target\n")
	assert.Contains(t, out, "- `orders` in `orders_a` (last progress 3h 12m ago): the schema change differs from the one that started it")
	assert.Contains(t, out, "- `orders` in `orders_b` (last progress 9d 4h ago): it is too old to resume")
	assert.Contains(t, out, "Applying restarts the copies from zero rows.")
	assert.Contains(t, out, "To keep the work already done, apply the schema changes that started them.",
		"two copies were started by two schema changes, so the remedy names both in the plural")
	assert.NotContains(t, out, "the schema change that started it",
		"the singular remedy points at one statement an operator cannot identify from two copies")
}

// A copy with no recorded progress to date it by still names the tables it
// covers; the age is omitted rather than rendered as a bare zero, which would
// read as "started just now" and understate what is being thrown away.
func TestRenderPlanComment_DiscardedCopyWithoutAge(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
		DiscardedCopies: []ExistingCopyData{
			{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers},
		},
	})

	assert.Contains(t, out, "⚠️ **Applying destroys work in progress**: **1** unfinished copy on the target\n")
	assert.Contains(t, out, "- `orders` in `testapp`: the schema change differs from the one that started it",
		"an unknown age is omitted rather than rendered as a bare zero, which would read as a copy that just started")
	assert.Contains(t, out, "To keep the work already done, apply the schema change that started it.")
	assert.NotContains(t, out, "last progress")
}

// A copy sits on one environment's target, so two environments planning the
// same DDL are not the same disclosure. Every environment SchemaBot serves in
// production posts through the multi-environment comment, and it is the comment
// an apply is confirmed from, so an environment whose target holds discardable
// work must show that in its own section rather than being folded under a
// combined header that speaks for a target it was never read from.
func TestRenderMultiEnvPlanComment_IdenticalDDLStillDisclosesOneEnvironmentsCopy(t *testing.T) {
	alterOrders := []KeyspaceChangeData{{
		Keyspace:   "testapp",
		Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
	}}

	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging": {Environment: "staging", IsMySQL: true, Changes: alterOrders},
			"production": {
				Environment: "production", IsMySQL: true, Changes: alterOrders,
				DiscardedCopies: []ExistingCopyData{
					{Namespace: "testapp", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers, Age: "9h 40m"},
				},
			},
		},
	})

	assert.Contains(t, out, "### Staging\n", "the environment holding no copy keeps its own section")
	assert.Contains(t, out, "### Production\n", "the environment holding the copy keeps its own section")
	assert.NotContains(t, out, "### Staging & Production",
		"one section cannot speak for two targets when only one of them holds discardable work")
	assert.Contains(t, out, "⚠️ **Applying destroys work in progress**: **1** unfinished copy on the target")
	assert.Contains(t, out, "- `orders` in `testapp` (last progress 9h 40m ago): the schema change differs from the one that started it")
}

// The two dispositions are opposite promises, so an environment that resumes
// its copy and one that destroys its copy can never share a section — the
// surviving promise would be shown over work that is about to be destroyed.
func TestRenderMultiEnvPlanComment_AdoptAndDiscardNeverShareASection(t *testing.T) {
	alterOrders := []KeyspaceChangeData{{
		Keyspace:   "testapp",
		Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
	}}
	copyOnOrders := ExistingCopyData{Namespace: "testapp", Tables: []string{"orders"}, Age: "45s"}
	discarded := copyOnOrders
	discarded.Reason = engine.DiscardCheckpointExpired

	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Environment: "staging", IsMySQL: true, Changes: alterOrders, AdoptedCopies: []ExistingCopyData{copyOnOrders}},
			"production": {Environment: "production", IsMySQL: true, Changes: alterOrders, DiscardedCopies: []ExistingCopyData{discarded}},
		},
	})

	assert.NotContains(t, out, "### Staging & Production")
	assert.Contains(t, out, "♻️ **Resuming work in progress**")
	assert.Contains(t, out, "⚠️ **Applying destroys work in progress**")
}

// A copy still being made sits on one environment's target, so identical DDL is
// not identical disclosure: an environment whose target holds live work must
// say so in its own section. Folding the two under a combined header would both
// drop the join promise and let one section speak for a target it was never
// read from.
func TestRenderMultiEnvPlanComment_IdenticalDDLStillDisclosesOneEnvironmentsRunningCopy(t *testing.T) {
	alterOrders := []KeyspaceChangeData{{
		Keyspace:   "testapp",
		Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"},
	}}

	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging": {Environment: "staging", IsMySQL: true, Changes: alterOrders},
			"production": {
				Environment: "production", IsMySQL: true, Changes: alterOrders,
				RunningCopies: []ExistingCopyData{
					{Namespace: "testapp", Tables: []string{"orders"}, Running: true},
				},
			},
		},
	})

	assert.Contains(t, out, "### Staging\n", "the environment holding no copy keeps its own section")
	assert.Contains(t, out, "### Production\n", "the environment holding the running copy keeps its own section")
	assert.NotContains(t, out, "### Staging & Production",
		"one section cannot speak for two targets when only one of them holds live work")
	assert.Contains(t, out, "♻️ **Work already in progress**: **1** unfinished copy still running on the target")
	assert.Contains(t, out, "- `orders` in `testapp` (still copying)")
}

// A running copy discarded because its checkpoint expired is both live and
// unresumable: rows are still being made while the checkpoint has aged past the
// engine's resume bound. The age is that reason's own evidence, so this one
// entry renders it alongside the running marker instead of suppressing it —
// dropping either half would hide the wedge from the operator deciding on it.
func TestWriteDiscardedCopies_RunningCheckpointExpiredKeepsItsAge(t *testing.T) {
	var sb strings.Builder
	writeDiscardedCopies(&sb, []ExistingCopyData{{
		Namespace: "testapp",
		Tables:    []string{"orders"},
		Reason:    engine.DiscardCheckpointExpired,
		Age:       "4d 2h",
		Running:   true,
	}}, false)

	assert.Contains(t, sb.String(),
		"- `orders` in `testapp` (still copying, last checkpoint 4d 2h ago): it is too old to resume")
}

// A plan against a clean target renders exactly as it always has: no copy
// disclosure at all, which is the ordinary case.
func TestRenderPlanComment_NoCopySectionOnCleanTarget(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)"}}},
	})

	assert.NotContains(t, out, "work in progress")
}
