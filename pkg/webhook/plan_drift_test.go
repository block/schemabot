package webhook

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// The drift summary names diverged deployments so the check's Change column
// tells an operator which deployment to reconcile.
func TestSummarizeReviewDrift_NamesDivergedDeployments(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{
			{Deployment: "eu", Class: api.DeploymentMatch},
			{Deployment: "au", Class: api.DeploymentDiverged},
		},
	}
	summary := summarizeReviewDrift(rollup)
	assert.Contains(t, summary, "diverged: au")
	assert.NotContains(t, summary, "eu")
}

// A deployment that could not be diffed or compared is reported as unverifiable,
// separately from divergence, so the two failure modes are distinguishable.
func TestSummarizeReviewDrift_SeparatesDivergedAndErrored(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{
			{Deployment: "eu", Class: api.DeploymentMatch},
			{Deployment: "au", Class: api.DeploymentDiverged},
			{Deployment: "us", Class: api.DeploymentErrored},
		},
	}
	summary := summarizeReviewDrift(rollup)
	assert.Contains(t, summary, "diverged: au")
	assert.Contains(t, summary, "could not verify: us")
}

// The stored drift summary is bounded to the change_summary column width and is
// kept on a single line with no markdown table separators, so a database with
// many drifted deployments cannot overflow the column or break the aggregate
// table rendering.
func TestSummarizeReviewDrift_BoundedAndSanitized(t *testing.T) {
	var entries []api.DeploymentRollupEntry
	entries = append(entries, api.DeploymentRollupEntry{Deployment: "eu", Class: api.DeploymentMatch})
	for range 200 {
		entries = append(entries, api.DeploymentRollupEntry{
			Deployment: "deployment-with-a-fairly-long-name",
			Class:      api.DeploymentDiverged,
		})
	}
	summary := summarizeReviewDrift(api.PlanRollup{Entries: entries})

	assert.LessOrEqual(t, utf8.RuneCountInString(summary), maxDriftSummaryLen)
	assert.NotContains(t, summary, "\n")
	assert.NotContains(t, summary, "|")
}

// Review-time drift fails the plan check closed even when the reviewed primary
// plan is a clean no-op, taking precedence over the plan's own outcome.
func TestPlanCheckConclusion_DriftFailsClosed(t *testing.T) {
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(false, false, false, true),
		"drift must block even a clean no-op primary plan")
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(true, false, false, true),
		"drift must block a plan that also has changes")
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(false, true, false, false),
		"a primary plan with errors fails closed")
	assert.Equal(t, checkConclusionFailure, planCheckConclusion(true, false, true, false),
		"a final engine refusal fails the plan check")
	assert.Equal(t, checkConclusionActionRequired, planCheckConclusion(true, false, false, false),
		"changes with no final refusal retain the existing action-required policy")
	assert.Equal(t, checkConclusionSuccess, planCheckConclusion(false, false, false, false),
		"a clean no-op plan with no drift passes")
}

// An engine that refuses a statement outright leaves the operator no apply to
// run, so the PR must not be mergeable. Vitess refuses constructs it cannot
// execute at all and PostgreSQL blocks a change it has no classifier verdict
// for; both are properties of the statement, so the plan check fails. A MySQL
// block previews a routing decision that apply time re-resolves against live
// table size and policy, so it stays action-required.
func TestPlanRefusalFailsCheck_FinalRefusalsOnly(t *testing.T) {
	blocked := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "commerce",
			TableChanges: []*apitypes.TableChangeResponse{{
				TableName:     "orders",
				DDL:           "ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
				ExecutionMode: "blocked",
				ModeReason:    "foreign key constraints are not supported",
			}},
		}},
	}

	assert.True(t, planRefusalFailsCheck(storage.DatabaseTypeVitess, blocked),
		"a Vitess refusal cannot be lifted by a re-plan, so it fails the check")
	assert.True(t, planRefusalFailsCheck(storage.DatabaseTypePostgres, blocked),
		"a PostgreSQL refusal fails the check")
	assert.False(t, planRefusalFailsCheck(storage.DatabaseTypeMySQL, blocked),
		"a MySQL block is re-resolved at apply time, so the check stays action-required")

	unblocked := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "commerce",
			TableChanges: []*apitypes.TableChangeResponse{{
				TableName: "orders",
				DDL:       "ALTER TABLE `orders` ADD COLUMN `note` varchar(255) DEFAULT NULL",
			}},
		}},
	}
	assert.False(t, planRefusalFailsCheck(storage.DatabaseTypeVitess, unblocked),
		"an ordinary Vitess change is not a refusal")
}

// A single-deployment database has nothing to compare, so the preview is nil and
// no drift section is rendered on its plan comment.
func TestDeploymentDriftPreview_NilForSingleDeployment(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{{Deployment: "primary", Class: api.DeploymentMatch}},
		Clean:   true,
	}
	assert.Nil(t, deploymentDriftPreview(rollup))
}

// A clean multi-deployment rollup becomes preview data flagged clean and
// computed, with the primary marked and every deployment classified as a match.
func TestDeploymentDriftPreview_CleanMultiDeployment(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{
			{Deployment: "eu", Class: api.DeploymentMatch},
			{Deployment: "au", Class: api.DeploymentMatch, Blocked: 2},
		},
		Clean: true,
	}
	preview := deploymentDriftPreview(rollup)
	assert.NotNil(t, preview)
	assert.True(t, preview.Computed)
	assert.True(t, preview.Clean)
	assert.Len(t, preview.Deployments, 2)
	assert.True(t, preview.Deployments[0].Primary)
	assert.False(t, preview.Deployments[1].Primary)
	assert.Equal(t, "match", preview.Deployments[1].Class)
	assert.Equal(t, 0, preview.Deployments[0].Blocked)
	assert.Equal(t, 2, preview.Deployments[1].Blocked)
}

// A diverged deployment carries a compact change-count detail; an errored
// deployment carries a sanitized error detail so the preview names why each
// deployment is blocking.
func TestDeploymentDriftPreview_DivergedAndErroredDetails(t *testing.T) {
	rollup := api.PlanRollup{
		Entries: []api.DeploymentRollupEntry{
			{Deployment: "eu", Class: api.DeploymentMatch},
			{Deployment: "au", Class: api.DeploymentDiverged, Diff: tern.ChangeSetDiff{
				UnexpectedInCandidate: []tern.ChangeSetDiffItem{{Table: "users"}},
				MissingFromCandidate:  []tern.ChangeSetDiffItem{{Table: "orders"}, {Table: "items"}},
			}},
			{Deployment: "us", Class: api.DeploymentErrored, Err: assert.AnError},
		},
		Clean: false,
	}
	preview := deploymentDriftPreview(rollup)
	assert.False(t, preview.Clean)
	assert.Equal(t, "diverged", preview.Deployments[1].Class)
	assert.Contains(t, preview.Deployments[1].Detail, "1 unexpected")
	assert.Contains(t, preview.Deployments[1].Detail, "2 missing")
	assert.Equal(t, "errored", preview.Deployments[2].Class)
	// The raw diff error stays out of the PR markdown: the preview carries only
	// the sanitized detail, and the underlying error text is not leaked.
	assert.Equal(t, erroredDriftDetail, preview.Deployments[2].Detail)
	assert.NotContains(t, preview.Deployments[2].Detail, assert.AnError.Error())
}

// A rollup where one deployment addresses several targets names each failing
// member by its routing pair, so the check's Change column identifies exactly
// which member to reconcile rather than a deployment with two of them.
func TestSummarizeReviewDrift_NamesMultiTargetMembers(t *testing.T) {
	rollup := api.PlanRollup{
		Planning: api.PlanIndependent,
		Entries: []api.DeploymentRollupEntry{
			{Deployment: "primary", Target: "testapp-001", Class: api.DeploymentPlanned},
			{Deployment: "primary", Target: "testapp-002", Class: api.DeploymentErrored},
			{Deployment: "eu-west", Target: "orders-eu", Class: api.DeploymentErrored},
		},
	}
	summary := summarizeReviewDrift(rollup)
	// Independent targets are never expected to agree, so an unplannable one is
	// not drift between them.
	assert.Contains(t, summary, "could not plan: primary/testapp-002, eu-west")
	assert.NotContains(t, summary, "drift blocks apply")
}

// plannedMember builds a clean, independently-planned rollup member running the
// given DDL. A member with no DDL is already at the desired schema.
func plannedMember(deployment, target string, ddl ...string) api.DeploymentRollupEntry {
	cs := tern.ChangeSet{}
	if len(ddl) > 0 {
		change := &ternv1.SchemaChange{Namespace: "testapp"}
		for _, stmt := range ddl {
			change.TableChanges = append(change.TableChanges, &ternv1.TableChange{
				TableName:  "users",
				Ddl:        stmt,
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
				Namespace:  "testapp",
			})
		}
		cs.Changes = []*ternv1.SchemaChange{change}
	}
	fp, err := tern.ChangeSetFingerprint(schema.DialectMySQL, cs)
	if err != nil {
		panic(err)
	}
	return api.DeploymentRollupEntry{
		DatabaseType:    "vitess",
		Deployment:      deployment,
		Target:          target,
		Class:           api.DeploymentPlanned,
		ChangeSet:       cs,
		PlanFingerprint: fp,
	}
}

// groupMembers flattens the grouped members for assertions.
func groupMembers(groups []templates.DeploymentPlanGroup) [][]string {
	out := make([][]string, len(groups))
	for i, g := range groups {
		out[i] = g.Members
	}
	return out
}

// Targets running the same work are described once and attributed to all of
// them, so a converged fleet does not repeat one plan per target.
func TestDeploymentPlanGroups_SameWorkGroupsTogether(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	rollup := api.PlanRollup{
		Clean:    true,
		Planning: api.PlanIndependent,
		Entries: []api.DeploymentRollupEntry{
			plannedMember("primary", "testapp_1", email),
			plannedMember("primary", "testapp_2", email),
			plannedMember("primary", "testapp_3", email),
		},
	}

	groups := deploymentPlanGroups(rollup)
	assert.Equal(t, [][]string{{"primary/testapp_1", "primary/testapp_2", "primary/testapp_3"}}, groupMembers(groups))
	assert.True(t, groups[0].Primary)
	assert.Equal(t, []string{email}, groups[0].Changes[0].Statements)
	assert.False(t, groups[0].Empty())
}

// Targets that hold their own schemas can need different work. Each distinct
// plan is its own group, so the comment describes every plan the apply would
// run rather than the reviewed one alone.
func TestDeploymentPlanGroups_DifferentWorkSplits(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	phone := "ALTER TABLE users ADD COLUMN phone VARCHAR(32)"
	rollup := api.PlanRollup{
		Clean:    true,
		Planning: api.PlanIndependent,
		Entries: []api.DeploymentRollupEntry{
			plannedMember("primary", "testapp_1", email),
			plannedMember("primary", "testapp_2", phone, email),
			plannedMember("primary", "testapp_3", email),
		},
	}

	groups := deploymentPlanGroups(rollup)
	assert.Equal(t, [][]string{
		{"primary/testapp_1", "primary/testapp_3"},
		{"primary/testapp_2"},
	}, groupMembers(groups))
	assert.Equal(t, []string{email}, groups[0].Changes[0].Statements)
	assert.Equal(t, []string{phone, email}, groups[1].Changes[0].Statements,
		"a group carries the plan its own members would run, not the reviewed one")
}

// Targets already at the desired schema form a group of their own, which the
// comment can name. Folding them into the changing targets would tell an
// operator the apply runs DDL on targets it will not touch.
func TestDeploymentPlanGroups_ConvergedTargetsAreTheirOwnGroup(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	rollup := api.PlanRollup{
		Clean:    true,
		Planning: api.PlanIndependent,
		Entries: []api.DeploymentRollupEntry{
			plannedMember("primary", "testapp_1", email),
			plannedMember("primary", "testapp_2"),
			plannedMember("primary", "testapp_3", email),
			plannedMember("primary", "testapp_4"),
		},
	}

	groups := deploymentPlanGroups(rollup)
	assert.Equal(t, [][]string{
		{"primary/testapp_1", "primary/testapp_3"},
		{"primary/testapp_2", "primary/testapp_4"},
	}, groupMembers(groups))
	assert.False(t, groups[0].Empty())
	assert.True(t, groups[1].Empty(), "targets with nothing to apply are named, not dropped")
}

// The primary's group comes first whatever the primary's own plan, because the
// reviewed plan is the one the operator has already read.
func TestDeploymentPlanGroups_PrimaryGroupComesFirst(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	rollup := api.PlanRollup{
		Clean:    true,
		Planning: api.PlanIndependent,
		Entries: []api.DeploymentRollupEntry{
			plannedMember("primary", "testapp_1"),
			plannedMember("primary", "testapp_2", email),
			plannedMember("primary", "testapp_3", email),
		},
	}

	groups := deploymentPlanGroups(rollup)
	assert.True(t, groups[0].Primary)
	assert.Equal(t, []string{"primary/testapp_1"}, groups[0].Members)
	assert.True(t, groups[0].Empty(), "the primary having nothing to apply does not move its group")
}

// Grouping describes targets that were planned, so a rollup that blocked
// carries none: the operator's next step is the target that could not be
// planned, not the plans of an apply that cannot run.
func TestDeploymentDriftPreview_BlockedRollupIsNotGrouped(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	blocked := plannedMember("primary", "testapp_2")
	blocked.Class = api.DeploymentErrored
	blocked.PlanFingerprint = ""
	blocked.ChangeSet = tern.ChangeSet{}
	rollup := api.PlanRollup{
		Clean:    false,
		Planning: api.PlanIndependent,
		Entries: []api.DeploymentRollupEntry{
			plannedMember("primary", "testapp_1", email),
			blocked,
		},
	}

	preview := deploymentDriftPreview(rollup)
	assert.Empty(t, preview.Plans)
	assert.Len(t, preview.Deployments, 2)
}

// Members required to match each other are not grouped: a clean mirrored rollup
// has already proved they are one group, and re-reporting that in the vocabulary
// of a fleet free to diverge would read as an outcome rather than the
// requirement that let the check pass.
func TestDeploymentDriftPreview_MirroredMembersAreNotGrouped(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	eu := plannedMember("eu", "eu", email)
	eu.Class = api.DeploymentMatch
	au := plannedMember("au", "au", email)
	au.Class = api.DeploymentMatch
	rollup := api.PlanRollup{
		Clean:    true,
		Planning: api.PlanMirrored,
		Entries:  []api.DeploymentRollupEntry{eu, au},
	}

	preview := deploymentDriftPreview(rollup)
	assert.Empty(t, preview.Plans)
}

// A clean independent rollup reaches the comment already grouped, so the
// rendering never has to fall back to describing the contract instead of this
// round's plans.
func TestDeploymentDriftPreview_CleanIndependentRollupCarriesGroups(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	rollup := api.PlanRollup{
		Clean:    true,
		Planning: api.PlanIndependent,
		Entries: []api.DeploymentRollupEntry{
			plannedMember("primary", "testapp_1", email),
			plannedMember("primary", "testapp_2"),
		},
	}

	preview := deploymentDriftPreview(rollup)
	assert.Len(t, preview.Plans, 2)
}

// A member's plan reaches the comment in the same shape the reviewed plan does,
// so a group's changes render through the code that renders the plan a reviewer
// has already read.
func TestMemberPlanChanges_CarriesNamespaceStatements(t *testing.T) {
	cs := tern.ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "testapp",
		TableChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE users ADD COLUMN email VARCHAR(255)"},
			{TableName: "orders", Ddl: "ALTER TABLE orders ADD COLUMN total BIGINT"},
		},
	}}}

	changes := memberPlanChanges(cs)
	assert.Equal(t, []templates.KeyspaceChangeData{{
		Keyspace: "testapp",
		Statements: []string{
			"ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			"ALTER TABLE orders ADD COLUMN total BIGINT",
		},
	}}, changes)
}

// A sharded namespace keeps both views of its changes, so the comment can show
// what applies to which shard rather than a namespace-level view that hides a
// shard.
func TestMemberPlanChanges_KeepsPerShardChanges(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	cs := tern.ChangeSet{
		Changes: []*ternv1.SchemaChange{{
			Namespace:    "testapp",
			TableChanges: []*ternv1.TableChange{{TableName: "users", Ddl: email}},
		}},
		Shards: []*ternv1.ShardPlan{
			{Namespace: "testapp", Shard: "-80", Changes: []*ternv1.TableChange{{TableName: "users", Ddl: email}}},
			{Namespace: "testapp", Shard: "80-", Changes: []*ternv1.TableChange{{TableName: "users", Ddl: email}}},
		},
	}

	changes := memberPlanChanges(cs)
	require.Len(t, changes, 1)
	assert.Equal(t, []string{email}, changes[0].Statements)
	assert.Equal(t, []templates.KeyspaceShardChange{
		{Shard: "-80", Statements: []string{email}},
		{Shard: "80-", Statements: []string{email}},
	}, changes[0].Shards)
}

// A shard already at the desired schema while its siblings change is carried as
// satisfied rather than dropped, so a partially-applied namespace shows its
// divergent state instead of looking uniform.
func TestMemberPlanChanges_MarksSatisfiedShards(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	cs := tern.ChangeSet{
		Changes: []*ternv1.SchemaChange{{
			Namespace:    "testapp",
			TableChanges: []*ternv1.TableChange{{TableName: "users", Ddl: email}},
		}},
		Shards: []*ternv1.ShardPlan{
			{Namespace: "testapp", Shard: "-80", Changes: []*ternv1.TableChange{{TableName: "users", Ddl: email}}},
			{Namespace: "testapp", Shard: "80-"},
		},
	}

	changes := memberPlanChanges(cs)
	require.Len(t, changes[0].Shards, 2)
	assert.False(t, changes[0].Shards[0].Satisfied)
	assert.True(t, changes[0].Shards[1].Satisfied)
}

// A namespace carried only by shard rows still reaches the comment. Dropping it
// would remove work from a plan the comment claims to describe in full.
func TestMemberPlanChanges_KeepsShardOnlyNamespace(t *testing.T) {
	email := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	cs := tern.ChangeSet{Shards: []*ternv1.ShardPlan{
		{Namespace: "testapp", Shard: "-80", Changes: []*ternv1.TableChange{{TableName: "users", Ddl: email}}},
	}}

	changes := memberPlanChanges(cs)
	require.Len(t, changes, 1)
	assert.Equal(t, "testapp", changes[0].Keyspace)
	assert.Equal(t, []string{email}, changes[0].Shards[0].Statements)
}

// A vschema rewrite carries no table DDL. It reaches the comment as a change the
// namespace needs, so a plan that only rewrites the vschema is not mistaken for
// a namespace with nothing to apply.
func TestMemberPlanChanges_CarriesVSchemaChange(t *testing.T) {
	cs := tern.ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "testapp",
		Metadata: map[string]string{
			apitypes.VSchemaChangedMetadataKey: "true",
			apitypes.VSchemaDiffMetadataKey:    "+ table users",
		},
	}}}

	changes := memberPlanChanges(cs)
	require.Len(t, changes, 1)
	assert.True(t, changes[0].VSchemaChanged)
	assert.Equal(t, "+ table users", changes[0].VSchemaDiff)
	assert.Empty(t, changes[0].Statements)
	assert.False(t, templates.DeploymentPlanGroup{Changes: changes}.Empty())
}

// A rendered VSchema diff marks the namespace as carrying work on its own. The
// two metadata keys annotate the same thing, so a renderer that recognized only
// the flag would call a member with work "already at this schema" — and, since
// the grouping key reads the other annotation too, group it with members that
// genuinely have nothing to run.
func TestMemberPlanChanges_DiffAloneIsVSchemaWork(t *testing.T) {
	cs := tern.ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "testapp",
		Metadata:  map[string]string{apitypes.VSchemaDiffMetadataKey: "+ table users"},
	}}}

	changes := memberPlanChanges(cs)
	require.Len(t, changes, 1)
	assert.True(t, changes[0].VSchemaChanged)
	assert.False(t, templates.DeploymentPlanGroup{Changes: changes}.Empty())

	// The grouping key agrees, so this member is not folded in with one that has
	// nothing to run.
	withDiff, err := tern.ChangeSetFingerprint(schema.DialectMySQL, cs)
	require.NoError(t, err)
	empty, err := tern.ChangeSetFingerprint(schema.DialectMySQL, tern.ChangeSet{})
	require.NoError(t, err)
	assert.NotEqual(t, empty, withDiff)
}

// A member already at the desired schema produces no changes at all, which is
// the group the comment names as having nothing to apply.
func TestMemberPlanChanges_EmptyPlanHasNoChanges(t *testing.T) {
	assert.Empty(t, memberPlanChanges(tern.ChangeSet{}))
	assert.True(t, templates.DeploymentPlanGroup{}.Empty())
}
