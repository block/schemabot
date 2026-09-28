package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func planWithChanges() PlanCommentData {
	return PlanCommentData{
		Database:    "orders",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "orders",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	}
}

// An operator who scoped their command with -d is answered with commands that
// stay scoped to the database they chose, so the follow-up they are being asked
// for can be pasted as-is in a repository that configures several databases.
// An unscoped command is answered with unscoped commands.
func TestPlanCommentCommandsCarryScopedDatabase(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*PlanCommentData)
		render  func(PlanCommentData) string
		unscope string
		scoped  string
	}{
		{
			name:    "apply",
			render:  RenderPlanComment,
			unscope: "schemabot apply -e staging",
			scoped:  "schemabot apply -e staging -d orders",
		},
		{
			name: "apply-confirm",
			mutate: func(d *PlanCommentData) {
				d.IsLocked = true
				d.PendingManualConfirmation = true
			},
			render:  RenderPlanComment,
			unscope: "schemabot apply-confirm -e staging",
			scoped:  "schemabot apply-confirm -e staging -d orders",
		},
		{
			name: "unlock",
			mutate: func(d *PlanCommentData) {
				d.IsLocked = true
				d.PendingManualConfirmation = true
			},
			render:  RenderPlanComment,
			unscope: "schemabot unlock",
			scoped:  "schemabot unlock -d orders",
		},
		{
			name: "unsafe changes rejected",
			mutate: func(d *PlanCommentData) {
				d.HasUnsafeChanges = true
				d.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: "drops a column", ChangeType: "drop"}}
			},
			render:  RenderUnsafeChangesBlocked,
			unscope: "schemabot apply -e staging --allow-unsafe",
			scoped:  "schemabot apply -e staging -d orders --allow-unsafe",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := planWithChanges()
			if tc.mutate != nil {
				tc.mutate(&data)
			}

			unscoped := tc.render(data)
			assert.Contains(t, unscoped, "\n"+tc.unscope+"\n", "an unscoped command is answered with a bare command")
			assert.NotContains(t, unscoped, " -d ")

			data.ScopedDatabase = "orders"
			assert.Contains(t, tc.render(data), "\n"+tc.scoped+"\n", "a -d command is answered with the same database")
		})
	}
}

// The scoped database sits between the environment and the deployment's tenant
// so every pasteable command reads the same way whatever produced it, and a
// command that carries one flag carries the other.
func TestPlanCommentScopedDatabasePrecedesTenant(t *testing.T) {
	data := planWithChanges()
	data.ScopedDatabase = "orders"
	data.Tenant = "acme"

	assert.Contains(t, RenderPlanComment(data), "\nschemabot apply -e staging -d orders --tenant acme\n")

	data.IsLocked = true
	data.PendingManualConfirmation = true
	locked := RenderPlanComment(data)
	assert.Contains(t, locked, "\nschemabot apply-confirm -e staging -d orders --tenant acme\n")
	assert.Contains(t, locked, "\nschemabot unlock -d orders --tenant acme\n")
}

// The locked plan footer's apply-confirm command repeats every option the apply
// was requested with, after the target and tenant, so the operator confirms
// the plan they were shown rather than a default one.
func TestLockedPlanCommentConfirmCommandCarriesApplyOptions(t *testing.T) {
	data := planWithChanges()
	data.ScopedDatabase = "orders"
	data.Tenant = "acme"
	data.IsLocked = true
	data.PendingManualConfirmation = true
	data.AllowUnsafe = true
	data.DeferCutover = true
	data.SkipRevert = true

	assert.Contains(t, RenderPlanComment(data), "\nschemabot apply-confirm -e staging -d orders --tenant acme --allow-unsafe --defer-cutover --skip-revert\n")
}

// An apply-confirm that names another environment than the pending
// confirmation was planned for is answered with two recovery commands, and
// both keep the rejected command's database scope, tenant, and option flags:
// the operator chose those, and apply-confirm reads them from the comment it
// arrives in and nowhere else. An unscoped command is answered unscoped. The
// apply command's consequences are pinned in full: it drops the pending
// confirmation, plans and applies the requested environment in one step,
// answers to the environment ordering gate, and pauses for apply-confirm only
// when its plan needs one.
func TestRenderConfirmationPlanForOtherEnvironmentScopesRecoveryCommands(t *testing.T) {
	const applyConsequences = "To apply `production` instead, dropping the pending `staging` confirmation and planning and applying `production` in one step, subject to the environment ordering gate and pausing for `apply-confirm` only if its plan needs it:\n\n"

	scoped := RenderConfirmationPlanForOtherEnvironment(ConfirmationRefusalData{
		RequestedBy:          "hubot",
		Database:             "orders",
		PlanEnvironment:      "staging",
		RequestedEnvironment: "production",
		Options:              ApplyCommandOptions{Tenant: "acme", DeferCutover: true},
	})
	assert.True(t, strings.HasPrefix(scoped, "## "+glyph.Refused+" Apply-confirm Refused — Production\n\n**Database**: `orders`\n\n"), scoped)
	assert.Contains(t, scoped, "The pending confirmation is for `staging`, not `production`; nothing was applied.\n\n")
	assert.Contains(t, scoped, "To confirm the `staging` plan:\n\n```\nschemabot apply-confirm -e staging -d orders --tenant acme --defer-cutover\n```\n")
	assert.Contains(t, scoped, applyConsequences+"```\nschemabot apply -e production -d orders --tenant acme --defer-cutover\n```\n")
	assert.Contains(t, scoped, "\n_Requested by @hubot_\n")

	unscoped := RenderConfirmationPlanForOtherEnvironment(ConfirmationRefusalData{PlanEnvironment: "staging", RequestedEnvironment: "production"})
	assert.True(t, strings.HasPrefix(unscoped, "## "+glyph.Refused+" Apply-confirm Refused — Production\n\nThe pending confirmation"), unscoped)
	assert.NotContains(t, unscoped, "**Database**")
	assert.Contains(t, unscoped, "```\nschemabot apply-confirm -e staging\n```\n")
	assert.Contains(t, unscoped, applyConsequences+"```\nschemabot apply -e production\n```\n")
	assert.NotContains(t, unscoped, "Requested by")
}

// The missing-plan refusal's recovery command carries the rejected command's
// database scope, tenant, and option flags, so it can be pasted as-is on a PR
// that manages several databases. An unscoped command is answered unscoped.
// The command's consequences are pinned in full: it replaces the pending
// confirmation with a fresh plan and applies it in one step, answers to the
// environment ordering gate, and pauses for apply-confirm only when its plan
// needs one.
func TestRenderConfirmationPlanUnavailableScopesRecoveryCommand(t *testing.T) {
	const applyConsequences = "To replace that confirmation with a fresh plan and apply it in one step, subject to the environment ordering gate and pausing for `apply-confirm` only if its plan needs it:\n\n"

	scoped := RenderConfirmationPlanUnavailable(ConfirmationRefusalData{
		RequestedBy:          "hubot",
		Database:             "orders",
		RequestedEnvironment: "production",
		Options:              ApplyCommandOptions{Tenant: "acme", AllowUnsafe: true},
	})
	assert.True(t, strings.HasPrefix(scoped, "## "+glyph.Refused+" Apply-confirm Refused — Production\n\n**Database**: `orders`\n\n"), scoped)
	assert.Contains(t, scoped, "The pending confirmation is not backed by a plan SchemaBot can load, so it could not verify which environment was reviewed; nothing was applied.\n\n")
	assert.Contains(t, scoped, applyConsequences+"```\nschemabot apply -e production -d orders --tenant acme --allow-unsafe\n```\n")
	assert.Contains(t, scoped, "\n_Requested by @hubot_\n")

	unscoped := RenderConfirmationPlanUnavailable(ConfirmationRefusalData{RequestedEnvironment: "production"})
	assert.NotContains(t, unscoped, "**Database**")
	assert.Contains(t, unscoped, applyConsequences+"```\nschemabot apply -e production\n```\n")
	assert.NotContains(t, unscoped, "Requested by")
}

// Both confirmation refusals are comments of their own, not generic error
// comments, so the recovery commands and the consequences that follow them
// survive intact however long the identifiers are. A database name at the
// engine's identifier limit with every option flag set at once produces the
// longest commands the comments can carry, and each command must still appear
// whole, in its own fenced block, with nothing truncated after it.
func TestConfirmationRefusalsCarryLongCommandsIntact(t *testing.T) {
	longDatabase := strings.Repeat("orders_ledger_v2", 4)
	require.Len(t, longDatabase, 64)
	everyFlag := ApplyCommandOptions{Tenant: "acme-holdings-eu", AllowUnsafe: true, DeferCutover: true, SkipRevert: true}
	flags := " -d " + longDatabase + " --tenant acme-holdings-eu --allow-unsafe --defer-cutover --skip-revert"

	otherEnvironment := RenderConfirmationPlanForOtherEnvironment(ConfirmationRefusalData{
		RequestedBy:          "hubot",
		Database:             longDatabase,
		PlanEnvironment:      "staging",
		RequestedEnvironment: "production",
		Options:              everyFlag,
	})
	assert.Greater(t, len([]rune(otherEnvironment)), maxCommentErrorLen, "the scenario must be one the generic error clamp would have cut")
	assert.Contains(t, otherEnvironment, "```\nschemabot apply-confirm -e staging"+flags+"\n```\n")
	assert.Contains(t, otherEnvironment, "pausing for `apply-confirm` only if its plan needs it:\n\n```\nschemabot apply -e production"+flags+"\n```\n\n_Requested by @hubot_\n")
	assert.NotContains(t, otherEnvironment, "…")

	unavailable := RenderConfirmationPlanUnavailable(ConfirmationRefusalData{
		RequestedBy:          "hubot",
		Database:             longDatabase,
		RequestedEnvironment: "production",
		Options:              everyFlag,
	})
	assert.Greater(t, len([]rune(unavailable)), maxCommentErrorLen, "the scenario must be one the generic error clamp would have cut")
	assert.Contains(t, unavailable, "pausing for `apply-confirm` only if its plan needs it:\n\n```\nschemabot apply -e production"+flags+"\n```\n\n_Requested by @hubot_\n")
	assert.NotContains(t, unavailable, "…")
}

// A plan run without -e answers with one comment covering every environment,
// and each of its commands carries the operator's -d the same way the
// single-environment comment does.
func TestMultiEnvPlanCommentCommandsCarryScopedDatabase(t *testing.T) {
	staging := planWithChanges()
	production := planWithChanges()
	production.Environment = "production"

	data := MultiEnvPlanCommentData{
		Database:     "orders",
		IsMySQL:      true,
		Environments: []string{"staging", "production", "sandbox"},
		Plans: map[string]*PlanCommentData{
			"staging":    &staging,
			"production": &production,
		},
		Errors: map[string]string{"sandbox": "connection refused"},
	}

	unscoped := RenderMultiEnvPlanComment(data)
	assert.Contains(t, unscoped, "\nschemabot apply -e staging\n")
	assert.Contains(t, unscoped, "\nschemabot apply -e production\n")
	assert.Contains(t, unscoped, "\nschemabot plan -e sandbox\n")
	assert.NotContains(t, unscoped, " -d ")

	data.ScopedDatabase = "orders"
	scoped := RenderMultiEnvPlanComment(data)
	assert.Contains(t, scoped, "\nschemabot apply -e staging -d orders\n")
	assert.Contains(t, scoped, "\nschemabot apply -e production -d orders\n")
	assert.Contains(t, scoped, "\nschemabot plan -e sandbox -d orders\n", "the retry for a failed environment stays scoped too")
}
