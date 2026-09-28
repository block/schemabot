package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	const applyConsequences = " drops it and plans and applies that environment in one step, subject to the environment ordering gate, pausing for `apply-confirm` only if its plan needs it."

	scoped := RenderConfirmationPlanForOtherEnvironment("staging", "production", "orders", ApplyCommandOptions{Tenant: "acme", DeferCutover: true})
	assert.Contains(t, scoped, "The pending confirmation is for `staging`, not `production`; nothing was applied.")
	assert.Contains(t, scoped, "Run `schemabot apply-confirm -e staging -d orders --tenant acme --defer-cutover` to confirm that plan.")
	assert.Contains(t, scoped, "`schemabot apply -e production -d orders --tenant acme --defer-cutover`"+applyConsequences)

	unscoped := RenderConfirmationPlanForOtherEnvironment("staging", "production", "", ApplyCommandOptions{})
	assert.Contains(t, unscoped, "Run `schemabot apply-confirm -e staging` to confirm that plan.")
	assert.Contains(t, unscoped, "`schemabot apply -e production`"+applyConsequences)
}

// The missing-plan rejection's recovery command carries the rejected command's
// database scope, tenant, and option flags, so it can be pasted as-is on a PR
// that manages several databases. An unscoped command is answered unscoped.
// The command's consequences are pinned in full: it replaces the pending
// confirmation with a fresh plan and applies it in one step, answers to the
// environment ordering gate, and pauses for apply-confirm only when its plan
// needs one.
func TestRenderConfirmationPlanUnavailableScopesRecoveryCommand(t *testing.T) {
	const applyConsequences = " to replace that confirmation with a fresh plan and apply it in one step; that apply is subject to the environment ordering gate and pauses for `apply-confirm` only if its plan needs it."

	scoped := RenderConfirmationPlanUnavailable("production", "orders", ApplyCommandOptions{Tenant: "acme", AllowUnsafe: true})
	assert.Contains(t, scoped, "Run `schemabot apply -e production -d orders --tenant acme --allow-unsafe`"+applyConsequences)

	unscoped := RenderConfirmationPlanUnavailable("production", "", ApplyCommandOptions{})
	assert.Contains(t, unscoped, "Run `schemabot apply -e production`"+applyConsequences)
}

// Both confirmation refusals are posted through the generic error comment,
// whose clamp cuts anything past its budget and appends a truncation marker.
// The recovery guidance sits at the end of each message, so a message that
// overflows loses exactly the part the operator needs. Every option flag set
// at once produces the longest commands the messages can carry, and both
// must still come out of the sanitizer unchanged.
func TestConfirmationRefusalsFitTheCommentErrorClamp(t *testing.T) {
	everyFlag := ApplyCommandOptions{Tenant: "acme", AllowUnsafe: true, DeferCutover: true, SkipRevert: true}

	otherEnvironment := RenderConfirmationPlanForOtherEnvironment("production", "production", "orders", everyFlag)
	assert.LessOrEqual(t, len([]rune(otherEnvironment)), maxCommentErrorLen)
	assert.Equal(t, otherEnvironment, sanitizeCommentError(otherEnvironment))

	unavailable := RenderConfirmationPlanUnavailable("production", "orders", everyFlag)
	assert.LessOrEqual(t, len([]rune(unavailable)), maxCommentErrorLen)
	assert.Equal(t, unavailable, sanitizeCommentError(unavailable))
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
