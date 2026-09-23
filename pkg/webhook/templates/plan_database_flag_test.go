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
