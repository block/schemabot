package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

func TestBranchFlagValidation(t *testing.T) {
	tests := []struct {
		name    string
		branch  string
		wantErr string
	}{
		{
			name:   "empty branch is allowed",
			branch: "",
		},
		{
			name:   "development branch is allowed",
			branch: "my-feature-branch",
		},
		{
			name:    "main branch is rejected",
			branch:  "main",
			wantErr: "cannot reuse the main branch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBranchFlag(tt.branch)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBuildApplyOptionsDefersDeployOnlyForPlanetScale(t *testing.T) {
	tests := []struct {
		name         string
		engine       string
		deferDeploy  bool
		watch        bool
		format       OutputFormat
		wantDeferred bool
	}{
		{
			name:         "interactive PlanetScale apply defers deploy for review",
			engine:       storage.EnginePlanetScale,
			watch:        true,
			format:       OutputFormatInteractive,
			wantDeferred: true,
		},
		{
			name:         "explicit PlanetScale defer deploy is preserved",
			engine:       storage.EnginePlanetScale,
			deferDeploy:  true,
			format:       OutputFormatLog,
			wantDeferred: true,
		},
		{
			name:   "interactive MySQL apply does not show PlanetScale deploy controls",
			engine: storage.EngineSpirit,
			watch:  true,
			format: OutputFormatInteractive,
		},
		{
			name:        "explicit MySQL defer deploy is not sent",
			engine:      storage.EngineSpirit,
			deferDeploy: true,
			format:      OutputFormatLog,
		},
		{
			name:   "unknown engine does not auto defer deploy",
			engine: "",
			watch:  true,
			format: OutputFormatInteractive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := buildApplyOptions(&apitypes.PlanResponse{Engine: tt.engine}, false, tt.deferDeploy, false, false, "", tt.watch, tt.format)
			_, gotDeferred := options["defer_deploy"]
			assert.Equal(t, tt.wantDeferred, gotDeferred)
		})
	}
}

func TestBuildApplyOptionsPreservesOtherOptions(t *testing.T) {
	options := buildApplyOptions(&apitypes.PlanResponse{Engine: storage.EnginePlanetScale}, true, true, true, true, "feature-branch", false, OutputFormatLog)

	require.Equal(t, "true", options["defer_cutover"])
	require.Equal(t, "true", options["defer_deploy"])
	require.Equal(t, "true", options["skip_revert"])
	require.Equal(t, "true", options["allow_unsafe"])
	require.Equal(t, "feature-branch", options["branch"])
}

// A blocked primary plan refuses before any lock request or confirmation, for
// a single target, a whole rollout beside a converged secondary, and a narrowed
// apply. A blocked verdict on a divergent shard is equally binding; neither
// unsafe consent nor automatic approval permits it, and --yield has no lock to
// release.
func TestApplyCmd_BlockedPrimaryRefusesBeforeLockOrPrompt(t *testing.T) {
	for _, scope := range []string{"single", "whole-rollout", "narrowed"} {
		for _, work := range []string{"namespace", "shard-only"} {
			for _, output := range []OutputFormat{OutputFormatInteractive, OutputFormatLog, OutputFormatJSON} {
				for _, consent := range []struct {
					name        string
					allowUnsafe bool
					autoApprove bool
				}{
					{name: "attended"},
					{name: "attended-allow-unsafe", allowUnsafe: true},
					{name: "auto-approved-allow-unsafe", allowUnsafe: true, autoApprove: true},
				} {
					t.Run(scope+"/"+work+"/"+string(output)+"/"+consent.name, func(t *testing.T) {
						blocked := &apitypes.TableChangeResponse{
							TableName: "users", DDL: "ALTER TABLE `users` DROP PRIMARY KEY", ChangeType: "alter",
							ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "direct execution is disabled",
							IsUnsafe: true, UnsafeReason: "drops a primary key",
						}
						plan := planWithTablesAndEngine("mysql", blocked)
						plan.PlanID = "plan-blocked"
						if work == "shard-only" {
							// The namespace-level change is the collapsed view
							// and carries no verdict; only the divergent shard's
							// row is blocked.
							collapsed := *blocked
							collapsed.ExecutionMode, collapsed.ModeReason = "", ""
							plan.Changes = []*apitypes.SchemaChangeResponse{{Namespace: "orders", TableChanges: []*apitypes.TableChangeResponse{&collapsed}}}
							plan.Shards = []*apitypes.ShardPlanResponse{{Namespace: "orders", Shard: "-80", Changes: []*apitypes.TableChangeResponse{blocked}}}
						}
						var target string
						paths := []string{"/api/status", "/api/plan"}
						switch scope {
						case "whole-rollout":
							plan.Rollout = &apitypes.PlanRolloutResponse{
								Members: 2, Independent: true,
								Groups: []*apitypes.PlanMemberGroupResponse{
									{Members: []string{"prod/payments-001"}, Primary: true, Changes: plan.Changes, Shards: plan.Shards},
									{Members: []string{"prod/payments-002"}},
								},
							}
						case "narrowed":
							target = "payments-002"
							plan.Deployment, plan.Target, plan.NarrowedTo = "prod", target, "prod/"+target
							paths = []string{"/api/plan", "/api/status"}
						}
						recorded, endpoint := newNarrowedPlanServer(t, plan)
						answerPrompt(t, "yes")
						cmd := ApplyCmd{
							SchemaDir: writeTestSchemaDir(t), Environment: "production", Target: target,
							AllowUnsafe: consent.allowUnsafe, AutoApprove: consent.autoApprove, Yield: true, Output: output,
						}
						var runErr error
						out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: endpoint}) }))

						assert.EqualError(t, runErr, `apply blocked: plan plan-blocked contains a blocked change for table "users": direct execution is disabled`)
						if output == OutputFormatJSON {
							assert.NotContains(t, out, "DROP PRIMARY KEY", "JSON output renders no plan; the error carries the refusal")
						} else {
							assert.Contains(t, out, "DROP PRIMARY KEY", "the refused statement is shown before the refusal")
						}
						assert.NotContains(t, out, "Do you want to apply these changes?")
						assert.NotContains(t, out, "--allow-unsafe", "unsafe consent cannot permit a blocked change")
						recorded.mu.Lock()
						defer recorded.mu.Unlock()
						assert.Equal(t, paths, recorded.paths, "no lock is checked, acquired, or released and no apply is requested")
						assert.Equal(t, target, recorded.planReq.Target)
					})
				}
			}
		}
	}
}

// An executable plan still shows its work, asks for consent, and sends the
// apply to the selected target after the operator answers yes.
func TestApplyCmd_ExecutablePrimaryStillPromptsAndApplies(t *testing.T) {
	recorded, endpoint := newNarrowedPlanServer(t, narrowedPlan(createUsers()))
	answerPrompt(t, "yes")
	cmd := targetedApplyCmd(t)
	cmd.AutoApprove = false
	cmd.NoLock = false
	cmd.Output = OutputFormatInteractive
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: endpoint}) }))

	require.NoError(t, runErr)
	assertBefore(t, out, "CREATE TABLE users (id BIGINT PRIMARY KEY);", "Do you want to apply these changes?")
	recorded.mu.Lock()
	defer recorded.mu.Unlock()
	assert.Equal(t, []string{"/api/plan", "/api/status", "/api/locks/testdb/mysql", "/api/locks/acquire", "/api/apply"}, recorded.paths)
	assert.Equal(t, "plan-narrowed", recorded.applyReq.PlanID)
	assert.Equal(t, "prod/payments-002", recorded.applyReq.Target)
}

// The refusal names the blocked table and the engine's reason, which carries
// the remedy: here a grant to provision, not a schema file to change.
func TestApplyCmd_BlockedPrimaryRefusalNamesTableAndReason(t *testing.T) {
	reason := "direct execution is enabled but the database user lacks a grant it needs to end sessions blocking the statement: grant it PROCESS and CONNECTION_ADMIN, then plan again"
	plan := planWithTablesAndEngine("mysql", &apitypes.TableChangeResponse{
		TableName: "users", DDL: "ALTER TABLE `users` DROP PRIMARY KEY", ChangeType: "alter",
		ExecutionMode: engine.ExecutionModeBlocked, ModeReason: reason,
	})
	recorded, endpoint := newNarrowedPlanServer(t, plan)
	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", AutoApprove: true, Output: OutputFormatJSON}
	var runErr error
	captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: endpoint}) })

	require.Error(t, runErr)
	assert.Contains(t, runErr.Error(), `"users"`)
	assert.Contains(t, runErr.Error(), reason)
	assert.NotContains(t, runErr.Error(), "change the schema files")
	recorded.mu.Lock()
	defer recorded.mu.Unlock()
	assert.Equal(t, []string{"/api/status", "/api/plan"}, recorded.paths)
}

// A change blocked for several independent reasons lists each on its own
// line, so an operator fixing the first is not surprised by the second on the
// next attempt. Interactive output shows the refused statement first; JSON
// output renders no plan and carries the whole refusal in the error.
func TestApplyCmd_BlockedPrimaryRefusalListsEachCause(t *testing.T) {
	reason := engine.JoinBlockedCauses([]string{
		"direct execution is enabled but the table is above the configured limit of 1,000,000 rows",
		"the target denies a grant the kill needs: grant it PROCESS, then plan again",
	})
	want := "apply blocked: plan plan-blocked contains a blocked change for table \"users\":\n" +
		"- direct execution is enabled but the table is above the configured limit of 1,000,000 rows\n" +
		"- the target denies a grant the kill needs: grant it PROCESS, then plan again"
	for _, output := range []OutputFormat{OutputFormatInteractive, OutputFormatJSON} {
		t.Run(string(output), func(t *testing.T) {
			plan := planWithTablesAndEngine("mysql", &apitypes.TableChangeResponse{
				TableName: "users", DDL: "ALTER TABLE `users` DROP PRIMARY KEY", ChangeType: "alter",
				ExecutionMode: engine.ExecutionModeBlocked, ModeReason: reason,
			})
			plan.PlanID = "plan-blocked"
			recorded, endpoint := newNarrowedPlanServer(t, plan)
			cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", AutoApprove: true, Output: output}
			var runErr error
			out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: endpoint}) }))

			assert.EqualError(t, runErr, want)
			if output == OutputFormatJSON {
				assert.Empty(t, out)
			} else {
				assert.Contains(t, out, "ALTER TABLE `users` DROP PRIMARY KEY")
			}
			recorded.mu.Lock()
			defer recorded.mu.Unlock()
			assert.Equal(t, []string{"/api/status", "/api/plan"}, recorded.paths)
		})
	}
}

// A blocked primary is refused before the rollout's member refusals: their
// remedy is to apply the refused members on their own and then the rollout
// again, which the primary's verdict would refuse after those members had
// changed, leaving the rollout split.
func TestApplyCmd_BlockedPrimaryRefusesBeforeMemberReruns(t *testing.T) {
	blocked := &apitypes.TableChangeResponse{
		TableName: "users", DDL: "ALTER TABLE `users` DROP PRIMARY KEY", ChangeType: "alter",
		ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "direct execution is enabled but the table is above the configured limit of 1,000,000 rows",
	}
	plan := planWithTablesAndEngine("mysql", blocked)
	plan.PlanID = "plan-blocked"
	plan.Rollout = &apitypes.PlanRolloutResponse{
		Members: 2, Independent: true,
		Groups: []*apitypes.PlanMemberGroupResponse{
			{Members: []string{"prod/payments-001"}, Primary: true, Changes: plan.Changes},
			{Members: []string{"prod/payments-002"}, Changes: plan.Changes},
		},
		Refused: []*apitypes.PlanMemberRefusalResponse{{
			Member: "prod/payments-002", Target: "payments-002", Reason: apitypes.PlanMemberNeedsTarget,
			Detail: `runs table "users" as direct-execution DDL, which a rollout-wide apply runs only from the pull request comment that discloses it under this target`,
		}},
	}
	recorded, endpoint := newNarrowedPlanServer(t, plan)
	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", AllowUnsafe: true, AutoApprove: true, Yield: true, Output: OutputFormatInteractive}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: endpoint}) }))

	assert.EqualError(t, runErr, `apply blocked: plan plan-blocked contains a blocked change for table "users": direct execution is enabled but the table is above the configured limit of 1,000,000 rows`)
	assert.NotContains(t, out, "then apply the rollout again for the rest")
	recorded.mu.Lock()
	defer recorded.mu.Unlock()
	assert.Equal(t, []string{"/api/status", "/api/plan"}, recorded.paths)
}
