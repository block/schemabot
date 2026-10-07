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
							plan.Changes = nil
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

						assert.EqualError(t, runErr, "apply blocked: the plan contains changes its target's engine refuses; change the schema files so the engine accepts them")
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
