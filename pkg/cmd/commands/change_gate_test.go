package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

var gateEnds = time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

// freezeRefusal is the 409 the server answers a freeze-refused apply, start or
// cutover with. recorded adds the override request SchemaBot recorded.
func freezeRefusal(recorded bool) apitypes.ErrorResponse {
	resp := apitypes.ErrorResponse{
		Error:     "production is frozen",
		ErrorCode: apitypes.ErrCodeChangeGateRefused,
		Blockers: []apitypes.ChangeGateBlocker{
			{Gate: "freeze", ID: "freeze-abc", Reason: "SEV-1234 payments latency"},
			{Gate: "freeze", ID: "freeze-def", Reason: "holiday freeze", EndsAt: &gateEnds},
		},
	}
	if recorded {
		resp.OverrideRequest = &apitypes.FreezeOverrideRequest{ID: "ovr-123"}
	}
	return resp
}

func gateUnavailable() apitypes.ErrorResponse {
	return apitypes.ErrorResponse{
		Error:     "could not check freezes",
		ErrorCode: apitypes.ErrCodeChangeGateUnavailable,
	}
}

// gateApplyServer serves one auto-approved apply whose POST /api/apply is
// answered with status and body, recording the apply request and the lock
// releases the CLI sends.
func gateApplyServer(t *testing.T, status int, body any) (*httptest.Server, *[]apitypes.ApplyRequest, *atomic.Int32) {
	t.Helper()
	plan := planWithTablesAndEngine("mysql", createUsers())
	plan.PlanID = "plan-gate"
	var applies []apitypes.ApplyRequest
	var releases atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /api/plan":
			assert.NoError(t, json.NewEncoder(w).Encode(plan))
		case "POST /api/locks/acquire":
			_, _ = w.Write([]byte(acquireCreatedLock))
		case "POST /api/apply":
			var req apitypes.ApplyRequest
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			applies = append(applies, req)
			w.WriteHeader(status)
			assert.NoError(t, json.NewEncoder(w).Encode(body))
		case "DELETE /api/locks":
			releases.Add(1)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		}
	}))
	t.Cleanup(server.Close)
	return server, &applies, &releases
}

func runGateApply(t *testing.T, server *httptest.Server, override string, output OutputFormat) (string, error) {
	t.Helper()
	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", AutoApprove: true, Override: override, Output: output}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL}) }))
	return out, runErr
}

func TestApplySendsTheOverrideReason(t *testing.T) {
	server, applies, _ := gateApplyServer(t, http.StatusOK, apitypes.ApplyResponse{Accepted: true, ApplyID: "apply-1"})

	_, err := runGateApply(t, server, "  SEV-1234 fix the index  ", OutputFormatJSON)

	require.NoError(t, err)
	require.Len(t, *applies, 1)
	assert.Equal(t, "SEV-1234 fix the index", (*applies)[0].Override)
}

// A freeze refusal is printed once as the refusal, with the next step for what
// the operator did, and the lock the command took is released because nothing
// was submitted.
func TestApplyPrintsTheFreezeRefusal(t *testing.T) {
	cases := map[string]struct {
		recorded bool
		override string
		want     []string
		notWant  []string
	}{
		"override recorded": {
			recorded: true, override: "SEV-1234 fix the index",
			want:    []string{"Override requested: ovr-123. A freeze approver other than you must run 'schemabot freeze approve ovr-123', then run this command again."},
			notWant: []string{"If this change can't wait"},
		},
		"no override asked": {
			want:    []string{`If this change can't wait, run the same command with --override "<reason>".`},
			notWant: []string{"Override requested"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server, applies, releases := gateApplyServer(t, http.StatusConflict, freezeRefusal(tc.recorded))

			out, err := runGateApply(t, server, tc.override, OutputFormatLog)

			require.ErrorIs(t, err, ErrSilent, "main must not print a second line")
			assert.Equal(t, 1, ExitCodeFor(err))
			assert.Len(t, *applies, 1)
			assert.Contains(t, out, "❄️ production is frozen. This change did not start.\n")
			assert.Contains(t, out, "   freeze-abc  SEV-1234 payments latency  (until lifted)\n")
			assert.Contains(t, out, "   freeze-def  holiday freeze  (until 2026-11-22T10:00:00Z)\n")
			for _, line := range tc.want {
				assert.Contains(t, out, line)
			}
			for _, line := range tc.notWant {
				assert.NotContains(t, out, line)
			}
			assert.Equal(t, 1, strings.Count(out, "is frozen"), "the refusal is printed once:\n%s", out)
			assert.Equal(t, int32(1), releases.Load(), "an unsubmitted lock is released:\n%s", out)
		})
	}
}

// With -o json stdout stays parseable: the refusal travels in the error.
func TestApplyJSONOutputKeepsStdoutCleanOnRefusal(t *testing.T) {
	server, _, _ := gateApplyServer(t, http.StatusConflict, freezeRefusal(true))

	out, err := runGateApply(t, server, "SEV-1234", OutputFormatJSON)

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrSilent)
	assert.ErrorContains(t, err, "production is frozen. This change did not start.")
	assert.NotContains(t, out, "is frozen")
}

func TestApplyExplainsAnUnavailableChangeGate(t *testing.T) {
	server, _, releases := gateApplyServer(t, http.StatusServiceUnavailable, gateUnavailable())

	_, err := runGateApply(t, server, "", OutputFormatLog)

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrSilent)
	assert.ErrorContains(t, err, "SchemaBot could not check the freezes, so nothing started; try again in a moment")
	assert.Equal(t, int32(1), releases.Load(), "nothing started, so the lock is released")
}

// Start and cutover send the reason in the control request and print the same
// refusal.
func TestStartAndCutoverFreezeRefusal(t *testing.T) {
	cases := map[string]struct {
		applyState string
		path       string
		run        func(override string) func(*Globals) error
		outcome    string
	}{
		"start": {
			state.Apply.Stopped, "/api/start",
			func(o string) func(*Globals) error {
				return (&StartCmd{ControlFlags: controlTestFlags(), Override: o}).Run
			},
			"❄️ staging is frozen. This change did not start.",
		},
		"cutover": {
			state.Apply.WaitingForCutover, "/api/cutover",
			func(o string) func(*Globals) error {
				return func(g *Globals) error {
					return (&CutoverCmd{ControlFlags: controlTestFlags(), Override: o}).Run(t.Context(), g)
				}
			},
			"❄️ staging is frozen. This cutover did not start.",
		},
	}
	for name, tc := range cases {
		t.Run(name+" with a recorded override request", func(t *testing.T) {
			server, requests := serveControlTestStub(t, controlTestStub{
				applyState: tc.applyState, controlPath: tc.path,
				controlStatus: http.StatusConflict, controlBody: freezeRefusal(true),
			})

			out, err := runControlCmd(server.URL, tc.run("SEV-1234 fix the index"))

			require.ErrorIs(t, err, ErrSilent)
			assertSentOnce(t, *requests)
			assert.Equal(t, "SEV-1234 fix the index", (*requests)[0].Override)
			assert.Contains(t, out, tc.outcome+"\n")
			assert.Contains(t, out, "   freeze-abc  SEV-1234 payments latency  (until lifted)\n")
			assert.Contains(t, out, "Override requested: ovr-123. A freeze approver other than you must run 'schemabot freeze approve ovr-123', then run this command again.")
			assert.Equal(t, 1, strings.Count(out, "is frozen"))
		})
		t.Run(name+" without --override", func(t *testing.T) {
			server, requests := serveControlTestStub(t, controlTestStub{
				applyState: tc.applyState, controlPath: tc.path,
				controlStatus: http.StatusConflict, controlBody: freezeRefusal(false),
			})

			out, err := runControlCmd(server.URL, tc.run(""))

			require.ErrorIs(t, err, ErrSilent)
			assertSentOnce(t, *requests)
			assert.Empty(t, (*requests)[0].Override)
			assert.Contains(t, out, tc.outcome+"\n")
			assert.Contains(t, out, `If this change can't wait, run the same command with --override "<reason>".`)
		})
		t.Run(name+" when the gate cannot check", func(t *testing.T) {
			server, _ := serveControlTestStub(t, controlTestStub{
				applyState: tc.applyState, controlPath: tc.path,
				controlStatus: http.StatusServiceUnavailable, controlBody: gateUnavailable(),
			})

			_, err := runControlCmd(server.URL, tc.run(""))

			require.Error(t, err)
			assert.NotErrorIs(t, err, ErrSilent)
			assert.ErrorContains(t, err, "SchemaBot could not check the freezes, so nothing started; try again in a moment")
		})
	}
}

// With -o json the error prints only the template's text, which is
// terminal-safe; the raw server message never reaches stderr, yet the API
// error is still there for lock cleanup.
func TestQuietChangeGateRefusalPrintsOnlyTheTemplate(t *testing.T) {
	apiErr := &client.APIError{
		Status: http.StatusConflict, ErrorCode: apitypes.ErrCodeChangeGateRefused,
		Message:         "production is frozen\n\x1b[31mINJECTED",
		Blockers:        []apitypes.ChangeGateBlocker{{Gate: "freeze", ID: "freeze-abc", Reason: "SEV-1234"}},
		OverrideRequest: &apitypes.FreezeOverrideRequest{ID: "ovr-123"},
	}

	err := reportChangeGate(apiErr, "production", "apply", true, true)

	assert.Equal(t, strings.Join(templates.ChangeGateRefusedText(templates.ChangeGateRefusedData{
		Environment: "production", Operation: "apply", Blockers: apiErr.Blockers,
		OverrideRequest: apiErr.OverrideRequest, OverrideAsked: true, Message: apiErr.Message,
	}), " "), err.Error())
	assert.NotContains(t, err.Error(), "INJECTED")
	assert.ErrorIs(t, err, apiErr, "the API error stays reachable for lock cleanup")
}

// An apply the server lets through a freeze on an override names the override
// request, so the operator sees that the freeze was bypassed and which record
// says so, in text and in JSON alike.
func TestApplyNamesTheOverrideThatLetItThroughAFreeze(t *testing.T) {
	accepted := apitypes.ApplyResponse{Accepted: true, ApplyID: "apply-1", FreezeOverride: "ovr-123"}

	t.Run("text", func(t *testing.T) {
		server, _, _ := gateApplyServer(t, http.StatusOK, accepted)
		cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", AutoApprove: true, Override: "SEV-1234 fix the index", Output: OutputFormatLog}
		var runErr error
		out := stripAnsi(captureStdout(func() {
			runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
		}))
		require.NoError(t, runErr)
		assert.Contains(t, out, "Apply started: apply-1\n❄️ production is frozen; override ovr-123 let this change through. See it with 'schemabot freeze override ovr-123'.\n")
	})

	t.Run("json", func(t *testing.T) {
		server, _, _ := gateApplyServer(t, http.StatusOK, accepted)
		cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", AutoApprove: true, Override: "SEV-1234 fix the index", Output: OutputFormatJSON}
		var runErr error
		out := captureStdout(func() { runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL}) })
		require.NoError(t, runErr)
		assert.Contains(t, out, `"freeze_override":"ovr-123"`)
	})

	t.Run("no freeze held it", func(t *testing.T) {
		server, _, _ := gateApplyServer(t, http.StatusOK, apitypes.ApplyResponse{Accepted: true, ApplyID: "apply-1"})
		cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", AutoApprove: true, Output: OutputFormatLog}
		var runErr error
		out := stripAnsi(captureStdout(func() { runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL}) }))
		require.NoError(t, runErr)
		assert.NotContains(t, out, "override")
	})
}

// An apply refused for its unsafe changes also shows the freezes that would
// refuse it, as plan does, so allowing the unsafe changes is not followed by
// a second refusal the operator could not see coming.
func TestBlockUnsafeApplyShowsTheFreezesToo(t *testing.T) {
	plan := planWithTablesAndEngine("mysql", &apitypes.TableChangeResponse{
		TableName: "legacy", DDL: "DROP TABLE `legacy`", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "drops a table",
	})
	plan.Freezes = []apitypes.Freeze{{ID: "freeze-abc", Environment: "production", Reason: "SEV-1234 payments latency", InEffect: true}}

	var err error
	out := stripAnsi(captureStdout(func() {
		err = blockUnsafeApply(plan, "orders", "production", "schema", "", "", OutputFormatLog)
	}))

	require.ErrorIs(t, err, ErrSilent)
	assert.Contains(t, out, "❄️ production is frozen, so applying this plan would be refused now.\n")
	assert.Contains(t, out, "   freeze-abc  SEV-1234 payments latency  (until lifted)\n")
	assert.Contains(t, out, `apply -s schema -e production --allow-unsafe --override "<why it can't wait>"`,
		"the retry carries --override, so allowing the unsafe change is not refused by the freeze next")
}

// The unsafe retry keeps the reason the operator already gave, and offers
// no --override at all when no freeze is in effect.
func TestBlockUnsafeApplyRetryOverride(t *testing.T) {
	newPlan := func(inEffect bool) *apitypes.PlanResponse {
		plan := planWithTablesAndEngine("mysql", &apitypes.TableChangeResponse{
			TableName: "legacy", DDL: "DROP TABLE `legacy`", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "drops a table",
		})
		plan.Freezes = []apitypes.Freeze{{ID: "freeze-abc", Environment: "production", Reason: "SEV-1234 payments latency", InEffect: inEffect}}
		return plan
	}
	for name, tc := range map[string]struct {
		inEffect bool
		override string
		want     string
	}{
		"the operator's own reason": {inEffect: true, override: "Hotfix: checkout needs it", want: "--allow-unsafe --override 'Hotfix: checkout needs it'\n"},
		"a scheduled freeze only":   {inEffect: false, want: "apply -s schema -e production --allow-unsafe\n"},
	} {
		t.Run(name, func(t *testing.T) {
			var err error
			out := stripAnsi(captureStdout(func() {
				err = blockUnsafeApply(newPlan(tc.inEffect), "orders", "production", "schema", "", tc.override, OutputFormatLog)
			}))
			require.ErrorIs(t, err, ErrSilent)
			assert.Contains(t, out, tc.want)
		})
	}
}
