package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

var (
	freezeStart = time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	freezeEnd   = time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
)

// freezeFixture is the on-demand freeze the test server holds.
func freezeFixture() apitypes.Freeze {
	return apitypes.Freeze{
		ID: "freeze-abc", Environment: "production", Kind: "on_demand", Running: "continue",
		StartsAt: freezeStart, Reason: "SEV-1234 payments latency", CreatedBy: "dana",
		Source: "manual", CreatedAt: freezeStart, InEffect: true,
	}
}

func overrideFixture() apitypes.FreezeOverrideRequest {
	return apitypes.FreezeOverrideRequest{
		ID: "ovr-123", Environment: "production", Action: "apply", TargetID: "orders/mysql/orders",
		Database: "orders", Blockers: []string{"freeze freeze-abc"}, Reason: "fix a failing checkout",
		RequestedBy: "sam", ExpiresAt: freezeStart.Add(time.Hour),
	}
}

// freezeAPI serves one canned reply and records the request it got.
type freezeAPI struct {
	status int
	reply  any

	method, path, query string
	body                map[string]any
}

func (s *freezeAPI) start(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.method, s.path, s.query = r.Method, r.URL.Path, r.URL.RawQuery
		s.body = nil
		if r.Method == http.MethodPost {
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&s.body))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(s.status)
		assert.NoError(t, json.NewEncoder(w).Encode(s.reply))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func runFreezeCmd(endpoint string, run func(*Globals) error) (string, error) {
	return runLockCmd(endpoint, run)
}

func TestFreezeCreateCmdPrintsTheFreezeAndSendsItsFlags(t *testing.T) {
	f := freezeFixture()
	f.Deployments, f.Databases, f.Running = []string{"payments"}, []string{"orders", "games"}, "stop"
	f.StopSweep = &apitypes.FreezeStopSweep{
		Stopped:    []string{"apply-111", "apply-222"},
		NotStopped: []apitypes.FreezeUnstoppedApply{{ApplyID: "apply-333", Reason: "stop is not supported for this schema change; use cancel to permanently cancel it"}},
	}
	api := &freezeAPI{status: http.StatusCreated, reply: f}
	endpoint := api.start(t)

	cmd := FreezeCreateCmd{
		Environment: "production", Reason: "SEV-1234 payments latency", Running: "stop",
		Deployment: []string{"payments"}, Database: []string{"orders", "games"},
		End: "2026-11-22T10:00:00Z",
	}
	out, err := runFreezeCmd(endpoint, cmd.Run)

	require.NoError(t, err)
	assert.Contains(t, out, "freeze-abc created: production frozen (on demand, until lifted) by dana. Reason: SEV-1234 payments latency\n")
	assert.Contains(t, out, "Scope:           deployments payments; databases orders, games\n")
	assert.Contains(t, out, "Starts:          2026-11-21T10:00:00Z\n")
	// The confirmation names every apply the freeze asked to stop, says the
	// stops are requests that status confirms, and names each one it could
	// not stop with its own remedy, so the responder knows what is still
	// running and is never told to run a stop that cannot work.
	assert.Contains(t, out, "Running applies: stop\n")
	assert.Contains(t, out, "Stop requested:  apply-111, apply-222\n")
	assert.Contains(t, out, "Confirm each one has stopped with 'schemabot status <apply-id>'.\n")
	assert.Contains(t, out, "Not stopped:     apply-333 keeps running: stop is not supported for this schema change; use cancel to permanently cancel it\n")
	assert.NotContains(t, out, "schemabot stop")
	assert.Equal(t, http.MethodPost, api.method)
	assert.Equal(t, "/api/freezes", api.path)
	assert.Equal(t, "SEV-1234 payments latency", api.body["reason"])
	assert.Equal(t, "production", api.body["environment"])
	assert.Equal(t, "stop", api.body["running"])
	assert.Equal(t, "2026-11-22T10:00:00Z", api.body["ends_at"])
	assert.NotContains(t, api.body, "kind")
}

// A stop freeze's confirmation says what setting it did to the applies already
// running: none were in scope, the sweep could not list them, or the freeze
// starts later and stops nothing now.
func TestFreezeCreateCmdReportsTheStopSweep(t *testing.T) {
	tests := []struct {
		name     string
		inEffect bool
		sweep    *apitypes.FreezeStopSweep
		want     []string
	}{
		{
			name: "NothingInScope", inEffect: true, sweep: &apitypes.FreezeStopSweep{Stopped: []string{}},
			want: []string{"Running applies: stop\n", "Stop requested:  none; no schema change in scope was pending or running\n"},
		},
		{
			name: "ListFailed", inEffect: true,
			sweep: &apitypes.FreezeStopSweep{Stopped: []string{}, Error: "SchemaBot could not list the schema changes in scope, so it stopped none of them."},
			want:  []string{"Stop requested:  none. SchemaBot could not list the schema changes in scope, so it stopped none of them.\n"},
		},
		{
			name: "StartsLater",
			want: []string{"Running applies: stop (applies running when it starts are not stopped; use 'schemabot stop')\n"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := freezeFixture()
			f.Running, f.InEffect, f.StopSweep = "stop", tc.inEffect, tc.sweep
			api := &freezeAPI{status: http.StatusCreated, reply: f}
			endpoint := api.start(t)

			out, err := runFreezeCmd(endpoint, (&FreezeCreateCmd{Environment: "production", Reason: "SEV-1234", Running: "stop"}).Run)

			require.NoError(t, err)
			for _, line := range tc.want {
				assert.Contains(t, out, line)
			}
			assert.NotContains(t, out, "Not stopped:")
		})
	}
}

// When every apply in scope is one the freeze could not stop, the
// confirmation says it requested no stop, never that nothing was in scope.
func TestFreezeCreateCmdReportsOnlyUnstoppedApplies(t *testing.T) {
	f := freezeFixture()
	f.Running, f.InEffect = "stop", true
	f.StopSweep = &apitypes.FreezeStopSweep{Stopped: []string{}, NotStopped: []apitypes.FreezeUnstoppedApply{
		{ApplyID: "apply-333", Reason: "stop is not supported for this schema change; use cancel to permanently cancel it"},
	}}
	api := &freezeAPI{status: http.StatusCreated, reply: f}
	endpoint := api.start(t)

	out, err := runFreezeCmd(endpoint, (&FreezeCreateCmd{Environment: "production", Reason: "SEV-1234", Running: "stop"}).Run)

	require.NoError(t, err)
	assert.Contains(t, out, "Stop requested:  none\n")
	assert.NotContains(t, out, "no schema change in scope")
	assert.Contains(t, out, "Not stopped:     apply-333 keeps running: stop is not supported")
}

func TestFreezeCreateCmdPlannedWindow(t *testing.T) {
	f := freezeFixture()
	f.Kind, f.InEffect, f.EndsAt = "planned", false, &freezeEnd
	api := &freezeAPI{status: http.StatusCreated, reply: f}
	endpoint := api.start(t)

	cmd := FreezeCreateCmd{Environment: "production", Reason: "holiday", Planned: true, Start: "2026-11-21T10:00:00Z", End: "2026-11-22T10:00:00Z"}
	out, err := runFreezeCmd(endpoint, cmd.Run)

	require.NoError(t, err)
	assert.Contains(t, out, "Running applies: continue\n")
	assert.Contains(t, out, "production freeze scheduled (planned, until 2026-11-22T10:00:00Z)")
	assert.Equal(t, "planned", api.body["kind"])
	assert.Equal(t, "2026-11-21T10:00:00Z", api.body["starts_at"])
}

// A time the CLI cannot read never reaches the server.
func TestFreezeCreateCmdRejectsABadTime(t *testing.T) {
	api := &freezeAPI{status: http.StatusCreated, reply: freezeFixture()}
	endpoint := api.start(t)

	_, err := runFreezeCmd(endpoint, (&FreezeCreateCmd{Environment: "production", Reason: "x", End: "tomorrow"}).Run)

	require.ErrorContains(t, err, "--end must be an RFC3339 time")
	assert.Empty(t, api.method)
}

// The server's validation message is what the operator sees, not a wrapped
// Go error.
func TestFreezeCommandsShowTheServerMessage(t *testing.T) {
	tests := []struct {
		name   string
		status int
		msg    string
		run    func(*Globals) error
	}{
		{"create refused", http.StatusBadRequest, "a planned freeze needs an end time", (&FreezeCreateCmd{Environment: "production", Reason: "x", Planned: true}).Run},
		{"show missing", http.StatusNotFound, `freeze "freeze-x" not found`, (&FreezeShowCmd{ID: "freeze-x"}).Run},
		{"approve forbidden", http.StatusForbidden, "an override needs an approver other than the person who asked for it", (&FreezeApproveCmd{ID: "ovr-123"}).Run},
		{"approve already approved", http.StatusConflict, `override request "ovr-123" is already approved`, (&FreezeApproveCmd{ID: "ovr-123"}).Run},
		{"override missing", http.StatusNotFound, `override request "ovr-9" not found`, (&FreezeOverrideCmd{ID: "ovr-9"}).Run},
		{"unfreeze lifted", http.StatusConflict, `freeze "freeze-abc" is already lifted`, (&UnfreezeCmd{ID: "freeze-abc"}).Run},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := &freezeAPI{status: tc.status, reply: apitypes.ErrorResponse{Error: tc.msg}}
			endpoint := api.start(t)

			out, err := runFreezeCmd(endpoint, tc.run)

			require.Error(t, err)
			assert.Equal(t, tc.msg, err.Error())
			assert.Empty(t, out)
		})
	}
}

func TestFreezeListCmdPrintsEachGroup(t *testing.T) {
	active := freezeFixture()
	lifted := freezeFixture()
	lifted.ID, lifted.InEffect = "freeze-old", false
	liftedAt := freezeStart.Add(time.Hour)
	lifted.LiftedAt, lifted.LiftedBy, lifted.LiftReason = &liftedAt, "erin", "SEV resolved"
	scheduled := freezeFixture()
	scheduled.ID, scheduled.Kind, scheduled.InEffect, scheduled.EndsAt = "freeze-next", "planned", false, &freezeEnd
	// A planned freeze cancelled before its window opened never held
	// anything; it keeps its planned window and says it was cancelled.
	cancelled := freezeFixture()
	cancelled.ID, cancelled.Kind, cancelled.InEffect = "freeze-cancelled", "planned", false
	cancelled.StartsAt = freezeStart.Add(48 * time.Hour)
	cancelledEnd, cancelledAt := freezeStart.Add(72*time.Hour), freezeStart.Add(2*time.Hour)
	cancelled.EndsAt, cancelled.LiftedAt, cancelled.LiftedBy = &cancelledEnd, &cancelledAt, "erin"
	api := &freezeAPI{status: http.StatusOK, reply: apitypes.FreezeListResponse{
		InEffect: []apitypes.Freeze{active}, Scheduled: []apitypes.Freeze{scheduled}, Recent: []apitypes.Freeze{cancelled, lifted},
	}}
	endpoint := api.start(t)

	out, err := runFreezeCmd(endpoint, (&FreezeListCmd{Environment: "production"}).Run)

	require.NoError(t, err)
	assert.Equal(t, "environment=production", api.query)
	assert.Contains(t, out, "In effect (1)\n  1. freeze-abc  production (on demand, in effect)\n")
	assert.Contains(t, out, "Scheduled (1)\n  1. freeze-next  production (planned)\n")
	assert.Contains(t, out, "Window:   2026-11-21T10:00:00Z until 2026-11-22T10:00:00Z\n")
	assert.Contains(t, out, "Recently ended (2)\n  1. freeze-cancelled  production (planned, cancelled)\n")
	assert.Contains(t, out, "Window:   2026-11-23T10:00:00Z until 2026-11-24T10:00:00Z, cancelled at 2026-11-21T12:00:00Z before it started\n")
	assert.Contains(t, out, "  2. freeze-old  production (on demand, lifted)\n")
	assert.Contains(t, out, "Window:   2026-11-21T10:00:00Z until lifted at 2026-11-21T11:00:00Z\n")
	assert.Contains(t, out, "Lifted:   by erin at 2026-11-21T11:00:00Z: SEV resolved\n")
	assert.Contains(t, out, "Reason:   SEV-1234 payments latency\n")
	assert.Contains(t, out, "By:       dana\n")
}

func TestFreezeListCmdSaysWhenThereAreNone(t *testing.T) {
	api := &freezeAPI{status: http.StatusOK, reply: apitypes.FreezeListResponse{}}
	endpoint := api.start(t)

	out, err := runFreezeCmd(endpoint, (&FreezeListCmd{}).Run)

	require.NoError(t, err)
	assert.Contains(t, out, "In effect (0)\n  None.\n")
	assert.Empty(t, api.query)
}

func TestFreezeShowCmdPrintsDetail(t *testing.T) {
	f := freezeFixture()
	f.ExternalID = "INC-42"
	api := &freezeAPI{status: http.StatusOK, reply: f}
	endpoint := api.start(t)

	out, err := runFreezeCmd(endpoint, (&FreezeShowCmd{ID: "freeze-abc"}).Run)

	require.NoError(t, err)
	assert.Equal(t, "/api/freezes/freeze-abc", api.path)
	assert.Contains(t, out, "Freeze freeze-abc  production (on demand, in effect)\n")
	assert.Contains(t, out, "Running:  continue (applies already running in scope)\n")
	assert.Contains(t, out, "Source:   manual\n")
	assert.Contains(t, out, "External: INC-42\n")
}

func TestFreezeApproveCmdPrintsTheApproval(t *testing.T) {
	approved := overrideFixture()
	api := &freezeAPI{status: http.StatusOK, reply: approved}
	endpoint := api.start(t)

	out, err := runFreezeCmd(endpoint, (&FreezeApproveCmd{ID: "ovr-123"}).Run)

	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, api.method)
	assert.Equal(t, "/api/freezes/overrides/ovr-123/approve", api.path)
	assert.Equal(t, "✓ Approved ovr-123 for apply on orders in production. Requested by sam: fix a failing checkout. "+
		"It expires at 2026-11-21T11:00:00Z. sam can now re-run the command.\n", out)
}

func TestFreezeOverrideCmdShowsTheRequest(t *testing.T) {
	api := &freezeAPI{status: http.StatusOK, reply: overrideFixture()}
	endpoint := api.start(t)

	out, err := runFreezeCmd(endpoint, (&FreezeOverrideCmd{ID: "ovr-123"}).Run)

	require.NoError(t, err)
	assert.Equal(t, http.MethodGet, api.method)
	assert.Equal(t, "/api/freezes/overrides/ovr-123", api.path)
	assert.Contains(t, out, "Override request ovr-123: waiting for approval\n")
	assert.Contains(t, out, "Action:       apply on orders in production\n")
	assert.Contains(t, out, "Blockers:     freeze freeze-abc\n")
}

// unfreeze confirms the lift with what the freeze now records: who lifted
// it, when, why, and what it covered.
func TestUnfreezeCmdLiftsTheFreeze(t *testing.T) {
	lifted := freezeFixture()
	liftedAt := freezeStart.Add(time.Hour)
	lifted.InEffect, lifted.LiftedAt, lifted.LiftedBy, lifted.LiftReason = false, &liftedAt, "erin", "SEV resolved"
	api := &freezeAPI{status: http.StatusOK, reply: lifted}
	endpoint := api.start(t)

	out, err := runFreezeCmd(endpoint, (&UnfreezeCmd{ID: "freeze-abc", Reason: "SEV resolved"}).Run)

	require.NoError(t, err)
	assert.Equal(t, "✓ freeze-abc lifted by erin at 2026-11-21T11:00:00Z: SEV resolved.\n   Scope:  whole environment in production\n", out)
	assert.Equal(t, "/api/freezes/freeze-abc/lift", api.path)
	assert.Equal(t, "SEV resolved", api.body["reason"])
}

// Unfreezing a planned freeze before its window opens cancels it; the
// confirmation says so rather than calling it lifted.
func TestUnfreezeCmdSaysAFreezeCancelledBeforeItStarted(t *testing.T) {
	planned := freezeFixture()
	planned.Kind, planned.InEffect, planned.StartsAt = "planned", false, freezeStart.Add(48*time.Hour)
	cancelledAt := freezeStart.Add(time.Hour)
	planned.LiftedAt, planned.LiftedBy, planned.LiftReason = &cancelledAt, "erin", "launch moved"
	api := &freezeAPI{status: http.StatusOK, reply: planned}
	endpoint := api.start(t)

	out, err := runFreezeCmd(endpoint, (&UnfreezeCmd{ID: "freeze-abc", Reason: "launch moved"}).Run)

	require.NoError(t, err)
	assert.Equal(t, "✓ freeze-abc cancelled before it started, by erin at 2026-11-21T11:00:00Z: launch moved.\n"+
		"   Scope:  whole environment in production\n", out)
}

// Whoever set a freeze chooses its reason, so control runes in it never reach
// the terminal.
func TestFreezeOutputDropsTerminalEscapes(t *testing.T) {
	f := freezeFixture()
	f.Reason = "latency\x1b[2J"
	api := &freezeAPI{status: http.StatusOK, reply: f}
	endpoint := api.start(t)

	out, err := runFreezeCmd(endpoint, (&FreezeShowCmd{ID: "freeze-abc"}).Run)

	require.NoError(t, err)
	assert.NotContains(t, out, "\x1b")
}

func TestFreezeFlagParsing(t *testing.T) {
	var cli struct {
		Freeze   FreezeCmd   `cmd:""`
		Unfreeze UnfreezeCmd `cmd:""`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	// A bare freeze with flags is the create command.
	ctx, err := parser.Parse([]string{"freeze", "-e", "production", "--reason", "SEV-1234", "--running", "stop",
		"--deployment", "payments", "--deployment", "billing", "--database", "orders", "--planned",
		"--start", "2026-11-21T10:00:00Z", "--end", "2026-11-22T10:00:00Z"})
	require.NoError(t, err)
	assert.Equal(t, "freeze create", ctx.Command())
	create := cli.Freeze.Create
	assert.Equal(t, "production", create.Environment)
	assert.Equal(t, "SEV-1234", create.Reason)
	assert.Equal(t, "stop", create.Running)
	assert.Equal(t, []string{"payments", "billing"}, create.Deployment)
	assert.Equal(t, []string{"orders"}, create.Database)
	assert.True(t, create.Planned)
	assert.Equal(t, "2026-11-22T10:00:00Z", create.End)

	// The subcommands still win over the default.
	for args, command := range map[string]string{
		"freeze list -e production":     "freeze list",
		"freeze show freeze-abc":        "freeze show <freeze-id>",
		"freeze approve ovr-123":        "freeze approve <ovr-id>",
		"freeze override ovr-123":       "freeze override <ovr-id>",
		"unfreeze freeze-abc":           "unfreeze <freeze-id>",
		"freeze create -e p --reason r": "freeze create",
	} {
		ctx, err = parser.Parse(strings.Fields(args))
		require.NoError(t, err, args)
		assert.Equal(t, command, ctx.Command(), args)
	}

	// Environment and reason are required to create a freeze. Each case gets
	// a fresh parser: kong keeps the values an earlier parse set.
	for args, missing := range map[string]string{
		"freeze --reason SEV-1234": "--environment",
		"freeze -e production":     "--reason",
	} {
		var fresh struct {
			Freeze FreezeCmd `cmd:""`
		}
		freshParser, err := kong.New(&fresh)
		require.NoError(t, err)
		_, err = freshParser.Parse(strings.Fields(args))
		require.ErrorContains(t, err, missing, args)
	}
}
