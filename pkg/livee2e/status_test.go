package livee2e

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// renderStatus renders the apply status comment SchemaBot posts for an apply
// in applyState.
func renderStatus(applyID, applyState string) string {
	return templates.RenderApplyStatusComment(templates.ApplyStatusCommentData{
		ApplyID:     applyID,
		Database:    "testdb",
		Environment: "development",
		RequestedBy: "octocat",
		State:       applyState,
		Engine:      "spirit",
	})
}

func TestParseApplyStatusReadsTheRenderedTemplate(t *testing.T) {
	cases := map[string]string{
		state.Apply.Pending:           StatusQueued,
		state.Apply.Running:           StatusInProgress,
		state.Apply.Resuming:          StatusResuming,
		state.Apply.WaitingForCutover: StatusWaitingForCutover,
		state.Apply.CuttingOver:       StatusCuttingOver,
		state.Apply.Completed:         StatusApplied,
		state.Apply.Stopped:           StatusStopped,
		state.Apply.Cancelled:         StatusCancelled,
		state.Apply.Failed:            StatusFailed,
	}
	for applyState, want := range cases {
		t.Run(applyState, func(t *testing.T) {
			got, ok := ParseApplyStatus(renderStatus("apply-0123456789abcdef", applyState))
			require.True(t, ok, "the status comment for %s should parse", applyState)
			assert.Equal(t, "apply-0123456789abcdef", got.ApplyID)
			assert.Equal(t, want, got.Status)
		})
	}
}

func TestParseApplyStatusIgnoresOtherComments(t *testing.T) {
	summary := templates.ApplyStatusCommentData{
		ApplyID:     "apply-0123456789abcdef",
		Database:    "testdb",
		Environment: "development",
		State:       state.Apply.Completed,
		Engine:      "spirit",
	}
	for name, body := range map[string]string{
		"terminal summary":  templates.RenderApplySummaryComment(summary),
		"failed summary":    renderSummary("apply-0123456789abcdef", state.Apply.Failed),
		"cancelled summary": renderSummary("apply-0123456789abcdef", state.Apply.Cancelled),
		"stopped summary":   renderSummary("apply-0123456789abcdef", state.Apply.Stopped),
		"reverted summary":  renderSummary("apply-0123456789abcdef", state.Apply.Reverted),
		"command":           "schemabot apply -e development",
		"status, no apply":  "**Status**: Applied\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, ok := ParseApplyStatus(body)
			assert.False(t, ok)
		})
	}
}

func TestParseApplyStatusReadsAStartedCommentWithoutStatus(t *testing.T) {
	body := templates.RenderApplyStarted(templates.ApplyStatusCommentData{
		ApplyID:     "apply-0123456789abcdef",
		Database:    "testdb",
		Environment: "development",
		State:       state.Apply.Running,
		Engine:      "spirit",
	})
	got, ok := ParseApplyStatus(body)
	require.True(t, ok)
	assert.Equal(t, ApplyStatus{ApplyID: "apply-0123456789abcdef"}, got)
}

// A sharded apply's status line appends its change count to the label.
func TestParseApplyStatusReadsAShardedStatusComment(t *testing.T) {
	body := templates.RenderShardedApplyComment(templates.ShardedApplyData{
		State:       state.Apply.WaitingForCutover,
		Environment: "development",
		Database:    "testdb",
		ApplyID:     "apply-0123456789abcdef",
		Keyspaces: []templates.ShardedKeyspace{{
			Keyspace: "commerce",
			Tables:   []templates.ShardedTableStatus{{Table: "orders", Status: state.Task.WaitingForCutover}},
		}},
	})
	require.Contains(t, body, " — 0 of 1 change applied", "the rendered status line carries the count")
	got, ok := ParseApplyStatus(body)
	require.True(t, ok)
	assert.Equal(t, ApplyStatus{ApplyID: "apply-0123456789abcdef", Status: StatusWaitingForCutover}, got)
}

func TestBlockedAfterReadsTheLockConflictReply(t *testing.T) {
	blocked := templates.RenderApplyBlockedByOtherPR(templates.ApplyLockConflictData{
		Database:    "testdb",
		Environment: "development",
		RequestedBy: "octocat",
		LockOwner:   "octo/repo#7",
		LockRepo:    "octo/repo",
		LockPR:      7,
	})
	comments := []Comment{{ID: 5, Body: blocked}}
	assert.True(t, blockedAfter(comments, 4))
	assert.False(t, blockedAfter(comments, 5), "a refusal of an earlier command does not count")

	checks := templates.RenderApplyBlockedByNonPassingChecks("development", []templates.BlockingCheck{{Name: "lint", State: "failure"}})
	assert.False(t, blockedAfter([]Comment{{ID: 5, Body: checks}}, 4), "a refusal for failing checks is not a lock conflict")
}

func TestLatestApplyStatusFollowsTheNewestComment(t *testing.T) {
	comments := []Comment{
		{ID: 10, Body: renderStatus("apply-aaaaaaaaaaaaaaaa", state.Apply.Running)},
		{ID: 11, Body: renderStatus("apply-bbbbbbbbbbbbbbbb", state.Apply.Completed)},
		{ID: 12, Body: renderStatus("apply-aaaaaaaaaaaaaaaa", state.Apply.Stopped)},
	}
	got, ok := latestApplyStatus(comments, "apply-aaaaaaaaaaaaaaaa")
	require.True(t, ok)
	assert.Equal(t, StatusStopped, got.Status)
}

func TestFirstApplyAfterSkipsEarlierApplies(t *testing.T) {
	comments := []Comment{
		{ID: 10, Body: renderStatus("apply-aaaaaaaaaaaaaaaa", state.Apply.Completed)},
		{ID: 30, Body: renderStatus("apply-cccccccccccccccc", state.Apply.Running)},
		{ID: 25, Body: renderStatus("apply-bbbbbbbbbbbbbbbb", state.Apply.Pending)},
	}
	got, ok := firstApplyAfter(comments, 20)
	require.True(t, ok)
	assert.Equal(t, "apply-bbbbbbbbbbbbbbbb", got.ApplyID)

	_, ok = firstApplyAfter(comments, 30)
	assert.False(t, ok)
}

func TestInFlightAppliesListsUnfinishedOnes(t *testing.T) {
	comments := []Comment{
		{ID: 1, Body: renderStatus("apply-aaaaaaaaaaaaaaaa", state.Apply.Running)},
		{ID: 2, Body: renderStatus("apply-aaaaaaaaaaaaaaaa", state.Apply.Completed)},
		{ID: 3, Body: renderStatus("apply-bbbbbbbbbbbbbbbb", state.Apply.Running)},
		{ID: 4, Body: renderStatus("apply-bbbbbbbbbbbbbbbb", state.Apply.Stopped)},
		{ID: 5, Body: renderStatus("apply-cccccccccccccccc", state.Apply.WaitingForCutover)},
		{ID: 6, Body: templates.RenderApplyStarted(templates.ApplyStatusCommentData{
			ApplyID: "apply-dddddddddddddddd", Database: "testdb", Environment: "development",
			State: state.Apply.Running, Engine: "spirit",
		})},
	}
	assert.Equal(t, []string{"apply-bbbbbbbbbbbbbbbb", "apply-cccccccccccccccc", "apply-dddddddddddddddd"}, inFlightApplies(comments))
}

// renderSummary renders the summary comment SchemaBot posts when an apply
// reaches applyState.
func renderSummary(applyID, applyState string) string {
	return templates.RenderApplySummaryComment(templates.ApplyStatusCommentData{
		ApplyID:     applyID,
		Database:    "testdb",
		Environment: "development",
		RequestedBy: "octocat",
		State:       applyState,
		Engine:      "spirit",
	})
}

func TestParseFinalSummaryReadsTheRenderedTemplate(t *testing.T) {
	cases := map[string]string{
		state.Apply.Completed: StatusApplied,
		state.Apply.Failed:    StatusFailed,
		state.Apply.Cancelled: StatusCancelled,
		state.Apply.Reverted:  StatusReverted,
	}
	for applyState, want := range cases {
		t.Run(applyState, func(t *testing.T) {
			got, ok := parseFinalSummary(renderSummary("apply-0123456789abcdef", applyState))
			require.True(t, ok, "the summary comment for %s should parse", applyState)
			assert.Equal(t, ApplyStatus{ApplyID: "apply-0123456789abcdef", Status: want}, got)
		})
	}
}

// renderRollback renders the comment SchemaBot posts for a rollback apply in
// applyState: the status comment, or the summary when summary is set.
func renderRollback(applyState string, summary bool) string {
	data := templates.ApplyStatusCommentData{
		ApplyID:     "apply-0123456789abcdef",
		Database:    "testdb",
		Environment: "development",
		RequestedBy: "octocat",
		State:       applyState,
		Engine:      "spirit",
		Rollback:    true,
	}
	if summary {
		return templates.RenderApplySummaryComment(data)
	}
	return templates.RenderApplyStatusComment(data)
}

func TestParseApplyStatusReadsARollbackStatusComment(t *testing.T) {
	cases := map[string]string{
		state.Apply.Running:   StatusInProgress,
		state.Apply.Stopped:   StatusStopped,
		state.Apply.Completed: StatusRolledBack,
	}
	for applyState, want := range cases {
		t.Run(applyState, func(t *testing.T) {
			got, ok := ParseApplyStatus(renderRollback(applyState, false))
			require.True(t, ok, "the rollback status comment for %s should parse", applyState)
			assert.Equal(t, ApplyStatus{ApplyID: "apply-0123456789abcdef", Status: want}, got)
		})
	}
	assert.True(t, isTerminalStatus(StatusRolledBack))
}

func TestParseFinalSummaryReadsARollbackSummary(t *testing.T) {
	cases := map[string]string{
		state.Apply.Completed: StatusRolledBack,
		state.Apply.Failed:    StatusFailed,
		state.Apply.Cancelled: StatusCancelled,
	}
	for applyState, want := range cases {
		t.Run(applyState, func(t *testing.T) {
			got, ok := parseFinalSummary(renderRollback(applyState, true))
			require.True(t, ok, "the rollback summary for %s should parse", applyState)
			assert.Equal(t, ApplyStatus{ApplyID: "apply-0123456789abcdef", Status: want}, got)
		})
	}
	_, ok := parseFinalSummary(renderRollback(state.Apply.Stopped, true))
	assert.False(t, ok, "a start can resume a stopped rollback, so its summary is not final")
}

func TestParseApplyStatusCutsTheRevertWindowCountdown(t *testing.T) {
	body := templates.RenderApplyStatusComment(templates.ApplyStatusCommentData{
		ApplyID:         "apply-0123456789abcdef",
		Database:        "testdb",
		Environment:     "development",
		RequestedBy:     "octocat",
		State:           state.Apply.RevertWindow,
		Engine:          "spirit",
		RevertExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	})
	require.Contains(t, body, " | Closes in", "the rendered status line carries the countdown")
	got, ok := ParseApplyStatus(body)
	require.True(t, ok)
	assert.Equal(t, ApplyStatus{ApplyID: "apply-0123456789abcdef", Status: "Revert Window"}, got)
}

func TestParseApplyStatusTakesAnOpaqueApplyID(t *testing.T) {
	got, ok := ParseApplyStatus(renderStatus("apply-2026_Nov-05.a1", state.Apply.Running))
	require.True(t, ok)
	assert.Equal(t, "apply-2026_Nov-05.a1", got.ApplyID)
	summary, ok := parseFinalSummary(renderSummary("apply-2026_Nov-05.a1", state.Apply.Completed))
	require.True(t, ok)
	assert.Equal(t, "apply-2026_Nov-05.a1", summary.ApplyID)
}

func TestParseFinalSummaryIgnoresAStoppedSummary(t *testing.T) {
	_, ok := parseFinalSummary(renderSummary("apply-0123456789abcdef", state.Apply.Stopped))
	assert.False(t, ok, "a start can resume a stopped apply, so its summary is not final")
	_, ok = parseFinalSummary(renderStatus("apply-0123456789abcdef", state.Apply.Completed))
	assert.False(t, ok, "a status comment is not a summary")
}

func TestFinishedStatusReadsASummaryWhenTheStatusCommentLags(t *testing.T) {
	comments := []Comment{
		{ID: 1, Body: renderStatus("apply-aaaaaaaaaaaaaaaa", state.Apply.Stopped)},
		{ID: 2, Body: renderSummary("apply-aaaaaaaaaaaaaaaa", state.Apply.Completed)},
	}
	got, ok := finishedStatus(comments, "apply-aaaaaaaaaaaaaaaa")
	require.True(t, ok)
	assert.Equal(t, StatusApplied, got)
	assert.Empty(t, inFlightApplies(comments))
}

// renderAlreadyFinished renders SchemaBot's refusal of a cancel for an apply
// that finished in applyState.
func renderAlreadyFinished(applyState string) string {
	return templates.RenderGenericError(templates.SchemaErrorData{
		CommandName: "cancel",
		Environment: "development",
		RequestedBy: "octocat",
		ErrorDetail: "schema change is already terminal (current state: " + applyState + ")",
	})
}

// renderStartRefused renders SchemaBot's refusal of a start for an apply that
// finished in applyState, which words each state differently.
func renderStartRefused(detail string) string {
	return templates.RenderGenericError(templates.SchemaErrorData{
		CommandName: "start",
		Environment: "development",
		RequestedBy: "octocat",
		ErrorDetail: detail,
	})
}

func TestRefusedAsFinishedReadsAStartRefusal(t *testing.T) {
	cases := map[string]string{
		"schema change already completed and cannot be started":                                         state.Apply.Completed,
		"schema change failed and cannot be started":                                                    state.Apply.Failed,
		"schema change was cancelled and cannot be started; open a new schema change to apply it again": state.Apply.Cancelled,
		"schema change was reverted and cannot be started":                                              state.Apply.Reverted,
	}
	for detail, want := range cases {
		t.Run(want, func(t *testing.T) {
			got, ok := refusedAsFinished([]Comment{{ID: 5, Body: renderStartRefused(detail)}}, 4)
			require.True(t, ok)
			assert.Equal(t, want, got)
		})
	}
}

func TestRefusedAsFinishedReadsTheRenderedRefusal(t *testing.T) {
	comments := []Comment{{ID: 5, Body: renderAlreadyFinished(state.Apply.Completed)}}
	got, ok := refusedAsFinished(comments, 4)
	require.True(t, ok)
	assert.Equal(t, state.Apply.Completed, got)
	_, ok = refusedAsFinished(comments, 5)
	assert.False(t, ok, "a refusal of an earlier command does not count")
}

func TestUnansweredApplyFindsAnApplyCommandWithNoReply(t *testing.T) {
	isBot := func(c Comment) bool { return c.IsBot }
	earlier := Comment{ID: 1, IsBot: true, Body: renderStatus("apply-aaaaaaaaaaaaaaaa", state.Apply.Cancelled)}
	command := Comment{ID: 2, Body: "schemabot apply --defer-cutover -e development"}

	assert.True(t, unansweredApply([]Comment{earlier, command}, isBot, "schemabot"))
	assert.False(t, unansweredApply([]Comment{earlier, command}, isBot, "otherbot"),
		"a command with another prefix is not an apply command")
	lateSummary := Comment{ID: 3, IsBot: true, Body: renderSummary("apply-aaaaaaaaaaaaaaaa", state.Apply.Cancelled)}
	assert.True(t, unansweredApply([]Comment{earlier, command, lateSummary}, isBot, "schemabot"),
		"a late summary of an earlier apply does not answer the command")

	started := Comment{ID: 4, IsBot: true, Body: renderStatus("apply-bbbbbbbbbbbbbbbb", state.Apply.WaitingForCutover)}
	assert.False(t, unansweredApply([]Comment{earlier, command, started}, isBot, "schemabot"))
	unrelated := Comment{ID: 4, IsBot: true, Body: renderAlreadyFinished(state.Apply.Completed)}
	assert.True(t, unansweredApply([]Comment{earlier, command, unrelated}, isBot, "schemabot"),
		"a reply that neither starts an apply nor refuses this one does not answer it")
	blocked := Comment{ID: 4, IsBot: true, Body: templates.RenderApplyBlockedByNonPassingChecks("development", []templates.BlockingCheck{{Name: "lint", State: "failure"}})}
	assert.False(t, unansweredApply([]Comment{earlier, command, blocked}, isBot, "schemabot"),
		"a refusal before the engine answers the command")
	closed := Comment{ID: 4, IsBot: true, Body: templates.RenderApplyBlockedClosedPR("development", "octocat", false)}
	assert.False(t, unansweredApply([]Comment{earlier, command, closed}, isBot, "schemabot"))
	assert.False(t, unansweredApply([]Comment{earlier}, isBot, "schemabot"), "no apply command, nothing unanswered")
}

// renderApplyRejections renders every reply SchemaBot posts to refuse an apply
// command before it reaches the engine.
func renderApplyRejections() map[string]string {
	schemaErr := templates.SchemaErrorData{
		CommandName:  "apply",
		Environment:  "development",
		RequestedBy:  "octocat",
		DatabaseName: "testdb",
	}
	unregistered := schemaErr
	unregistered.UnregisteredConfigs = []templates.UnregisteredSchemaConfigData{
		{Database: "testdb", SchemaPath: "schema/testdb"},
		{Database: "otherdb", SchemaPath: "schema/otherdb"},
	}
	plan := schemaErr
	plan.CommandName = "plan"
	lock := templates.ApplyLockConflictData{
		Database:    "testdb",
		Environment: "development",
		RequestedBy: "octocat",
		LockOwner:   "hubot",
		LockRepo:    "octo/other",
		LockPR:      3,
		LockCreated: time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC),
	}
	actor := templates.ActorAuthorizationCommentData{RequestedBy: "octocat", CommandName: "apply", Database: "testdb", Environment: "development"}
	review := templates.ReviewGateData{Database: "testdb", Environment: "development", RequestedBy: "octocat"}
	failing := []templates.BlockingCheck{{Name: "lint", State: "failure"}}
	return map[string]string{
		"lock held by another PR":         templates.RenderApplyBlockedByOtherPR(lock),
		"apply already in progress":       templates.RenderApplyInProgress(lock),
		"closed PR":                       templates.RenderApplyBlockedClosedPR("development", "octocat", false),
		"failing checks":                  templates.RenderApplyBlockedByNonPassingChecks("development", failing),
		"pending checks":                  templates.RenderApplyBlockedByInProgressChecks("development", failing, nil),
		"check status error":              templates.RenderApplyBlockedByCheckStatusError("development", errors.New("unavailable"), nil),
		"prior environment":               templates.RenderApplyBlockedByPriorEnv("testdb", "production", "staging", "pending", "apply"),
		"prior environment check error":   templates.RenderApplyBlockedByPriorEnvCheckError("staging", "unavailable"),
		"prior environment check missing": templates.RenderApplyBlockedByMissingPriorEnvCheck("staging"),
		"prior environment check stale":   templates.RenderApplyBlockedByStalePriorEnvCheck("staging", "aaaaaaa", "bbbbbbb"),
		"prior environment untrusted":     templates.RenderApplyBlockedByUntrustedPriorEnvCheck("staging", "SchemaBot (staging)", []string{"other-app"}),
		"prior environment in progress":   templates.RenderApplyBlockedByPriorEnvInProgress("testdb", "production", "staging"),
		"prior environment stopped":       templates.RenderApplyBlockedByStoppedPriorEnvApply("testdb", "production", "staging"),
		"unlisted environment":            templates.RenderApplyBlockedByUnlistedEnvironment("qa", []string{"staging", "production"}),
		"stale schema":                    templates.RenderStaleSchemaRejection(templates.StaleSchemaRejectionData{RequestedBy: "octocat", Database: "testdb", Environment: "development", DiscoverySHA: "aaaaaaa", CurrentSHA: "bbbbbbb", Action: "apply"}),
		"newer base schema":               templates.RenderBaseSchemaFreshnessRejection(templates.BaseSchemaFreshnessRejectionData{RequestedBy: "octocat", Database: "testdb", Environment: "development", SchemaPath: "schema/testdb"}),
		"review required":                 templates.RenderReviewRequired(review),
		"review gate error":               templates.RenderReviewGateError(review),
		"command not authorized":          templates.RenderPRCommandNotAuthorized(actor),
		"authorization database":          templates.RenderPRCommandDatabaseNotConfigured(actor),
		"authorization unavailable":       templates.RenderPRCommandAuthorizationUnavailable(actor),
		"database not found":              templates.RenderDatabaseNotFound(schemaErr),
		"database not configured":         templates.RenderDatabaseNotConfigured(schemaErr),
		"database not registered":         templates.RenderDatabaseNotRegistered(schemaErr),
		"databases not registered":        templates.RenderDatabaseNotRegistered(unregistered),
		"database repository not allowed": templates.RenderDatabaseRepoNotAllowed(schemaErr),
		"no schema files changed":         templates.RenderNoManagedSchemaChanges(schemaErr),
		"invalid environment":             templates.RenderInvalidEnv("apply", []string{"staging"}),
		"missing environment":             templates.RenderMissingEnv("apply"),
	}
}

func TestIsApplyRejectionReadsEveryRenderedRefusal(t *testing.T) {
	bodies := renderApplyRejections()
	for name, body := range bodies {
		assert.True(t, isApplyRejection(body), "%s should read as a refusal: %q", name, firstLine(body))
	}
	for _, entry := range applyRejections {
		matched := false
		for _, body := range bodies {
			if strings.Contains(firstLine(body), entry) {
				matched = true
				break
			}
		}
		assert.True(t, matched, "no rendered refusal has the headline %q", entry)
	}
}

func TestIsApplyRejectionSkipsAFailureAfterTheEngine(t *testing.T) {
	failed := templates.RenderGenericError(templates.SchemaErrorData{
		CommandName: "apply",
		Environment: "development",
		RequestedBy: "octocat",
		ErrorDetail: "the apply was not accepted",
	})
	assert.False(t, isApplyRejection(failed), "SchemaBot can post Apply Failed after the engine accepted the apply")
	assert.False(t, isApplyRejection(renderStatus("apply-0123456789abcdef", state.Apply.Running)))
}
