package livee2e

import (
	"testing"

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
		state.Apply.Reverted:  "Reverted",
	}
	for applyState, want := range cases {
		t.Run(applyState, func(t *testing.T) {
			got, ok := parseFinalSummary(renderSummary("apply-0123456789abcdef", applyState))
			require.True(t, ok, "the summary comment for %s should parse", applyState)
			assert.Equal(t, ApplyStatus{ApplyID: "apply-0123456789abcdef", Status: want}, got)
		})
	}
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
	refused := Comment{ID: 4, IsBot: true, Body: renderAlreadyFinished(state.Apply.Completed)}
	assert.False(t, unansweredApply([]Comment{earlier, command, refused}, isBot, "schemabot"),
		"any other reply answers the command")
	assert.False(t, unansweredApply([]Comment{earlier}, isBot, "schemabot"), "no apply command, nothing unanswered")
}
