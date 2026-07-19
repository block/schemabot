package templates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// A progress comment re-stamps its rendered timestamps ("Last updated" footer,
// attribution line) on every render, so raw bodies from two renders of the same
// status never compare equal. The content key collapses those timestamps: equal
// keys mean an edit would change nothing else the reader can see.
func TestCommentContentKeyEqualForUnchangedStatus(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		ApplyID:     "apply_123",
		RequestedBy: "octocat",
		State:       state.Apply.Running,
		Tables: []TableProgressData{
			{TableName: "users", Status: state.Task.Running, PercentComplete: 50, RowsCopied: 500, RowsTotal: 1000},
		},
	}

	withTemplateTimestamp(t, "2026-06-16 19:42:00 UTC")
	first := RenderApplyStatusComment(data)
	withTemplateTimestamp(t, "2026-06-16 19:42:30 UTC")
	second := RenderApplyStatusComment(data)

	assert.NotEqual(t, first, second, "renders at different times differ in their stamped timestamps")
	assert.Equal(t, CommentContentKey(first), CommentContentKey(second))
	assert.Contains(t, CommentContentKey(first), "50%", "the key keeps the progress content")
	assert.NotContains(t, CommentContentKey(first), "19:42:00", "the key collapses rendered timestamps")
}

// Real progress changes must always change the content key, so the observer
// keeps editing whenever the reader would see something new.
func TestCommentContentKeyChangesWithProgress(t *testing.T) {
	withTemplateTimestamp(t, "2026-06-16 19:42:00 UTC")
	render := func(percent int, rows int64) string {
		return RenderApplyStatusComment(ApplyStatusCommentData{
			Database:    "testapp",
			Environment: "staging",
			RequestedBy: "octocat",
			State:       state.Apply.Running,
			Tables: []TableProgressData{
				{TableName: "users", Status: state.Task.Running, PercentComplete: percent, RowsCopied: rows, RowsTotal: 1000},
			},
		})
	}

	assert.NotEqual(t, CommentContentKey(render(50, 500)), CommentContentKey(render(70, 700)))
}

// The revert-window countdown is a duration, not a timestamp, so it survives
// into the content key: two renders of an otherwise-unchanged revert window
// have different keys and the comment keeps being edited as the window closes.
func TestCommentContentKeyKeepsRevertCountdownLive(t *testing.T) {
	withTemplateTimestamp(t, "2026-06-16 19:42:00 UTC")
	expires := time.Date(2026, 6, 16, 20, 0, 0, 0, time.UTC)
	data := ApplyStatusCommentData{
		Database:        "testapp",
		Environment:     "staging",
		RequestedBy:     "octocat",
		State:           state.Apply.RevertWindow,
		RevertExpiresAt: expires.Format(time.RFC3339),
	}

	originalNow := NowFunc
	t.Cleanup(func() { NowFunc = originalNow })

	NowFunc = func() time.Time { return expires.Add(-20 * time.Minute) }
	first := RenderApplyStatusComment(data)
	NowFunc = func() time.Time { return expires.Add(-10 * time.Minute) }
	second := RenderApplyStatusComment(data)

	assert.Contains(t, first, "Closes in 20m")
	assert.Contains(t, second, "Closes in 10m")
	assert.NotEqual(t, CommentContentKey(first), CommentContentKey(second))
}

// A progress comment states its own refresh cadence beside the "Last updated"
// footer, so a reader watching a long apply knows how far apart updates are
// expected instead of reading a slowed cadence as a stall. Zero cadence
// (terminal finalizations and one-shot renders) omits the note entirely.
func TestLastUpdatedFooterShowsUpdateCadence(t *testing.T) {
	withTemplateTimestamp(t, "2026-06-16 19:42:00 UTC")
	data := ApplyStatusCommentData{
		Database:      "testapp",
		Environment:   "staging",
		RequestedBy:   "octocat",
		State:         state.Apply.Running,
		UpdateCadence: 2 * time.Minute,
	}

	out := RenderApplyStatusComment(data)
	assert.Contains(t, out, "(2026-06-16 19:42:00 UTC) · updates every ~2m_")

	data.UpdateCadence = 0
	out = RenderApplyStatusComment(data)
	assert.Contains(t, out, "(2026-06-16 19:42:00 UTC)_")
	assert.NotContains(t, out, "updates every")
}

// The cadence note is part of the visible content, so a cadence change alone
// changes the content key: when the decay schedule moves to a slower rung, the
// comment is edited once and its advertised cadence stays truthful.
func TestCommentContentKeyChangesWithUpdateCadence(t *testing.T) {
	withTemplateTimestamp(t, "2026-06-16 19:42:00 UTC")
	render := func(cadence time.Duration) string {
		return RenderApplyStatusComment(ApplyStatusCommentData{
			Database:      "testapp",
			Environment:   "staging",
			RequestedBy:   "octocat",
			State:         state.Apply.Running,
			UpdateCadence: cadence,
		})
	}

	assert.NotEqual(t, CommentContentKey(render(30*time.Second)), CommentContentKey(render(time.Minute)))
}
