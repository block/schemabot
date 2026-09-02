package webhook

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// An operator who issues a control command sees only that SchemaBot accepted it.
// Acceptance means the command was queued, so when the driver — local, or remote
// and mirrored back — fails it, the PR comment must say so; otherwise the
// operator is left watching an apply that never obeys the command.
func TestStatusCommentSurfacesRejectedControlCommands(t *testing.T) {
	apply := &storage.Apply{
		ApplyIdentifier: "apply-1", Database: "commerce", Environment: "production",
		State: state.Apply.Running, Engine: storage.EnginePlanetScale,
	}
	observer := func(settled ...*storage.ApplyControlRequest) *CommentObserver {
		return &CommentObserver{
			stor: &stubStorage{
				ops:     &stubApplyOperationStore{ops: []*storage.ApplyOperation{{ID: 7, Deployment: "eu", State: state.ApplyOperation.Running}}},
				settled: settled,
			},
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
	}

	t.Run("a failed control request is named with its reason", func(t *testing.T) {
		body := observer(&storage.ApplyControlRequest{
			Operation:    storage.ControlOperationRevert,
			Status:       storage.ControlRequestFailed,
			RequestedBy:  "octocat",
			ErrorMessage: "deploy request is outside its revert window",
		}).formatStatusComment(apply, nil)

		assert.Contains(t, body, "Command not applied")
		assert.Contains(t, body, "`revert` was accepted but did not take effect")
		assert.Contains(t, body, "requested by `octocat`")
		assert.Contains(t, body, "outside its revert window")
	})

	// A rejection recorded by a previous release for a retired operation names
	// a command the operator can no longer issue: the notice's "re-issue the
	// command" remedy is impossible, so nothing could ever clear it.
	t.Run("a rejection for a retired operation adds no notice", func(t *testing.T) {
		body := observer(&storage.ApplyControlRequest{
			Operation:    storage.ControlOperation("volume"),
			Status:       storage.ControlRequestFailed,
			RequestedBy:  "octocat",
			ErrorMessage: "the engine rejected the volume change",
		}).formatStatusComment(apply, nil)

		assert.NotContains(t, body, "Command not applied")
	})

	t.Run("a completed control request adds no notice", func(t *testing.T) {
		body := observer(&storage.ApplyControlRequest{
			Operation: storage.ControlOperationCutover,
			Status:    storage.ControlRequestCompleted,
		}).formatStatusComment(apply, nil)

		assert.NotContains(t, body, "Command not applied")
	})

	t.Run("no control commands leaves the comment unchanged", func(t *testing.T) {
		assert.NotContains(t, observer().formatStatusComment(apply, nil), "Command not applied")
	})

	// A command that reached this plane without an operator identity — an
	// internal resume, or a plane that predates the caller field — records
	// SchemaBot's own forwarding caller. Crediting the command to that internal
	// caller would tell the operator someone else issued it.
	t.Run("a request that names no operator is not attributed", func(t *testing.T) {
		body := observer(&storage.ApplyControlRequest{
			Operation:    storage.ControlOperationStart,
			Status:       storage.ControlRequestFailed,
			RequestedBy:  storage.ForwardingControlRequestCaller,
			ErrorMessage: "no stopped schema change to resume",
		}).formatStatusComment(apply, nil)

		assert.Contains(t, body, "`start` was accepted but did not take effect")
		assert.Contains(t, body, "no stopped schema change to resume")
		assert.NotContains(t, body, "requested by")
	})

	// The terminal summary is the last comment the operator reads, so a command
	// that never took effect has to survive into it — otherwise the rejection is
	// visible only on a progress comment the summary supersedes.
	t.Run("the terminal summary carries the notice", func(t *testing.T) {
		terminal := *apply
		terminal.State = state.Apply.Completed
		body := observer(&storage.ApplyControlRequest{
			Operation:    storage.ControlOperationCutover,
			Status:       storage.ControlRequestFailed,
			RequestedBy:  "octocat",
			ErrorMessage: "cutover rejected: apply already finished",
		}).summaryCommentFromOps(t.Context(), &terminal, nil, nil, nil, nil)

		assert.Contains(t, body, "Command not applied")
		assert.Contains(t, body, "`cutover` was accepted but did not take effect")
		assert.Contains(t, body, "requested by `octocat`")
		assert.Contains(t, body, "cutover rejected: apply already finished")
	})
}
