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
			Operation:    storage.ControlOperationVolume,
			Status:       storage.ControlRequestFailed,
			RequestedBy:  "octocat",
			ErrorMessage: "throttle deploy request 42 failed: 404 Not Found",
		}).formatStatusComment(apply, nil)

		assert.Contains(t, body, "Command not applied")
		assert.Contains(t, body, "`volume` was accepted but did not take effect")
		assert.Contains(t, body, "requested by `octocat`")
		assert.Contains(t, body, "404 Not Found")
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

	// Volume is proxied entirely through SchemaBot's own forwarding path, so its
	// stored request never names a person. Crediting the command to the internal
	// caller would tell the operator someone else issued it.
	t.Run("a request that names no operator is not attributed", func(t *testing.T) {
		body := observer(&storage.ApplyControlRequest{
			Operation:    storage.ControlOperationVolume,
			Status:       storage.ControlRequestFailed,
			RequestedBy:  storage.ForwardingControlRequestCaller,
			ErrorMessage: "engine does not support volume changes",
		}).formatStatusComment(apply, nil)

		assert.Contains(t, body, "`volume` was accepted but did not take effect")
		assert.Contains(t, body, "engine does not support volume changes")
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
