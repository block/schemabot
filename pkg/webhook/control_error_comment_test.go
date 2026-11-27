package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// controlOperationApplyStore serves the apply a control command loads. The
// first read is the webhook handler's own check of the apply; every later read
// is the control operation's, and fails with loadErr when one is set.
type controlOperationApplyStore struct {
	storage.ApplyStore
	apply   *storage.Apply
	loadErr error
	reads   atomic.Int32
}

func (s *controlOperationApplyStore) GetByApplyIdentifier(context.Context, string) (*storage.Apply, error) {
	if s.reads.Add(1) > 1 && s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.apply, nil
}

// controlStubTernClient stands in for an engine client that a refused control
// operation never calls.
type controlStubTernClient struct {
	tern.Client
}

func controlOperationTestApply(engine string) *storage.Apply {
	return &storage.Apply{
		ApplyIdentifier: "apply-123",
		Database:        "orders",
		Environment:     "staging",
		Deployment:      api.DefaultDeployment,
		Engine:          engine,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
	}
}

// A control command whose operation fails inside SchemaBot tells the PR which
// command did not complete and what to do next. The failure's own text, which
// here names an internal address, stays in the server log with the identifiers
// an operator needs to find it.
func TestControlCommandInternalFailureKeepsErrorOutOfComment(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	st := &planControlStorage{applies: &controlOperationApplyStore{
		apply:   controlOperationTestApply(storage.EngineSpirit),
		loadErr: errors.New(rawInfrastructureError),
	}}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
	var logs bytes.Buffer
	h.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	h.handleStopCommand("octocat/hello-world", 1, 12345, "testuser", planControlCommandResult())

	body := requireComment(t, comments, "control operation failure comment")
	assert.Contains(t, body, controlFailureLine("stop"))
	requireNoInfrastructureDetail(t, body)
	assert.NotContains(t, body, "10.1.2.3")
	assert.NotContains(t, body, "failed to look up apply")
	logged := logs.String()
	assert.Contains(t, logged, "stop PR command failed")
	assert.Contains(t, logged, rawInfrastructureError)
	assert.Contains(t, logged, `"apply_id":"apply-123"`)
	assert.Contains(t, logged, `"repo":"octocat/hello-world"`)
	assert.Contains(t, logged, `"pr":1`)
	assert.Contains(t, logged, `"environment":"staging"`)
}

// A control command the operation refuses with text SchemaBot wrote shows that
// text, because it carries the next step the requester can take.
func TestControlCommandAuthoredRefusalShowsItsText(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	st := &planControlStorage{applies: &controlOperationApplyStore{
		apply: controlOperationTestApply(storage.EnginePlanetScale),
	}}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
	h.service.RegisterTernClient(api.DefaultDeployment, "staging", &controlStubTernClient{})

	h.handleStopCommand("octocat/hello-world", 1, 12345, "testuser", planControlCommandResult())

	body := requireComment(t, comments, "control operation refusal comment")
	assert.Contains(t, body, "stop is not supported for this schema change; use cancel to permanently cancel it")
	assert.NotContains(t, body, "SchemaBot could not complete the stop command")
}

// A control command's failure line keeps an untyped failure's text, and any
// wrap around it, out of the comment.
func TestControlOperationFailureMessage(t *testing.T) {
	t.Run("an untyped failure asks to check the apply before another attempt", func(t *testing.T) {
		got := controlOperationFailureMessage(errors.New(rawInfrastructureError), "cutover", "apply-123", "staging")

		assert.Equal(t, controlFailureLine("cutover"), got)
	})

	t.Run("a wrapped untyped failure keeps the wrap out of the line", func(t *testing.T) {
		wrapped := fmt.Errorf("failed to look up apply %q: %w", "apply-123", errors.New(rawInfrastructureError))

		got := controlOperationFailureMessage(wrapped, "skip-revert", "apply-123", "staging")

		assert.NotContains(t, got, "10.1.2.3")
		assert.Equal(t, controlFailureLine("skip-revert"), got)
	})

	t.Run("a remote data plane that did not answer is named as such", func(t *testing.T) {
		err := fmt.Errorf("check remote apply apply-123 before cutover: %w",
			grpcstatus.Error(grpccodes.Unavailable, rawInfrastructureError))

		got := controlOperationFailureMessage(err, "cutover", "apply-123", "staging")

		assert.Equal(t, userFacingRemoteUnavailableError("", ""), got)
		requireNoInfrastructureDetail(t, got)
	})
}

// controlFailureLine is the PR comment line for a control command whose
// operation failed inside SchemaBot on apply-123.
func controlFailureLine(command string) string {
	return "SchemaBot could not complete the " + command + " command for apply `apply-123`. " +
		"It may already be recorded, so check the progress comment for this apply before running the command again; " +
		"the SchemaBot server logs have the cause."
}

// Each control command whose operation fails inside SchemaBot names itself in
// the fixed line and keeps the failure's own text out of the PR comment.
func TestEveryControlCommandNamesItselfInFailureLine(t *testing.T) {
	const repo, pr, installation, requester = "octocat/hello-world", 1, int64(12345), "testuser"
	commands := map[string]func(h *Handler, r CommandResult){
		"stop":        func(h *Handler, r CommandResult) { h.handleStopCommand(repo, pr, installation, requester, r) },
		"cancel":      func(h *Handler, r CommandResult) { h.handleCancelCommand(repo, pr, installation, requester, r) },
		"start":       func(h *Handler, r CommandResult) { h.handleStartCommand(repo, pr, installation, requester, r) },
		"release":     func(h *Handler, r CommandResult) { h.handleReleaseCommand(repo, pr, installation, requester, r) },
		"cutover":     func(h *Handler, r CommandResult) { h.handleCutoverCommand(repo, pr, installation, requester, r) },
		"skip-revert": func(h *Handler, r CommandResult) { h.handleSkipRevertCommand(repo, pr, installation, requester, r) },
		"revert":      func(h *Handler, r CommandResult) { h.handleRevertCommand(repo, pr, installation, requester, r) },
	}
	for name, run := range commands {
		t.Run(name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			comments := recordComments(t, mux)
			st := &planControlStorage{applies: &controlOperationApplyStore{
				apply:   controlOperationTestApply(storage.EngineSpirit),
				loadErr: errors.New(rawInfrastructureError),
			}}
			h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

			run(h, planControlCommandResult())

			body := requireComment(t, comments, name+" failure comment")
			assert.Contains(t, body, controlFailureLine(name))
			requireNoInfrastructureDetail(t, body)
		})
	}
}

// A control command on an apply with no deployment recorded says that
// running it again fails the same way, rather than asking for another attempt.
func TestControlCommandOnApplyWithoutDeploymentSaysRetryWillNotHelp(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	apply := controlOperationTestApply(storage.EngineSpirit)
	apply.Deployment = ""
	st := &planControlStorage{applies: &controlOperationApplyStore{apply: apply}}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

	h.handleCutoverCommand("octocat/hello-world", 1, 12345, "testuser", planControlCommandResult())

	body := requireComment(t, comments, "missing deployment comment")
	assert.Contains(t, body, "The cutover was not attempted for apply")
	assert.Contains(t, body, "because SchemaBot has no deployment recorded for it, so nothing was changed.")
	assert.Contains(t, body, "Sending it again will fail the same way.")
	assert.NotContains(t, body, "missing stored deployment metadata")
	assert.NotContains(t, body, "running the command again")
}

// controlOperationListStore serves the operations a refused PostgreSQL stop
// reads to name its remedy, or fails with listErr when one is set.
type controlOperationListStore struct {
	storage.ApplyOperationStore
	listErr error
}

func (s *controlOperationListStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return nil, nil
}

// planControlOperationStorage is planControlStorage with the apply's
// operations.
type planControlOperationStorage struct {
	planControlStorage
	operations storage.ApplyOperationStore
}

func (s *planControlOperationStorage) ApplyOperations() storage.ApplyOperationStore {
	return s.operations
}

// An operator comments stop on a running PostgreSQL apply. PostgreSQL can't
// pause it, so the comment says so and names what to do in the PR's own
// terms: the cancel comment for a lone index build, the apply's progress
// comment for anything that finishes on its own, and both when SchemaBot
// cannot read which kind of statement is running. The CLI's commands, which
// the server's refusal names, never reach the comment.
func TestStopCommandOnPostgresNamesTheRemedyInCommentTerms(t *testing.T) {
	tests := []struct {
		name    string
		listErr error
		want    string
	}{
		{
			name: "a schema change that finishes on its own points at its progress comment",
			want: "This schema change can't be paused, and it finishes on its own; the progress comment for apply `apply-123` follows it to the end.",
		},
		{
			name:    "an unreadable statement kind names both remedies",
			listErr: errors.New(rawInfrastructureError),
			want: "This schema change can't be paused. If it is a single index build, end it by commenting `schemabot cancel apply-123 -e staging`. " +
				"Anything else finishes on its own, and the progress comment for apply `apply-123` follows it to the end.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			comments := recordComments(t, mux)
			st := &planControlOperationStorage{
				planControlStorage: planControlStorage{applies: &controlOperationApplyStore{
					apply: controlOperationTestApply(storage.EnginePostgres),
				}},
				operations: &controlOperationListStore{listErr: tt.listErr},
			}
			h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
			h.service.RegisterTernClient(api.DefaultDeployment, "staging", &controlStubTernClient{})

			h.handleStopCommand("octocat/hello-world", 1, 12345, "testuser", planControlCommandResult())

			body := requireComment(t, comments, "refused PostgreSQL stop comment")
			assert.Contains(t, html.UnescapeString(body), tt.want)
			assert.NotContains(t, body, "schemabot progress")
			assert.NotContains(t, body, "--no-watch")
			requireNoInfrastructureDetail(t, body)
		})
	}
}

// Each stop refusal code names its remedy as a PR comment command or as the
// apply's progress comment, and any other code leaves the line to the
// refusal's own text.
func TestStopUnsupportedMessage(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{
			code: apitypes.ErrCodeStopUnsupportedCancel,
			want: "This schema change can't be paused. To end it, comment `schemabot cancel apply-123 -e staging`.",
		},
		{
			code: apitypes.ErrCodeStopUnsupportedFinishes,
			want: "This schema change can't be paused, and it finishes on its own; the progress comment for apply `apply-123` follows it to the end.",
		},
		{
			code: apitypes.ErrCodeStopUnsupported,
			want: "This schema change can't be paused. If it is a single index build, end it by commenting `schemabot cancel apply-123 -e staging`. " +
				"Anything else finishes on its own, and the progress comment for apply `apply-123` follows it to the end.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			got, ok := stopUnsupportedMessage(tt.code, "apply-123", "staging")
			assert.True(t, ok)
			assert.Equal(t, tt.want, got)
		})
	}

	got, ok := stopUnsupportedMessage(apitypes.ErrCodeInvalidRequest, "apply-123", "staging")
	assert.False(t, ok, "a code outside the stop refusals is left to the refusal's text")
	assert.Empty(t, got)
}
