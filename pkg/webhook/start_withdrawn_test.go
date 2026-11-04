package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

const startWithdrawnPostDeadline = 5 * time.Second

// noticeStorage serves the apply and control request reads and the start
// request's notice updates that the withdrawal notice paths make.
type noticeStorage struct {
	storage.Storage
	apply    *storage.Apply
	controls *noticeControlRequests
}

func (s *noticeStorage) Applies() storage.ApplyStore { return noticeApplies{apply: s.apply} }

func (s *noticeStorage) ControlRequests() storage.ControlRequestStore { return s.controls }

type noticeApplies struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (a noticeApplies) GetByApplyIdentifier(_ context.Context, identifier string) (*storage.Apply, error) {
	if identifier != a.apply.ApplyIdentifier {
		return nil, nil
	}
	return a.apply, nil
}

// noticeControlRequests holds one request per operation and applies metadata
// updates under a lock, as the store's row lock does.
type noticeControlRequests struct {
	storage.ControlRequestStore
	mu   sync.Mutex
	rows map[storage.ControlOperation]*storage.ApplyControlRequest
}

func (c *noticeControlRequests) GetByOperation(_ context.Context, _ int64, operation storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	row, ok := c.rows[operation]
	if !ok {
		return nil, nil
	}
	cp := *row
	return &cp, nil
}

func (c *noticeControlRequests) UpdateMetadata(_ context.Context, _ int64, operation storage.ControlOperation, update func(*storage.ApplyControlRequest) ([]byte, error)) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	row, ok := c.rows[operation]
	if !ok {
		return false, nil
	}
	cp := *row
	metadata, err := update(&cp)
	if err != nil {
		return true, err
	}
	if metadata != nil {
		row.Metadata = metadata
	}
	return true, nil
}

// startNoticeState decodes the notice state recorded on the start request.
func (c *noticeControlRequests) startNoticeState(t *testing.T) startNotice {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(c.rows[storage.ControlOperationStart].Metadata, &fields))
	var notice startNotice
	require.NoError(t, json.Unmarshal(fields[startNoticeMetadataKey], &notice))
	return notice
}

// startWithdrawnHandler returns a handler over the apply whose start and stop
// requests are given, and a func that waits for the handler's background work
// and returns the comments posted on octocat/hello-world#7 so far. The handler
// keeps tracking background work after each wait, so a test can wait again.
func startWithdrawnHandler(t *testing.T, apply *storage.Apply, start, stop *storage.ApplyControlRequest) (*Handler, *noticeControlRequests, func() []string) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	var mu sync.Mutex
	var posted []string
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/7/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		mu.Lock()
		posted = append(posted, body.Body)
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 99}))
	})
	controls := &noticeControlRequests{rows: map[storage.ControlOperation]*storage.ApplyControlRequest{}}
	if start != nil {
		controls.rows[storage.ControlOperationStart] = start
	}
	if stop != nil {
		controls.rows[storage.ControlOperationStop] = stop
	}
	h := &Handler{
		service:   api.New(&noticeStorage{apply: apply, controls: controls}, &api.ServerConfig{}, nil, testLogger()),
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}
	return h, controls, func() []string {
		ctx, cancel := context.WithTimeout(t.Context(), startWithdrawnPostDeadline)
		defer cancel()
		h.DrainInProcessWebhookWork(ctx)
		require.NoError(t, ctx.Err(), "the withdrawal notice work did not finish")
		h.inProcessWebhookMu.Lock()
		h.inProcessWebhookDraining = false
		h.inProcessWebhookMu.Unlock()
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), posted...)
	}
}

func startWithdrawnApply() *storage.Apply {
	return &storage.Apply{
		ID:              11,
		ApplyIdentifier: "apply-a1b2c3d4e5f67890",
		Database:        "orders",
		Environment:     "production",
		Repository:      "octocat/hello-world",
		PullRequest:     7,
		InstallationID:  42,
	}
}

// withdrawnStart is a start request a stop completed by withdrawing it.
func withdrawnStart(requestedBy string) *storage.ApplyControlRequest {
	return &storage.ApplyControlRequest{
		ApplyID:     11,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestCompleted,
		RequestedBy: requestedBy,
		Metadata:    []byte(`{"started_count":1}`),
	}
}

func stopRequest(requestedBy string) *storage.ApplyControlRequest {
	return &storage.ApplyControlRequest{
		ApplyID:     11,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestCompleted,
		RequestedBy: requestedBy,
	}
}

func renderedWithdrawal(stoppedBy string) string {
	return templates.RenderStartWithdrawn(templates.StartWithdrawnData{
		ApplyID:     "apply-a1b2c3d4e5f67890",
		Environment: "production",
		StoppedBy:   stoppedBy,
	})
}

// An operator starts a stopped schema change from the CLI and then stops it
// before any driver acts on the start. The apply never leaves stopped, so no
// drive updates the PR; SchemaBot tells the PR the start was withdrawn. No
// acknowledgement is owed for a start that was not a command on the PR, so the
// notice posts at once.
func TestReportQueuedStartWithdrawnTellsThePR(t *testing.T) {
	h, controls, posted := startWithdrawnHandler(t, startWithdrawnApply(), withdrawnStart("cli:alice@laptop.local"), nil)

	h.reportQueuedStartWithdrawn(startWithdrawnApply(), "cli:bob@laptop.local")

	assert.Equal(t, []string{renderedWithdrawal("")}, posted(), "one comment, with no stopper named for a caller that is not a GitHub user")
	assert.Equal(t, startNotice{Withdrawn: true, Acknowledged: true, Notified: true}, controls.startNoticeState(t))
}

// A caller supplied by an API client is not trusted to be a well-formed
// GitHub caller. One whose username carries markdown is posted without
// attribution, so the stop cannot write its own heading into the PR comment.
func TestReportQueuedStartWithdrawnOmitsAMalformedStopper(t *testing.T) {
	h, _, posted := startWithdrawnHandler(t, startWithdrawnApply(), withdrawnStart("cli:alice@laptop.local"), nil)

	h.reportQueuedStartWithdrawn(startWithdrawnApply(), "github:alice\n\n## Forged status\ntext@other/repo#1")

	comments := posted()
	require.Len(t, comments, 1)
	assert.Equal(t, renderedWithdrawal(""), comments[0])
	assert.NotContains(t, comments[0], "Forged status")
}

// A stop from a GitHub user on another PR names them.
func TestReportQueuedStartWithdrawnNamesAGitHubStopper(t *testing.T) {
	h, _, posted := startWithdrawnHandler(t, startWithdrawnApply(), withdrawnStart("cli:alice@laptop.local"), nil)

	h.reportQueuedStartWithdrawn(startWithdrawnApply(), formatGitHubCaller("alice", "octocat/other", 3))

	comments := posted()
	require.Len(t, comments, 1)
	assert.Contains(t, comments[0], "**Stopped by**: @alice\n")
}

// A stop sent as a command on the same PR already gets a reply saying it
// withdrew the start, so the PR is not told a second time.
func TestReportQueuedStartWithdrawnSkipsAStopCommandOnThePR(t *testing.T) {
	h, _, posted := startWithdrawnHandler(t, startWithdrawnApply(), withdrawnStart("cli:alice@laptop.local"), nil)

	h.reportQueuedStartWithdrawn(startWithdrawnApply(), formatGitHubCaller("alice", "octocat/hello-world", 7))

	assert.Empty(t, posted())
}

// An apply with no PR, such as one started from the CLI, has nowhere to post.
func TestReportQueuedStartWithdrawnSkipsAnApplyWithoutAPR(t *testing.T) {
	apply := startWithdrawnApply()
	apply.Repository, apply.PullRequest, apply.InstallationID = "", 0, 0
	h, _, posted := startWithdrawnHandler(t, apply, withdrawnStart("cli:alice@laptop.local"), nil)

	h.reportQueuedStartWithdrawn(apply, "cli:bob@laptop.local")

	assert.Empty(t, posted())
}

// A start that a later start has already re-armed is no longer the one the
// stop withdrew; the later start's acknowledgement updates the PR.
func TestReportQueuedStartWithdrawnSkipsARearmedStart(t *testing.T) {
	start := withdrawnStart("cli:carol@laptop.local")
	start.Status = storage.ControlRequestPending
	h, _, posted := startWithdrawnHandler(t, startWithdrawnApply(), start, nil)

	h.reportQueuedStartWithdrawn(startWithdrawnApply(), "cli:bob@laptop.local")

	assert.Empty(t, posted())
}

// Alice starts the schema change from the PR, and Bob stops it from the CLI
// before Alice's acknowledgement has landed. The notice waits for the
// acknowledgement, so the PR reads acknowledgement, then withdrawal, and gets
// the notice once however often either side runs again.
func TestStartWithdrawnBeforeItsAcknowledgementPostsOnceAfterIt(t *testing.T) {
	aliceStart := formatGitHubCaller("alice", "octocat/hello-world", 7)
	h, controls, posted := startWithdrawnHandler(t, startWithdrawnApply(), withdrawnStart(aliceStart), stopRequest("github:bob@octocat/other#3"))

	h.reportQueuedStartWithdrawn(startWithdrawnApply(), "github:bob@octocat/other#3")
	assert.Empty(t, posted(), "the notice waits for the start's acknowledgement")

	h.recordStartAcknowledged(t.Context(), "octocat/hello-world", 7, 42, "apply-a1b2c3d4e5f67890", "production", aliceStart)
	h.reportQueuedStartWithdrawn(startWithdrawnApply(), "github:bob@octocat/other#3")
	h.recordStartAcknowledged(t.Context(), "octocat/hello-world", 7, 42, "apply-a1b2c3d4e5f67890", "production", aliceStart)

	assert.Equal(t, []string{renderedWithdrawal("bob")}, posted(), "one notice, posted after the acknowledgement, naming the stopper")
	assert.Equal(t, startNotice{Withdrawn: true, Acknowledged: true, Notified: true}, controls.startNoticeState(t))
}

// Alice's acknowledgement lands first and Bob's stop withdraws the start after
// it. The stop posts the notice, and a repeated acknowledgement does not post
// it again.
func TestStartWithdrawnAfterItsAcknowledgementPostsOnce(t *testing.T) {
	aliceStart := formatGitHubCaller("alice", "octocat/hello-world", 7)
	start := withdrawnStart(aliceStart)
	start.Status = storage.ControlRequestPending
	h, controls, posted := startWithdrawnHandler(t, startWithdrawnApply(), start, nil)

	h.recordStartAcknowledged(t.Context(), "octocat/hello-world", 7, 42, "apply-a1b2c3d4e5f67890", "production", aliceStart)
	assert.Empty(t, posted(), "no stop has withdrawn the start yet")

	controls.mu.Lock()
	start.Status = storage.ControlRequestCompleted
	controls.mu.Unlock()
	h.reportQueuedStartWithdrawn(startWithdrawnApply(), "cli:bob@laptop.local")
	h.recordStartAcknowledged(t.Context(), "octocat/hello-world", 7, 42, "apply-a1b2c3d4e5f67890", "production", aliceStart)

	assert.Equal(t, []string{renderedWithdrawal("")}, posted())
	assert.Equal(t, startNotice{Withdrawn: true, Acknowledged: true, Notified: true}, controls.startNoticeState(t))
}

// A driver ran Alice's start and a later stop settled the apply back at
// stopped, all before her acknowledgement landed. Only a withdrawal records
// itself on the start request, so the PR is not told the start was withdrawn.
func TestStartADriverRanIsNotReportedAsWithdrawn(t *testing.T) {
	aliceStart := formatGitHubCaller("alice", "octocat/hello-world", 7)
	h, controls, posted := startWithdrawnHandler(t, startWithdrawnApply(), withdrawnStart(aliceStart), stopRequest("cli:bob@laptop.local"))

	h.recordStartAcknowledged(t.Context(), "octocat/hello-world", 7, 42, "apply-a1b2c3d4e5f67890", "production", aliceStart)

	assert.Empty(t, posted())
	assert.Equal(t, startNotice{Acknowledged: true}, controls.startNoticeState(t))
}

// The start request already belongs to Carol's later start, so Alice's
// acknowledgement does not mark it.
func TestStartAcknowledgementLeavesAnotherCallersStart(t *testing.T) {
	carolStart := withdrawnStart(formatGitHubCaller("carol", "octocat/hello-world", 7))
	carolStart.Status = storage.ControlRequestPending
	h, controls, posted := startWithdrawnHandler(t, startWithdrawnApply(), carolStart, nil)

	h.recordStartAcknowledged(t.Context(), "octocat/hello-world", 7, 42, "apply-a1b2c3d4e5f67890", "production", formatGitHubCaller("alice", "octocat/hello-world", 7))

	assert.Empty(t, posted())
	controls.mu.Lock()
	defer controls.mu.Unlock()
	assert.JSONEq(t, `{"started_count":1}`, string(carolStart.Metadata), "the request is left as it was")
}

func TestCallerIsPRCommand(t *testing.T) {
	assert.True(t, callerIsPRCommand("github:alice@octocat/hello-world#7", "octocat/hello-world", 7))
	assert.False(t, callerIsPRCommand("github:alice@octocat/hello-world#8", "octocat/hello-world", 7), "a command on another PR")
	assert.False(t, callerIsPRCommand("github:alice@octocat/other#7", "octocat/hello-world", 7), "a command on another repo")
	assert.False(t, callerIsPRCommand("cli:alice@laptop.local", "octocat/hello-world", 7))
	assert.False(t, callerIsPRCommand("", "octocat/hello-world", 7))
}
