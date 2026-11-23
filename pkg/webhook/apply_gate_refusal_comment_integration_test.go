//go:build integration

package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// gateRefusalEdit is one edit the fake GitHub accepted for a PR comment.
type gateRefusalEdit struct {
	ID   string
	Body string
}

// recordCommentEdits serves comment edits on the fake GitHub, answering with
// status, and returns what it receives. The fake answers every posted comment
// with ID 99, so that is the ID an edit of a posted comment carries.
func recordCommentEdits(mux *http.ServeMux, status int) <-chan gateRefusalEdit {
	edits := make(chan gateRefusalEdit, 10)
	mux.HandleFunc("PATCH /repos/octocat/hello-world/issues/comments/{id}", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		edits <- gateRefusalEdit{ID: r.PathValue("id"), Body: body.Body}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, `{"id":99}`)
	})
	return edits
}

func freezeDatabase(t *testing.T, svc *api.Service, dbName, environment, reason string) {
	t.Helper()
	freezeID := engine.NewFreezeID()
	_, err := svc.Storage().Freezes().Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier:  freezeID,
		Environment: environment,
		Databases:   []string{dbName},
		Kind:        storage.FreezeKindOnDemand,
		Running:     storage.FreezeRunningContinue,
		StartsAt:    time.Now().Add(-time.Hour),
		Reason:      reason,
		CreatedBy:   "alice",
		Source:      storage.FreezeSourceManual,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = svc.Storage().Freezes().Lift(context.WithoutCancel(t.Context()), freezeID, "alice", "test done")
	})
}

// An automatic apply posts its "Applying automatically" comment before the
// change gates run, so a freeze that refuses the apply afterwards leaves that
// comment promising a run that never happens. The refusal comment still posts,
// the lock is released, and the earlier comment is edited to say it did not run.
func TestE2EAutomaticApplyRefusedByAFreezeEditsItsComment(t *testing.T) {
	dbName := "webhook_apply_frozen_edit"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	freezeDatabase(t, svc, dbName, driftEnv, "incident 45")

	var edits <-chan gateRefusalEdit
	apply := runRolloutWebhookWithFiles(t, svc, dbName,
		buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e " + driftEnv, isPR: true}, nil),
		map[string]string{"users.sql": usersWithEmailSchema},
		func(mux *http.ServeMux, _ *planFlowResult) { edits = recordCommentEdits(mux, http.StatusOK) })

	announced := awaitCommentContaining(t, apply, "**Applying automatically**")
	assert.Contains(t, announced, "🔒 **Lock acquired by**")
	awaitCommentContaining(t, apply, "## ❄️ Apply Blocked by a Freeze\n")

	edit := awaitCapture(t, edits, "the announcing comment to be edited", func(gateRefusalEdit) bool { return true })
	assert.Equal(t, "99", edit.ID, "the edit targets the comment the apply posted")
	assert.Contains(t, edit.Body, "**Not applied** — see the comment below for why.\n")
	assert.Contains(t, edit.Body, "🔓 **Lock released**")
	assert.NotContains(t, edit.Body, "Applying automatically")
	assert.NotContains(t, edit.Body, "Lock acquired")
	assert.Contains(t, edit.Body, "ALTER TABLE", "the edited comment still shows the plan it was posted with")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}

// The edit is cosmetic: GitHub refusing it changes nothing the operator acts
// on. The refusal comment is posted and the lock is released regardless.
func TestE2EAutomaticApplyRefusedByAFreezeSurvivesAFailedEdit(t *testing.T) {
	dbName := "webhook_apply_frozen_edit_fails"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)
	freezeDatabase(t, svc, dbName, driftEnv, "incident 46")

	var edits <-chan gateRefusalEdit
	apply := runRolloutWebhookWithFiles(t, svc, dbName,
		buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e " + driftEnv, isPR: true}, nil),
		map[string]string{"users.sql": usersWithEmailSchema},
		func(mux *http.ServeMux, _ *planFlowResult) { edits = recordCommentEdits(mux, http.StatusNotFound) })

	awaitCommentContaining(t, apply, "## ❄️ Apply Blocked by a Freeze\n")
	awaitCapture(t, edits, "the edit attempt", func(gateRefusalEdit) bool { return true })

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}

// apply-confirm acts on a comment that asked for the confirmation, not one that
// announced an automatic apply, so a freeze refusing it posts the refusal and
// edits nothing.
func TestE2EApplyConfirmRefusedByAFreezeEditsNoComment(t *testing.T) {
	dbName := "webhook_confirm_frozen_no_edit"
	svc := setupE2EService(t, dbName)
	seedCheck(t, svc, dbName, "staging", "action_required")
	seedPendingConfirmation(t, svc, dbName, "plan-pending-"+dbName, "staging")
	freezeDatabase(t, svc, dbName, "staging", "incident 47")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client := gh.NewClient(nil)
	client.BaseURL = baseURL
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": confirmEnvironmentUsersSQL},
		fmt.Sprintf("database: %s\ntype: mysql\n", dbName), dbName)
	edits := recordCommentEdits(mux, http.StatusOK)
	h := newE2EHandler(t, svc, client)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply-confirm -e staging", isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	awaitCommentContaining(t, result, "Apply-confirm Blocked by a Freeze")
	// Any edit happens in the handler's own work, so once that work is done
	// there is nothing left that could still make one.
	drainCtx, cancel := context.WithTimeout(t.Context(), webhookIntegrationPollDeadline)
	defer cancel()
	h.DrainInProcessWebhookWork(drainCtx)
	require.NoError(t, drainCtx.Err(), "the command's work must finish before the test checks for edits")
	assert.Empty(t, edits, "a refused apply-confirm edits no comment")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}
