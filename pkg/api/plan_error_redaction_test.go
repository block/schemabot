package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

// sensitivePlanFailure is the kind of text a storage driver or an engine call
// returns: an address and credentials that must stay in the server log.
const sensitivePlanFailure = "dial tcp 10.0.0.5:3306: user:pass@tcp(db.internal:3306)/schemabot: connection refused"

const (
	wantPlanFailedMessage = `plan failed for database "payments" in environment "staging": SchemaBot hit an internal error. ` +
		`Plan again in a moment. If it keeps failing, a SchemaBot operator can find the cause in the server log.`
	wantPlanRolloutFailedMessage = `plan failed for database "testapp" in environment "production": SchemaBot could not finish planning every rollout member. ` +
		`Plan again in a moment. If it keeps failing, a SchemaBot operator can find the cause in the server log.`
	wantPullFailedMessage = `pull schema failed for database "orders" in environment "production": SchemaBot hit an internal error while reading the live schema. ` +
		`Retry in a moment. If it keeps failing, a SchemaBot operator can find the cause in the server log.`
	wantApplyFailedMessage = `apply failed for plan plan-1: SchemaBot hit an internal error. ` +
		`The apply may already be recorded, so check the schema change's status before applying again. ` +
		`A SchemaBot operator can find the cause in the server log.`
)

// captureServiceLogs sends the service's logs to a buffer as JSON lines.
func captureServiceLogs(svc *Service) *bytes.Buffer {
	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	return &logs
}

// decodeErrorResponse decodes an error body and checks that none of the
// sensitive text reached it.
func decodeErrorResponse(t *testing.T, rec *httptest.ResponseRecorder) apitypes.ErrorResponse {
	t.Helper()
	var resp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
	for _, secret := range []string{"10.0.0.5", "user:pass", "db.internal"} {
		assert.NotContains(t, rec.Body.String(), secret)
	}
	return resp
}

// redactionPlanRequest is the plan request the plan handler cases send to the
// payments database.
func redactionPlanRequest() PlanRequest {
	return PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Repository:  "octo/payments",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
	}
}

// A plan that fails inside SchemaBot answers 500 with a fixed message that
// names the database and the environment and says to plan again. The cause
// stays in the server log with the same identifiers.
func TestHandlePlanAnswersFixedMessageWhenPlanningFails(t *testing.T) {
	tests := []struct {
		name   string
		client *mockTernClient
		plans  *capturingPlanStore
	}{
		{
			name:   "engine call fails",
			client: &mockTernClient{planErr: errors.New(sensitivePlanFailure)},
			plans:  &capturingPlanStore{},
		},
		{
			name:   "remote data plane fails",
			client: &mockTernClient{isRemote: true, planErr: status.Error(codes.Internal, sensitivePlanFailure)},
			plans:  &capturingPlanStore{},
		},
		{
			name:   "plan storage fails",
			client: &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-new"}},
			plans:  &capturingPlanStore{createErr: errors.New(sensitivePlanFailure)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := withheldDropService(tt.plans, tt.client)
			logs := captureServiceLogs(svc)

			rec := postJSON(t, svc.handlePlan, "/api/plan", redactionPlanRequest())

			require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
			resp := decodeErrorResponse(t, rec)
			assert.Equal(t, wantPlanFailedMessage, resp.Error)

			line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), "plan failed")
			assert.Equal(t, "payments", line["database"])
			assert.Equal(t, "staging", line["environment"])
			assert.Equal(t, "octo/payments", line["repository"])
			assert.Contains(t, line["error"], "user:pass")
		})
	}
}

// An environment whose members cannot be resolved fails the rollout check
// before any plan is made. The configuration text stays in the server log.
func TestHandlePlanAnswersFixedMessageWhenRolloutMembersDoNotResolve(t *testing.T) {
	svc := withheldDropService(&capturingPlanStore{}, &mockTernClient{})
	svc.config.Databases["payments"] = DatabaseConfig{
		Type: storage.DatabaseTypeMySQL,
		Environments: map[string]EnvironmentConfig{
			"staging": {
				Deployments:     map[string]DeploymentTarget{"user:pass@10.0.0.5:3306": {}},
				DeploymentOrder: []string{"user:pass@10.0.0.5:3306"},
			},
		},
	}
	logs := captureServiceLogs(svc)

	rec := postJSON(t, svc.handlePlan, "/api/plan", redactionPlanRequest())

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	resp := decodeErrorResponse(t, rec)
	assert.Equal(t, wantPlanFailedMessage, resp.Error)

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()),
		"plan failed: the environment's rollout members did not resolve, so the rollout rendering check refuses the plan")
	assert.Equal(t, "payments", line["database"])
	assert.Equal(t, "staging", line["environment"])
	assert.Contains(t, line["error"], "user:pass")
}

// unreadablePrimaryPlanStore stores every plan and then cannot read the
// primary's back, which is the read that lists what a rollout-wide apply of
// the plan refuses.
type unreadablePrimaryPlanStore struct {
	recordingPlanStore
}

func (s *unreadablePrimaryPlanStore) Get(ctx context.Context, planIdentifier string) (*storage.Plan, error) {
	if len(s.created) > 1 {
		return nil, errors.New(sensitivePlanFailure)
	}
	return s.recordingPlanStore.Get(ctx, planIdentifier)
}

// A plan whose primary was made but whose rollout members could not all be
// planned answers 500 with a fixed message that says to plan again. The cause
// stays in the server log with the plan's identifier.
func TestHandlePlanAnswersFixedMessageWhenRolloutMembersCannotBePlanned(t *testing.T) {
	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	client := &mockTernClient{
		planResp:     reviewed,
		planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)"),
	}
	svc := multiTargetService(t, client, &unreadablePrimaryPlanStore{})
	logs := captureServiceLogs(svc)
	request := planDiffReq(t)
	request.RendersRollout = true
	request.Repository = "octo/testapp"

	rec := postJSON(t, svc.handlePlan, "/api/plan", request)

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	resp := decodeErrorResponse(t, rec)
	assert.Equal(t, wantPlanRolloutFailedMessage, resp.Error)

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), "plan failed: the rollout's member plans could not be completed")
	assert.Equal(t, "testapp", line["database"])
	assert.Equal(t, "production", line["environment"])
	assert.Equal(t, "octo/testapp", line["repository"])
	assert.Equal(t, reviewed.PlanId, line["plan_id"])
	assert.Contains(t, line["error"], "user:pass")
}

// A plan the engine declines, and a plan SchemaBot refuses with the change to
// make, keep their text: it tells the author what to change in the schema
// files or in the deployment, and it carries no storage or engine cause.
func TestHandlePlanKeepsTextWrittenForTheReader(t *testing.T) {
	const refusal = "the schema files drop table orders_seq; remove the drop or list the table under drop_tables"
	withheld := redactionPlanRequest()
	withheld.IgnoreTables = []string{"orders_archive"}

	tests := []struct {
		name    string
		request PlanRequest
		client  *mockTernClient
		want    string
	}{
		{
			name:    "engine declines the plan",
			request: redactionPlanRequest(),
			client:  &mockTernClient{planErr: fmt.Errorf("plan namespace: %w", &engine.PlanRefusedError{Err: errors.New(refusal)})},
			want:    "plan failed: " + refusal,
		},
		{
			name:    "engine declines the plan under a wrap that names an address",
			request: redactionPlanRequest(),
			client: &mockTernClient{planErr: fmt.Errorf("plan namespace payments via 10.0.0.5:3306: %w",
				&engine.PlanRefusedError{Err: errors.New(refusal)})},
			want: "plan failed: " + refusal,
		},
		{
			name:    "remote data plane declines the plan",
			request: redactionPlanRequest(),
			client:  &mockTernClient{isRemote: true, planErr: status.Error(codes.FailedPrecondition, refusal)},
			want:    "plan failed: " + refusal,
		},
		{
			name:    "remote data plane declines the plan under a wrap that names an address",
			request: redactionPlanRequest(),
			client: &mockTernClient{isRemote: true, planErr: fmt.Errorf("plan via 10.0.0.5:3306: %w",
				status.Error(codes.FailedPrecondition, refusal))},
			want: "plan failed: " + refusal,
		},
		{
			name:    "deployment plans a drop that ignore_tables withholds",
			request: withheld,
			client: &mockTernClient{planResp: &ternv1.PlanResponse{
				PlanId:  "plan-withheld",
				Changes: dropsPlan("payments", "orders_archive"),
			}},
			want: `plan failed: the plan from deployment "default" proposes dropping "orders_archive", which ignore_tables withholds. ` +
				`That deployment's planner did not apply the exclusion, so the plan is refused rather than reviewed as a drop. ` +
				`Upgrade that deployment to a build that supports ignore_tables`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := withheldDropService(&capturingPlanStore{}, tt.client)

			rec := postJSON(t, svc.handlePlan, "/api/plan", tt.request)

			require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
			var resp apitypes.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.want, resp.Error)
		})
	}
}

// A remote data plane's plan refusal reaches the webhook marked as text written
// for the reader, so the pull request explains the refusal the same way it
// does a local engine's. The text is the refusal's own, without the gRPC
// framing or the wrap that names an address, and the status code still reads
// through it.
func TestExecutePlanMarksARemoteRefusalAsAuthored(t *testing.T) {
	const refusal = "drop_tables entries were written in the last 24h"
	svc := withheldDropService(&capturingPlanStore{}, &mockTernClient{isRemote: true,
		planErr: fmt.Errorf("plan via 10.0.0.5:3306: %w", status.Error(codes.FailedPrecondition, refusal))})

	_, err := svc.ExecutePlan(t.Context(), redactionPlanRequest())

	require.Error(t, err)
	text, quotesRepository, ok := AuthoredTextOf(err)
	require.True(t, ok, "a remote refusal must be marked as authored: %v", err)
	assert.Equal(t, refusal, text)
	assert.True(t, quotesRepository)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// A pull that fails inside SchemaBot answers 500 with a fixed message that
// names the database and the environment and says to retry. The cause stays in
// the server log.
func TestHandlePullSchemaAnswersFixedMessageWhenPullFails(t *testing.T) {
	client := &mockTernClient{pullSchemaErr: errors.New(sensitivePlanFailure)}
	svc := newAppScopedPullService(t, map[string]string{"orders": "commerce"}, client)
	logs := captureServiceLogs(svc)

	rec := postJSON(t, svc.handlePullSchema, "/api/pull", apitypes.PullSchemaRequest{Database: "orders", Environment: "production"})

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	resp := decodeErrorResponse(t, rec)
	assert.Equal(t, wantPullFailedMessage, resp.Error)

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), "pull schema failed")
	assert.Equal(t, "orders", line["database"])
	assert.Equal(t, "production", line["environment"])
	assert.Contains(t, line["error"], "user:pass")
}

// A pull request naming a namespace SchemaBot cannot pull is the caller's to
// fix, so it answers 400 with SchemaBot's own text, which says what to change.
func TestHandlePullSchemaKeepsTextWrittenForTheReader(t *testing.T) {
	svc := newAppScopedPullService(t, map[string]string{"orders": "commerce"}, &mockTernClient{})

	rec := postJSON(t, svc.handlePullSchema, "/api/pull", apitypes.PullSchemaRequest{
		Database:    "orders",
		Environment: "production",
		Namespaces:  []string{"a/b"},
	})

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	var resp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, `pull schema failed: pull namespace "a/b" must be a single path component`, resp.Error)
	assert.Equal(t, apitypes.ErrCodeInvalidRequest, resp.ErrorCode)
}

// A pull whose remote deployment cannot be reached answers 503 with a fixed
// message and keeps engine_unavailable, so the CLI still knows to retry. The
// connection error, which names the address it dialed, stays in the server log.
func TestHandlePullSchemaAnswersFixedMessageWhenDeploymentIsUnavailable(t *testing.T) {
	client := &mockTernClient{isRemote: true, pullSchemaErr: status.Error(codes.Unavailable,
		`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.5:9090: connect: connection refused"`)}
	svc := newAppScopedPullService(t, map[string]string{"orders": "commerce"}, client)
	logs := captureServiceLogs(svc)

	rec := postJSON(t, svc.handlePullSchema, "/api/pull", apitypes.PullSchemaRequest{Database: "orders", Environment: "production"})

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	resp := decodeErrorResponse(t, rec)
	assert.Equal(t, `pull schema failed for database "orders" in environment "production": SchemaBot could not reach the deployment that serves it. `+
		`Retry in a moment. If it keeps failing, a SchemaBot operator can find the cause in the server log.`, resp.Error)
	assert.Equal(t, apitypes.ErrCodeEngineUnavailable, resp.ErrorCode)

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), "pull schema failed because remote deployment is unavailable")
	assert.Equal(t, "orders", line["database"])
	assert.Contains(t, line["error"], "10.0.0.5:9090")
}

// An apply whose plan was made for a different rollout member than the one it
// would run on keeps the text that says to re-plan, through the handler's
// message builder and under an outer wrap.
func TestApplyFailureKeepsReplanTextWhenPlanMemberDiffers(t *testing.T) {
	svc := withheldDropService(&capturingPlanStore{}, &mockTernClient{})
	plan := &storage.Plan{Database: "payments", PlanIdentifier: "plan-1", Deployment: "east", Target: "payments-east"}
	targets := []routing.ExecutionTarget{{Deployment: "west", Target: "payments-west"}}

	_, err := svc.resolveApplyMembers(t.Context(), plan, "staging", targets)
	require.Error(t, err)

	got := applyFailureMessage("plan-1", fmt.Errorf("create apply via 10.0.0.5:3306: %w", err))
	assert.Contains(t, got, "re-plan the environment so the apply runs the plan its target was planned for")
	assert.NotContains(t, got, "10.0.0.5")
	assert.NotContains(t, got, "SchemaBot hit an")
}

// An apply that fails inside SchemaBot answers 500 with a fixed message that
// names the plan and says the apply may already be recorded. The cause stays
// in the server log with the plan and environment.
func TestHandleApplyAnswersFixedMessageWhenApplyFails(t *testing.T) {
	plan := executeApplyTestPlan()
	plan.Deployment = "user:pass@10.0.0.5:3306"
	svc, _ := newQueueApplyTestService(plan, &mockTernClient{}, &capturingApplyStore{})
	logs := captureServiceLogs(svc)

	rec := postJSON(t, svc.handleApply, "/api/apply", ApplyRequest{PlanID: "plan-1", Environment: "staging"})

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	resp := decodeErrorResponse(t, rec)
	assert.Equal(t, wantApplyFailedMessage, resp.Error)
	assert.NotEqual(t, apitypes.ErrCodeStorageError, resp.ErrorCode)

	line := requireLogLine(t, decodeLogLines(t, logs.Bytes()), "apply failed")
	assert.Equal(t, "plan-1", line["plan_id"])
	assert.Equal(t, "staging", line["environment"])
	assert.Contains(t, line["error"], "user:pass")
}

// A failure SchemaBot wrote for the reader keeps its text, and an outer wrap
// around it adds nothing to the response. Every other error is replaced.
func TestFailureMessagesKeepOnlyTextWrittenForTheReader(t *testing.T) {
	authored := fmt.Errorf("database %q (staging) via "+sensitivePlanFailure+": %w", "payments", operatorMessagef("re-plan the environment, then apply again"))
	raw := fmt.Errorf("store plan: %w", errors.New(sensitivePlanFailure))

	assert.Equal(t, "apply failed: re-plan the environment, then apply again", applyFailureMessage("plan-1", authored))
	assert.Equal(t, "plan failed: re-plan the environment, then apply again", planFailureMessage("payments", "staging", authored))
	assert.Equal(t, "plan failed: re-plan the environment, then apply again", planRolloutFailureMessage("payments", "staging", authored))

	for name, message := range map[string]string{
		"apply":            applyFailureMessage("plan-1", raw),
		"pull":             pullFailureMessage("orders", "production"),
		"pull unavailable": pullUnavailableMessage("orders", "production"),
		"plan":             planFailureMessage("payments", "staging", raw),
		"plan rollout":     planRolloutFailureMessage("payments", "staging", raw),
	} {
		assert.NotContains(t, message, "10.0.0.5", name)
		assert.NotContains(t, message, "user:pass", name)
		assert.True(t, strings.HasSuffix(message, "server log."), "%s: %s", name, message)
	}
}

// A drop check that stopped because a schema file would not parse keeps its
// text, which names the file for its author. A drop check that stopped for any
// other reason, such as the parser not being available, is replaced.
func TestPlanFailureMessageForUnselectedDropCheck(t *testing.T) {
	declared := map[string]*ternv1.SchemaFiles{
		"ns_1": {Files: map[string]string{"payments.sql": "CREATE TABLE `payments` ("}},
	}
	checkFailure := func(cause error) error {
		return fmt.Errorf("plan via 10.0.0.5:3306: %w", &UnselectedTableDropCheckError{
			Database: "orders", Environment: "production", Target: "orders-001", Err: cause,
		})
	}

	_, parseCause := tablesDeclaredBy(declared, []string{"ns_1"}, "mysql")
	require.ErrorAs(t, parseCause, new(*schemaFileParseError))
	got := planFailureMessage("orders", "production", checkFailure(parseCause))
	assert.True(t, strings.HasPrefix(got, `plan failed: check planned drops of database "orders" environment "production" target "orders-001" `), got)
	assert.Contains(t, got, "ns_1/payments.sql")
	assert.NotContains(t, got, "10.0.0.5")

	_, parserCause := tablesDeclaredBy(declared, []string{"ns_1"}, "nosuchdb")
	require.Error(t, parserCause)
	assert.NotErrorAs(t, parserCause, new(*schemaFileParseError))
	storageCause := fmt.Errorf("load schema files: %w", errors.New(sensitivePlanFailure))
	for name, cause := range map[string]error{"parser unavailable": parserCause, "storage": storageCause} {
		got := planFailureMessage("orders", "production", checkFailure(cause))
		assert.Equal(t, `plan failed for database "orders" in environment "production": SchemaBot hit an internal error. `+
			`Plan again in a moment. If it keeps failing, a SchemaBot operator can find the cause in the server log.`, got, name)
	}
}
