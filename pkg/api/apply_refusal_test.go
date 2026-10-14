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
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// postApplyForRefusal sends body to POST /api/apply through the service's
// routes and decodes the error response the refusal carries.
func postApplyForRefusal(t *testing.T, svc *Service, body string) (int, apitypes.ErrorResponse) {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/apply", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp), "refusal body must be an error response")
	return w.Code, resp
}

// applyOutcomeRecorder captures the apply counter and the ExecuteApply spans a
// request records.
type applyOutcomeRecorder struct {
	metrics *sdkmetric.ManualReader
	spans   *tracetest.InMemoryExporter
}

func recordApplyOutcomes(t *testing.T) applyOutcomeRecorder {
	t.Helper()
	return applyOutcomeRecorder{metrics: installManualMetricReader(t), spans: setupTraceTest(t)}
}

// statuses returns the apply counter's total per status attribute.
func (r applyOutcomeRecorder) statuses(t *testing.T) map[string]int64 {
	t.Helper()
	totals := make(map[string]int64)
	for _, dp := range collectCounterPoints(t, r.metrics, "schemabot.applies.total") {
		totals[attributeValue(t, dp, "status")] += dp.Value
	}
	return totals
}

// spanStatuses returns the status code of every ExecuteApply span.
func (r applyOutcomeRecorder) spanStatuses() []codes.Code {
	var statuses []codes.Code
	for _, span := range r.spans.GetSpans() {
		if span.Name == "ExecuteApply" {
			statuses = append(statuses, span.Status.Code)
		}
	}
	return statuses
}

// multiTargetApplyHTTPService serves the multi-target testapp/production
// environment over HTTP: the apply's plan is the primary's, and each member
// plan in plans is found by the member lookup apply creation runs.
func multiTargetApplyHTTPService(primary *storage.Plan, members []*storage.Plan) (*Service, *capturingApplyStore, *capturingTaskStore) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"production": multiTargetEnv()},
			},
		},
	}
	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	plans := &listingPlanStore{mockPlanLookupStore: mockPlanLookupStore{plan: primary}, plans: members}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:     plans,
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, cfg, map[string]tern.Client{"eu/production": &mockTernClient{}}, logger)
	return svc, applies, tasks
}

// An apply on a plan that carries a change the engine refuses can never
// succeed, whatever the caller retries with. It is refused as a 422 with the
// plan_blocked code, so clients stop retrying and server-error alerting does
// not count a request the caller has to fix.
func TestApplyHandler_BlockedPlanIsUnprocessable(t *testing.T) {
	plan := executeApplyTestPlan()
	plan.Namespaces["testdb"].Tables[0].ExecutionMode = "blocked"
	plan.Namespaces["testdb"].Tables[0].ModeReason = "requires privileges unavailable to the engine"
	applies := &capturingApplyStore{}
	svc, tasks := newQueueApplyTestService(plan, &mockTernClient{}, applies)
	outcomes := recordApplyOutcomes(t)

	// allow_unsafe cannot unlock a blocked change, so the refusal is the same
	// with or without it.
	for _, body := range []string{
		`{"plan_id":"plan-1","environment":"staging"}`,
		`{"plan_id":"plan-1","environment":"staging","options":{"allow_unsafe":"true"}}`,
	} {
		status, resp := postApplyForRefusal(t, svc, body)

		assert.Equal(t, http.StatusUnprocessableEntity, status, body)
		assert.Equal(t, apitypes.ErrCodePlanBlocked, resp.ErrorCode, body)
		assert.Equal(t, `apply rejected: stored plan plan-1 contains a blocked change for table "users": requires privileges unavailable to the engine`, resp.Error, body)
	}
	assert.False(t, apitypes.IsRetryableErrorCode(apitypes.ErrCodePlanBlocked))
	assert.Nil(t, applies.apply, "a blocked plan must not store an apply")
	assert.Empty(t, tasks.tasks)
	assert.Equal(t, map[string]int64{"rejected": 2}, outcomes.statuses(t), "a refusal is counted apart from server errors")
	assert.Equal(t, []codes.Code{codes.Unset, codes.Unset}, outcomes.spanStatuses(), "a refusal does not mark the span as an error")
}

// An apply on a plan with an unsafe change that did not consent to it is the
// caller's to fix by retrying with allow_unsafe=true, so it is a 400 with the
// unsafe_opt_in_required code a client can key its remedy on.
func TestApplyHandler_UnsafePlanWithoutOptInIsBadRequest(t *testing.T) {
	t.Run("table change", func(t *testing.T) {
		plan := executeApplyTestPlan()
		plan.Namespaces["testdb"].Tables[0] = storage.TableChange{
			Namespace: "testdb",
			Table:     "legacy_users",
			DDL:       "DROP TABLE `legacy_users`",
			Operation: "drop",
		}
		applies := &capturingApplyStore{}
		svc, tasks := newQueueApplyTestService(plan, &mockTernClient{}, applies)
		outcomes := recordApplyOutcomes(t)

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging"}`)

		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, apitypes.ErrCodeUnsafeOptInRequired, resp.ErrorCode)
		assert.Equal(t, `apply rejected: stored plan plan-1 contains unsafe change for table "legacy_users": DROP TABLE removes all data; retry with allow_unsafe=true`, resp.Error)
		assert.Nil(t, applies.apply, "an unconsented unsafe change must not store an apply")
		assert.Empty(t, tasks.tasks)
		assert.Equal(t, map[string]int64{"rejected": 1}, outcomes.statuses(t), "a refusal is counted apart from server errors")
		assert.Equal(t, []codes.Code{codes.Unset}, outcomes.spanStatuses(), "a refusal does not mark the span as an error")
	})

	t.Run("VSchema change", func(t *testing.T) {
		plan := vschemaGateTestPlan(map[string]string{
			storage.PlanMetadataVSchemaChanged:   "true",
			storage.PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
		})
		svc, applies, tasks := newVSchemaGateTestService(plan)

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging"}`)

		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, apitypes.ErrCodeUnsafeOptInRequired, resp.ErrorCode)
		assert.Equal(t, `apply rejected: stored plan plan-1 contains an unsafe VSchema change in namespace "testdb": removing vindex email_idx changes query routing; retry with allow_unsafe=true`, resp.Error)
		assert.Nil(t, applies.apply, "an unconsented unsafe VSchema change must not store an apply")
		assert.Empty(t, tasks.tasks)
	})
	assert.False(t, apitypes.IsRetryableErrorCode(apitypes.ErrCodeUnsafeOptInRequired))
}

// refusalLog captures the service's warnings as JSON lines, so a test can read
// the fields an operator filters on.
func refusalLog(svc *Service) *bytes.Buffer {
	var buf bytes.Buffer
	svc.logger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return &buf
}

// memberRefusalLogFields decodes the one warning a refused member plan logs.
func memberRefusalLogFields(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	var fields map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry), "log line must be JSON: %s", line)
		if entry["msg"] == "apply rejected: a rollout member's own plan cannot run in an apply created from the primary's plan" {
			require.Nil(t, fields, "a refused member plan logs one warning")
			fields = entry
		}
	}
	require.NotNil(t, fields, "a refused member plan logs a warning; got: %s", logs.String())
	return fields
}

// A rollout member planned against its own live schema can carry a change the
// primary's plan does not. Its refusal names the member, in the response and
// in the warning an operator filters on, and its kind decides the status and
// code: a blocked or unconsented unsafe change gets what the primary's would,
// because the caller's remedy is the same, while an unsafe change the caller
// was never shown is never answered with a code that invites the opt-in.
func TestApplyHandler_MemberPlanRefusalsAreClientErrors(t *testing.T) {
	primary := primaryPlanRow("testapp-001")
	primary.Environment = "production"

	t.Run("blocked member plan", func(t *testing.T) {
		member := memberPlanWithChange(storage.TableChange{
			Namespace:     "testapp",
			Table:         "orders",
			Operation:     "alter",
			DDL:           "ALTER TABLE `orders` ADD COLUMN `region` varchar(16)",
			ExecutionMode: "blocked",
			ModeReason:    "the engine refuses this statement",
		})
		svc, applies, tasks := multiTargetApplyHTTPService(primary, []*storage.Plan{member})
		logs := refusalLog(svc)
		outcomes := recordApplyOutcomes(t)

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-primary","environment":"production","renders_rollout":true}`)

		assert.Equal(t, http.StatusUnprocessableEntity, status)
		assert.Equal(t, apitypes.ErrCodePlanBlocked, resp.ErrorCode)
		assert.Equal(t, `apply rejected: rollout member eu/testapp-002: stored plan plan-second contains a blocked change for table "orders": the engine refuses this statement`, resp.Error)
		assert.Nil(t, applies.apply, "a blocked member plan must not store an apply")
		assert.Empty(t, tasks.tasks)
		fields := memberRefusalLogFields(t, logs)
		assert.Equal(t, "eu/testapp-002", fields["member"])
		assert.Equal(t, "blocked", fields["refusal"])
		assert.Equal(t, "plan-primary", fields["plan_id"])
		assert.Equal(t, map[string]int64{"rejected": 1}, outcomes.statuses(t), "a refusal is counted apart from server errors")
		assert.Equal(t, []codes.Code{codes.Unset}, outcomes.spanStatuses(), "a refusal does not mark the span as an error")
	})

	t.Run("unsafe member plan without opt-in", func(t *testing.T) {
		member := memberPlanWithChange(storage.TableChange{
			Namespace: "testapp",
			Table:     "legacy_orders",
			Operation: "drop",
			DDL:       "DROP TABLE `legacy_orders`",
		})
		svc, applies, tasks := multiTargetApplyHTTPService(primary, []*storage.Plan{member})
		logs := refusalLog(svc)

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-primary","environment":"production","renders_rollout":true}`)

		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, apitypes.ErrCodeUnsafeOptInRequired, resp.ErrorCode)
		assert.Equal(t, `apply rejected: rollout member eu/testapp-002: stored plan plan-second contains unsafe change for table "legacy_orders": DROP TABLE removes all data; retry with allow_unsafe=true`, resp.Error)
		assert.Nil(t, applies.apply, "an unconsented unsafe member plan must not store an apply")
		assert.Empty(t, tasks.tasks)
		fields := memberRefusalLogFields(t, logs)
		assert.Equal(t, "eu/testapp-002", fields["member"])
		assert.Equal(t, "unsafe_without_opt_in", fields["refusal"])
	})

	t.Run("undisclosed unsafe member plan under the opt-in", func(t *testing.T) {
		member := memberPlanWithChange(storage.TableChange{
			Namespace: "testapp",
			Table:     "legacy_orders",
			Operation: "drop",
			DDL:       "DROP TABLE `legacy_orders`",
		})
		svc, applies, tasks := multiTargetApplyHTTPService(primary, []*storage.Plan{member})
		logs := refusalLog(svc)

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-primary","environment":"production","renders_rollout":true,"options":{"allow_unsafe":"true"}}`)

		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, apitypes.ErrCodeInvalidRequest, resp.ErrorCode, "no opt-in covers a change the caller was never shown")
		assert.Equal(t, `apply rejected: rollout member eu/testapp-002: plan plan-second carries an unsafe change for table "legacy_orders" that the primary target's plan does not carry, so the disclosure on the primary target's plan plan-primary never named it and no opt-in covers it`, resp.Error)
		assert.Nil(t, applies.apply, "an undisclosed unsafe member plan must not store an apply")
		assert.Empty(t, tasks.tasks)
		fields := memberRefusalLogFields(t, logs)
		assert.Equal(t, "eu/testapp-002", fields["member"])
		assert.Equal(t, "undisclosed_unsafe", fields["refusal"])
	})
}

// A plan the apply names that does not exist is refused with a typed error
// carrying the plan identifier, whether the store reports the miss as a nil
// plan or as the sentinel, so a PR comment can name the plan without
// presenting the error text.
func TestExecuteApplyMissingPlanIsTyped(t *testing.T) {
	tests := map[string]struct {
		plans    *mockPlanLookupStore
		wantText string
	}{
		"nil plan": {plans: &mockPlanLookupStore{}, wantText: "plan not found: plan-gone"},
		"sentinel": {plans: &mockPlanLookupStore{err: storage.ErrPlanNotFound}, wantText: "plan not found: plan-gone"},
		// A store that attaches a reason to the sentinel keeps it on the error
		// for the log line.
		"sentinel with reason": {
			plans:    &mockPlanLookupStore{err: fmt.Errorf("read plan from replica: %w", storage.ErrPlanNotFound)},
			wantText: "read plan from replica: plan not found: plan-gone",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
			svc := New(&mockStorageWithPlanLookup{plans: tt.plans}, testServerConfig(), nil, logger)

			_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-gone", Environment: "staging"})

			require.ErrorIs(t, err, storage.ErrPlanNotFound)
			missing, ok := errors.AsType[*PlanNotFoundError](err)
			require.True(t, ok, "a missing plan must be a *PlanNotFoundError, got %T", err)
			assert.Equal(t, "plan-gone", missing.PlanID)
			assert.Equal(t, tt.wantText, err.Error())
		})
	}
}

// failingLockStore fails every lock read with err.
type failingLockStore struct {
	storage.LockStore
	err error
}

func (s *failingLockStore) Get(context.Context, string, string) (*storage.Lock, error) {
	return nil, s.err
}

// errDriverStorage reads like a SQL driver failure: it names the storage
// host and the statement, neither of which belongs in an API response.
var errDriverStorage = errors.New("Error 1205 (HY000): Lock wait timeout exceeded; try restarting transaction: dial tcp 10.0.0.5:3306")

// A storage failure while queueing an apply is the server's fault, not the
// caller's: it stays a 500 with the storage_error code and is counted and
// traced as an apply error, so a client keeps treating it as a server error
// rather than as a request it has to change. The response is a fixed line
// naming the plan; the SQL driver's text stays in the server log.
func TestApplyHandler_StorageFailureStaysServerError(t *testing.T) {
	const wantMessage = "apply failed: storage error while queueing plan plan-1; see server logs and check for a stored apply before retrying"

	// The server log carries what the response withholds: the SQL driver's
	// text, and the identifiers that tie the failed attempt to its plan, its
	// routing, its PR and its caller.
	t.Run("storing the apply fails", func(t *testing.T) {
		applies := &capturingApplyStore{err: errDriverStorage}
		plan := executeApplyTestPlan()
		plan.Repository = "acme/schemas"
		plan.PullRequest = 42
		svc, tasks := newQueueApplyTestService(plan, &mockTernClient{}, applies)
		var logs bytes.Buffer
		svc.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelError}))
		outcomes := recordApplyOutcomes(t)

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging","caller":"github:octocat"}`)

		assert.Equal(t, http.StatusInternalServerError, status)
		assert.Equal(t, apitypes.ErrCodeStorageError, resp.ErrorCode)
		assert.Equal(t, wantMessage, resp.Error)
		assert.NotContains(t, resp.Error, "10.0.0.5")
		assert.NotContains(t, resp.Error, "Lock wait timeout")
		assert.Nil(t, applies.apply)
		assert.Empty(t, tasks.tasks)
		assert.Equal(t, map[string]int64{"error": 1}, outcomes.statuses(t))
		assert.Equal(t, []codes.Code{codes.Error}, outcomes.spanStatuses())

		for _, attr := range []string{
			"plan_id=plan-1", "database=testdb", "database_type=mysql", "deployment=" + DefaultDeployment,
			"environment=staging", "repo=acme/schemas", "pr=42", "caller=github:octocat",
			`operation="store apply and tasks"`, "10.0.0.5",
		} {
			assert.Contains(t, logs.String(), attr)
		}
		assert.Regexp(t, `apply_id=apply-[0-9a-f]+`, logs.String(), "the log names the apply that was being queued")
	})

	t.Run("reading the lock fails", func(t *testing.T) {
		applies := &capturingApplyStore{}
		svc, _ := newQueueApplyTestService(executeApplyTestPlan(), &mockTernClient{}, applies)
		svc.storage.(*mockStorageWithApplyStores).locks = &failingLockStore{err: errDriverStorage}

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging"}`)

		assert.Equal(t, http.StatusInternalServerError, status)
		assert.Equal(t, apitypes.ErrCodeStorageError, resp.ErrorCode)
		assert.Equal(t, wantMessage, resp.Error)
		assert.NotContains(t, resp.Error, "10.0.0.5")
		assert.Nil(t, applies.apply)
	})

	// Listing a rollout's member plans fails while the apply is being queued.
	// The failure names the apply being queued, so the error log can be
	// matched to the attempt, and the caller still gets the fixed line.
	t.Run("listing the member plans fails", func(t *testing.T) {
		primary := primaryPlanRow("testapp-001")
		primary.Environment = "production"
		svc, applies, _ := multiTargetApplyHTTPService(primary, nil)
		svc.storage.(*mockStorageWithApplyStores).plans.(*listingPlanStore).listErr = errDriverStorage

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-primary","environment":"production","renders_rollout":true}`)
		assert.Equal(t, http.StatusInternalServerError, status)
		assert.Equal(t, apitypes.ErrCodeStorageError, resp.ErrorCode)
		assert.Equal(t, "apply failed: storage error while queueing plan plan-primary; see server logs and check for a stored apply before retrying", resp.Error)
		assert.Nil(t, applies.apply)

		_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-primary", Environment: "production", RendersRollout: true})
		storageErr, ok := errors.AsType[*applyStorageError](err)
		require.True(t, ok, "a member-plan listing failure is a storage failure: %v", err)
		assert.True(t, strings.HasPrefix(storageErr.ApplyIdentifier, "apply-"), "the failure names the apply being queued, got %q", storageErr.ApplyIdentifier)
	})

	// Storage refusing the apply on the merits is not a storage failure: an
	// active apply on the target keeps its conflict answer and its message.
	t.Run("an active apply on the target stays a conflict", func(t *testing.T) {
		applies := &capturingApplyStore{err: fmt.Errorf("apply apply-7 (state running) holds its targets: %w", storage.ErrActiveApplyExists)}
		svc, _ := newQueueApplyTestService(executeApplyTestPlan(), &mockTernClient{}, applies)

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging"}`)

		assert.Equal(t, http.StatusConflict, status)
		assert.Equal(t, apitypes.ErrCodeActiveApplyExists, resp.ErrorCode)
		assert.Equal(t, "apply blocked by active apply: store apply and tasks: apply apply-7 (state running) holds its targets: active apply already exists", resp.Error)

		_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})
		require.ErrorIs(t, err, storage.ErrActiveApplyExists)
		_, isStorageFailure := errors.AsType[*applyStorageError](err)
		assert.False(t, isStorageFailure, "a conflict storage decided on the merits is not a storage failure")
	})

	// A request the plan's validation refuses keeps its own message.
	t.Run("a validation refusal keeps its message", func(t *testing.T) {
		plan := executeApplyTestPlan()
		plan.DatabaseType = storage.DatabaseTypePostgres
		svc, _ := newQueueApplyTestService(plan, &mockTernClient{}, &capturingApplyStore{})

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging","options":{"defer_cutover":"true"}}`)

		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, apitypes.ErrCodeInvalidRequest, resp.ErrorCode)
		assert.Equal(t, `apply rejected: database "testdb": deferred cutover is not supported for database_type: postgres`, resp.Error)
	})

	// An engine routing failure is not a storage failure and keeps its message.
	t.Run("an engine routing failure keeps its message", func(t *testing.T) {
		plan := executeApplyTestPlan()
		plan.Deployment = "unrouted"
		svc, _ := newQueueApplyTestService(plan, &mockTernClient{}, &capturingApplyStore{})

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging"}`)

		assert.Equal(t, http.StatusInternalServerError, status)
		assert.NotEqual(t, apitypes.ErrCodeStorageError, resp.ErrorCode)
		assert.Equal(t, `apply failed: database "testdb" (staging): unknown deployment "unrouted": tern deployment not configured`, resp.Error)
	})
}
