package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// newPullFailureService serves one MySQL database from one deployment and
// records every log line as JSON, so a test can count how often a failure was
// logged and read the fields it was logged with.
func newPullFailureService(t *testing.T, client *mockTernClient) (*Service, *bytes.Buffer) {
	t.Helper()
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "orders-production", Deployment: "us"},
				},
			},
		},
		TernDeployments: TernConfig{"us": {"production": "tern.example.com:80"}},
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return New(&mockStorage{}, cfg, map[string]tern.Client{"us/production": client}, logger), &logs
}

// pullLogRecords returns the decoded log records at the given level.
func pullLogRecords(t *testing.T, logs *bytes.Buffer, level string) []map[string]any {
	t.Helper()
	var records []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(logs.Bytes()))
	for scanner.Scan() {
		var record map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record), scanner.Text())
		if record["level"] == level {
			records = append(records, record)
		}
	}
	require.NoError(t, scanner.Err())
	return records
}

func postPull(t *testing.T, ctx context.Context, svc *Service) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/pull",
		strings.NewReader(`{"database":"orders","environment":"production","type":"mysql","namespaces":["orders"]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

// A pull whose read fails is logged exactly once, and that one line names the
// target and namespace the read went to and the data-plane endpoint it used,
// so an operator triaging it never has to join two lines to find where the
// read went. The response carries the failure without the endpoint.
func TestPullSchemaHandlerLogsAReadFailureOnceWithItsTarget(t *testing.T) {
	client := &mockTernClient{pullSchemaErr: status.Error(codes.Internal, "read information_schema: connection reset")}
	svc, logs := newPullFailureService(t, client)

	w := postPull(t, t.Context(), svc)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "pull schema failed: rpc error: code = Internal desc = read information_schema: connection reset")
	assert.NotContains(t, w.Body.String(), "mock", "the data-plane endpoint must stay out of the response")

	errorLogs := pullLogRecords(t, logs, "ERROR")
	require.Len(t, errorLogs, 1, "a failed pull is logged once")
	record := errorLogs[0]
	assert.Equal(t, "pull schema failed", record["msg"])
	assert.Equal(t, "orders", record["database"])
	assert.Equal(t, "production", record["environment"])
	assert.Equal(t, "us", record["deployment"])
	assert.Equal(t, "orders-production", record["target"])
	assert.Equal(t, "orders", record["namespace"])
	assert.Equal(t, "mock", record["endpoint"])
	assert.Equal(t, false, record["is_remote"])
}

// A remote data plane that is unreachable is still answered with a 503 the
// caller can match on, and is still logged once with the target it names.
func TestPullSchemaHandlerLogsAnUnavailableRemoteOnce(t *testing.T) {
	client := &mockTernClient{pullSchemaErr: status.Error(codes.Unavailable, "no healthy upstream"), isRemote: true}
	svc, logs := newPullFailureService(t, client)

	w := postPull(t, t.Context(), svc)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var body apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, apitypes.ErrCodeEngineUnavailable, body.ErrorCode)
	assert.Contains(t, body.Error, `remote deployment "us" target "orders-production" unavailable`)

	errorLogs := pullLogRecords(t, logs, "ERROR")
	require.Len(t, errorLogs, 1, "a failed pull is logged once")
	record := errorLogs[0]
	assert.Equal(t, "pull schema failed because remote deployment is unavailable", record["msg"])
	assert.Equal(t, "us", record["deployment"])
	assert.Equal(t, "orders-production", record["target"])
	assert.Equal(t, "orders", record["namespace"])
	assert.Equal(t, true, record["is_remote"])
}

// A caller that hangs up mid-pull cancels the read. That is the caller's
// choice, not a failure on this side, so it is a warning, and nothing is
// written to a connection no one is reading.
func TestPullSchemaHandlerWarnsWhenTheCallerDisconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	client := &mockTernClient{
		pullSchemaHook: func(*ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
			cancel()
			return nil, status.Error(codes.Canceled, context.Canceled.Error())
		},
	}
	svc, logs := newPullFailureService(t, client)

	w := postPull(t, ctx, svc)

	assert.Empty(t, w.Body.String(), "no response is written to a caller that left")
	assert.Empty(t, pullLogRecords(t, logs, "ERROR"))
	warnings := pullLogRecords(t, logs, "WARN")
	var abandoned []map[string]any
	for _, record := range warnings {
		if record["msg"] == "pull schema abandoned because the caller disconnected before it finished" {
			abandoned = append(abandoned, record)
		}
	}
	require.Len(t, abandoned, 1)
	assert.Equal(t, "orders-production", abandoned[0]["target"])
}

// slowPullClient answers a pull after a delay, standing in for the live-schema
// read of a database large enough that it outlasts the server-wide write
// timeout.
type slowPullClient struct {
	delay time.Duration
	*mockTernClient
}

func (c *slowPullClient) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	time.Sleep(c.delay)
	return c.mockTernClient.PullSchema(ctx, req)
}

// A pull that takes longer than the server-wide write timeout still delivers
// its answer. Without the route's own budget, the read finishes and its
// response is written to a connection the server already closed, so the caller
// sees a dropped request and retries a read that had succeeded.
func TestPullSchemaRoute_OutlivesTheServerWideWriteTimeout(t *testing.T) {
	client := &slowPullClient{
		delay: 300 * time.Millisecond,
		mockTernClient: &mockTernClient{pullSchemaResp: &ternv1.PullSchemaResponse{
			Database:    "orders",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Namespaces: map[string]*ternv1.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			},
			TableCount: 1,
		}},
	}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "orders-production", Deployment: "us"},
				},
			},
		},
		TernDeployments: TernConfig{"us": {"production": "tern.example.com:80"}},
	}
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	svc := New(&mockStorage{}, cfg, map[string]tern.Client{"us/production": client}, logger)

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	server := httptest.NewUnstartedServer(mux)
	server.Config.WriteTimeout = 50 * time.Millisecond
	server.Start()
	t.Cleanup(server.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/api/pull",
		strings.NewReader(`{"database":"orders","environment":"production","type":"mysql"}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	require.NoError(t, err, "the pull must answer past the server-wide write timeout")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var body apitypes.PullSchemaResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, int32(1), body.TableCount)
}

// A caller that hangs up while the fetched schema is still being processed is
// not written a success response. The read finished, but there is no one left
// to answer, so the pull ends with a warning.
func TestPullSchemaHandlerWarnsWhenTheCallerDisconnectsAfterTheRead(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	client := &mockTernClient{
		pullSchemaHook: func(*ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
			cancel()
			return &ternv1.PullSchemaResponse{
				Database:    "orders",
				Type:        storage.DatabaseTypeMySQL,
				Environment: "production",
				Namespaces: map[string]*ternv1.PulledNamespace{
					"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
				},
				TableCount: 1,
			}, nil
		},
	}
	svc, logs := newPullFailureService(t, client)

	w := postPull(t, ctx, svc)

	assert.Empty(t, w.Body.String(), "no response is written to a caller that left")
	assert.Empty(t, pullLogRecords(t, logs, "ERROR"))
	var abandoned []map[string]any
	for _, record := range pullLogRecords(t, logs, "WARN") {
		if record["msg"] == "pull schema abandoned because the caller disconnected before it finished" {
			abandoned = append(abandoned, record)
		}
	}
	require.Len(t, abandoned, 1)
	assert.Equal(t, "orders", abandoned[0]["database"])
	assert.Equal(t, "production", abandoned[0]["environment"])
}

// hangingPullClient never finishes a read on its own, standing in for a
// live-schema read that outlasts the pull budget. It returns only when the
// context it was given ends.
type hangingPullClient struct {
	*mockTernClient
}

func (c *hangingPullClient) PullSchema(ctx context.Context, _ *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	<-ctx.Done()
	return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
}

// A pull whose read outlasts its budget still answers. The read stops short of
// the connection's write deadline, so the caller gets a 504 with a JSON error
// over the socket instead of a dropped connection it would retry blindly.
func TestPullSchemaRoute_AnswersWhenTheReadOutlastsItsBudget(t *testing.T) {
	client := &hangingPullClient{mockTernClient: &mockTernClient{}}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "orders-production", Deployment: "us"},
				},
			},
		},
		TernDeployments: TernConfig{"us": {"production": "tern.example.com:80"}},
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	svc := New(&mockStorage{}, cfg, map[string]tern.Client{"us/production": client}, logger)
	svc.pullSchemaBudget = 600 * time.Millisecond
	svc.pullResponseReserve = 300 * time.Millisecond

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	server := httptest.NewUnstartedServer(mux)
	server.Config.WriteTimeout = 50 * time.Millisecond
	server.Start()
	t.Cleanup(server.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/api/pull",
		strings.NewReader(`{"database":"orders","environment":"production","type":"mysql"}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	require.NoError(t, err, "a pull that runs out of time must still answer")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusGatewayTimeout, resp.StatusCode)
	var body apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "pull schema did not finish within 300ms; pull fewer namespaces per request or retry later", body.Error)

	errorLogs := pullLogRecords(t, &logs, "ERROR")
	require.Len(t, errorLogs, 1, "a pull that runs out of time is logged once")
	assert.Equal(t, "pull schema did not finish within its budget", errorLogs[0]["msg"])
	assert.Equal(t, "orders-production", errorLogs[0]["target"])
}

// The read always stops before the connection's write deadline, so there is
// time left to send a pull's answer.
func TestPullResponseReserveLeavesTheReadMostOfTheBudget(t *testing.T) {
	assert.Greater(t, pullResponseReserve, time.Duration(0))
	assert.Less(t, pullResponseReserve, apitypes.PullSchemaBudget/2)
}
