package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// mockStorage implements storage.Storage for testing.
type mockStorage struct {
	pingErr error
}

func (m *mockStorage) Locks() storage.LockStore                 { return nil }
func (m *mockStorage) Plans() storage.PlanStore                 { return nil }
func (m *mockStorage) Applies() storage.ApplyStore              { return nil }
func (m *mockStorage) Tasks() storage.TaskStore                 { return nil }
func (m *mockStorage) ApplyLogs() storage.ApplyLogStore         { return nil }
func (m *mockStorage) ApplyComments() storage.ApplyCommentStore { return nil }
func (m *mockStorage) Checks() storage.CheckStore               { return nil }
func (m *mockStorage) Settings() storage.SettingsStore          { return nil }
func (m *mockStorage) Ping(ctx context.Context) error           { return m.pingErr }
func (m *mockStorage) Close() error                             { return nil }

// mockTernClient implements tern.Client for testing.
type mockTernClient struct {
	healthErr      error
	volumeResp     *ternv1.VolumeResponse
	volumeErr      error
	stopResp       *ternv1.StopResponse
	stopErr        error
	stopReq        *ternv1.StopRequest // captured request
	startResp      *ternv1.StartResponse
	startErr       error
	startReq       *ternv1.StartRequest // captured request
	cutoverResp    *ternv1.CutoverResponse
	cutoverErr     error
	cutoverReq     *ternv1.CutoverRequest // captured request
	revertResp     *ternv1.RevertResponse
	revertErr      error
	revertReq      *ternv1.RevertRequest // captured request
	skipRevertResp *ternv1.SkipRevertResponse
	skipRevertErr  error
	skipRevertReq  *ternv1.SkipRevertRequest // captured request
}

func (m *mockTernClient) Health(ctx context.Context) error { return m.healthErr }
func (m *mockTernClient) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	return nil, nil
}
func (m *mockTernClient) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	return nil, nil
}
func (m *mockTernClient) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	return nil, nil
}
func (m *mockTernClient) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	m.cutoverReq = req
	if m.cutoverResp != nil {
		return m.cutoverResp, m.cutoverErr
	}
	return nil, m.cutoverErr
}
func (m *mockTernClient) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	m.stopReq = req
	if m.stopResp != nil {
		return m.stopResp, m.stopErr
	}
	return nil, m.stopErr
}
func (m *mockTernClient) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	m.startReq = req
	if m.startResp != nil {
		return m.startResp, m.startErr
	}
	return nil, m.startErr
}
func (m *mockTernClient) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	if m.volumeResp != nil {
		return m.volumeResp, m.volumeErr
	}
	return nil, m.volumeErr
}
func (m *mockTernClient) Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	m.revertReq = req
	if m.revertResp != nil {
		return m.revertResp, m.revertErr
	}
	return nil, m.revertErr
}
func (m *mockTernClient) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	m.skipRevertReq = req
	if m.skipRevertResp != nil {
		return m.skipRevertResp, m.skipRevertErr
	}
	return nil, m.skipRevertErr
}
func (m *mockTernClient) RollbackPlan(ctx context.Context, database string) (*ternv1.PlanResponse, error) {
	return nil, nil
}
func (m *mockTernClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	return nil
}
func (m *mockTernClient) IsRemote() bool { return false }
func (m *mockTernClient) Close() error   { return nil }

// testServerConfig returns a minimal valid ServerConfig for testing.
// Only includes "staging" environment - tests that need "production"
// should create their own config or add it to the mock ternClients.
func testServerConfig() *ServerConfig {
	return &ServerConfig{
		TernDeployments: TernConfig{
			"default": TernEndpoints{
				"staging": "localhost:9090",
			},
		},
	}
}

func newTestService() *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorage{}, testServerConfig(), nil, logger)
}

func TestHealth(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.Equal(t, "ok", resp["status"])
	})

	t.Run("unhealthy", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		svc := New(&mockStorage{pingErr: errors.New("connection refused")}, testServerConfig(), nil, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.NotEmpty(t, resp["error"], "expected error message")
	})
}

func TestServiceClose(t *testing.T) {
	svc := newTestService()
	assert.NoError(t, svc.Close())
}

func TestTernHealth(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": &mockTernClient{},
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/tern-health/default/staging", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.Equal(t, "ok", resp["status"])
		assert.Equal(t, "default", resp["deployment"])
		assert.Equal(t, "staging", resp["environment"])
	})

	t.Run("unhealthy", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": &mockTernClient{healthErr: errors.New("connection refused")},
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/tern-health/default/staging", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.NotEmpty(t, resp["error"], "expected error message")
	})

	t.Run("unknown deployment", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": &mockTernClient{},
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/tern-health/unknown/staging", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.NotEmpty(t, resp["error"], "expected error message")
	})

	t.Run("unknown environment", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": &mockTernClient{},
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/tern-health/default/production", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestVolumeHandler(t *testing.T) {
	t.Run("success returns volume values", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": &mockTernClient{
				volumeResp: &ternv1.VolumeResponse{
					Accepted:       true,
					PreviousVolume: 3,
					NewVolume:      11,
				},
			},
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging", "volume": 11}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/volume", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")

		// Verify the response includes volume values (not zeros)
		assert.Equal(t, true, resp["accepted"])
		prevVol, _ := resp["previous_volume"].(float64) // JSON numbers are float64
		newVol, _ := resp["new_volume"].(float64)
		assert.Equal(t, float64(3), prevVol)
		assert.Equal(t, float64(11), newVol)
	})

	t.Run("invalid volume range", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging", "volume": 0}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/volume", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.NotEmpty(t, resp["error"], "expected error message for invalid volume")
	})

	t.Run("missing database", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "volume": 5}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/volume", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestStopHandler(t *testing.T) {
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			stopResp: &ternv1.StopResponse{
				Accepted:     true,
				StoppedCount: 2,
			},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": mock,
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging", "apply_id": "apply-abc123"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		// Verify apply_id was passed through to the tern client
		require.NotNil(t, mock.stopReq, "expected stop request to be captured")
		assert.Equal(t, "apply-abc123", mock.stopReq.ApplyId)
		assert.Equal(t, "testdb", mock.stopReq.Database)
	})

	t.Run("works without apply_id", func(t *testing.T) {
		mock := &mockTernClient{
			stopResp: &ternv1.StopResponse{
				Accepted:     true,
				StoppedCount: 1,
			},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": mock,
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Empty(t, mock.stopReq.ApplyId)

		var resp apitypes.StopResponse
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.True(t, resp.Accepted, "expected accepted=true")
		assert.Equal(t, int64(1), resp.StoppedCount)
	})

	t.Run("missing database", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestStartHandler(t *testing.T) {
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			startResp: &ternv1.StartResponse{
				Accepted:     true,
				StartedCount: 3,
			},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": mock,
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging", "apply_id": "apply-xyz789"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		// Verify apply_id was passed through to the tern client
		require.NotNil(t, mock.startReq, "expected start request to be captured")
		assert.Equal(t, "apply-xyz789", mock.startReq.ApplyId)
		assert.Equal(t, "testdb", mock.startReq.Database)
	})

	t.Run("works without apply_id", func(t *testing.T) {
		mock := &mockTernClient{
			startResp: &ternv1.StartResponse{
				Accepted:     true,
				StartedCount: 1,
			},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": mock,
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Empty(t, mock.startReq.ApplyId)

		var resp apitypes.StartResponse
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.True(t, resp.Accepted, "expected accepted=true")
		assert.Equal(t, int64(1), resp.StartedCount)
	})

	t.Run("missing database", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestCutoverHandler(t *testing.T) {
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			cutoverResp: &ternv1.CutoverResponse{
				Accepted: true,
			},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": mock,
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging", "apply_id": "apply-cut123"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		require.NotNil(t, mock.cutoverReq, "expected cutover request to be captured")
		assert.Equal(t, "apply-cut123", mock.cutoverReq.ApplyId)
	})

	t.Run("works without apply_id", func(t *testing.T) {
		mock := &mockTernClient{
			cutoverResp: &ternv1.CutoverResponse{
				Accepted: true,
			},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": mock,
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Empty(t, mock.cutoverReq.ApplyId)
	})

	t.Run("missing database", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestRevertHandler(t *testing.T) {
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			revertResp: &ternv1.RevertResponse{Accepted: true},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{"default/staging": mock}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging", "apply_id": "apply-rev123"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/revert", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, mock.revertReq)
		assert.Equal(t, "apply-rev123", mock.revertReq.ApplyId)
		assert.Equal(t, "testdb", mock.revertReq.Database)
	})

	t.Run("works without apply_id", func(t *testing.T) {
		mock := &mockTernClient{
			revertResp: &ternv1.RevertResponse{Accepted: true},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{"default/staging": mock}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/revert", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Empty(t, mock.revertReq.ApplyId)
	})
}

func TestSkipRevertHandler(t *testing.T) {
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			skipRevertResp: &ternv1.SkipRevertResponse{Accepted: true},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{"default/staging": mock}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging", "apply_id": "apply-skip456"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/skip-revert", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, mock.skipRevertReq)
		assert.Equal(t, "apply-skip456", mock.skipRevertReq.ApplyId)
		assert.Equal(t, "testdb", mock.skipRevertReq.Database)
	})

	t.Run("works without apply_id", func(t *testing.T) {
		mock := &mockTernClient{
			skipRevertResp: &ternv1.SkipRevertResponse{Accepted: true},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{"default/staging": mock}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"database": "testdb", "environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/skip-revert", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Empty(t, mock.skipRevertReq.ApplyId)
	})
}

func TestDeriveErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		state    string
		errMsg   string
		expected string
	}{
		{"failed with error", "failed", "Spirit: table copy failed", apitypes.ErrCodeEngineError},
		{"failed without error", "failed", "", ""},
		{"running with error", "running", "something", ""},
		{"completed", "completed", "", ""},
		{"stopped", "stopped", "", ""},
		{"proto state format", "STATE_FAILED", "engine error", apitypes.ErrCodeEngineError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, deriveErrorCode(tt.state, tt.errMsg))
		})
	}
}
