package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

type emptyStorage struct {
	storage.Storage
}

func (s *emptyStorage) Close() error {
	return nil
}

func (s *emptyStorage) Checks() storage.CheckStore {
	return &emptyCheckStore{}
}

func (s *emptyStorage) Applies() storage.ApplyStore {
	return &emptyApplyStore{}
}

func (s *emptyStorage) Locks() storage.LockStore {
	return &emptyLockStore{}
}

type emptyLockStore struct {
	storage.LockStore
}

func (s *emptyLockStore) Get(ctx context.Context, database, dbType string) (*storage.Lock, error) {
	return nil, nil
}

func (s *emptyLockStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Lock, error) {
	return nil, nil
}

type emptyCheckStore struct {
	storage.CheckStore
}

func (s *emptyCheckStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	return nil, nil
}

func (s *emptyCheckStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Check, error) {
	return nil, nil
}

type emptyApplyStore struct {
	storage.ApplyStore
}

func (s *emptyApplyStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Apply, error) {
	return nil, nil
}

func (s *emptyApplyStore) GetByApplyIdentifier(ctx context.Context, applyIdentifier string) (*storage.Apply, error) {
	return nil, nil
}

type failingStorage struct {
	emptyStorage
}

func (s *failingStorage) Checks() storage.CheckStore {
	return &failingCheckStore{}
}

type failingCheckStore struct {
	storage.CheckStore
}

func (s *failingCheckStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	return nil, errors.New("storage read failed")
}

type sequenceStorage struct {
	emptyStorage
	checks *sequenceCheckStore
}

func (s *sequenceStorage) Checks() storage.CheckStore {
	return s.checks
}

type sequenceCheckStore struct {
	storage.CheckStore
	results []*storage.Check
	calls   int
}

func (s *sequenceCheckStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	s.calls++
	if len(s.results) == 0 {
		return nil, nil
	}
	check := s.results[0]
	s.results = s.results[1:]
	return check, nil
}

func TestWebhookEnvironmentFiltering(t *testing.T) {
	t.Run("non-allowed environment ignored with explicit response", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(&emptyStorage{}, &api.ServerConfig{
			AllowedEnvironments: []string{"staging"},
			Repos:               map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		// Plan targeting production should be ignored by this instance because
		// only staging is in allowed_environments.
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot plan -e production",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "environment handled by another instance")
	})

	t.Run("allowed environment proceeds", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(&emptyStorage{}, &api.ServerConfig{
			AllowedEnvironments: []string{"staging"},
			Repos:               map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		// Plan for staging should proceed past the environment filter. It will fail
		// downstream because there's no schema config on GitHub, but the response
		// proves the environment filter did not block it.
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot plan -e staging",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		// The plan command gets past the environment filter and enters the plan handler.
		// With no service/storage wired up fully, it responds with "plan started".
		assert.NotContains(t, rr.Body.String(), "environment handled by another instance")
	})

	t.Run("empty config allows all environments", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(&emptyStorage{}, &api.ServerConfig{
			Repos: map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		// Plan for production with no allowed_environments config should proceed
		// (empty config allows all environments).
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot plan -e production",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.NotContains(t, rr.Body.String(), "environment handled by another instance")
	})
}

//go:fix inline
func TestRespondToUnscoped(t *testing.T) {
	falseVal := false
	trueVal := true
	t.Run("help skipped when respond_to_unscoped is false", func(t *testing.T) {
		service := api.New(nil, &api.ServerConfig{
			RespondToUnscoped: &falseVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service: service,
			logger:  testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "unscoped command skipped")
	})

	t.Run("invalid command skipped when respond_to_unscoped is false", func(t *testing.T) {
		service := api.New(nil, &api.ServerConfig{
			RespondToUnscoped: &falseVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service: service,
			logger:  testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot foobar",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "unscoped command skipped")
	})

	t.Run("targeted command skipped by non-matching tenant", func(t *testing.T) {
		service := api.New(nil, &api.ServerConfig{
			Tenant:            "alpha",
			RespondToUnscoped: &trueVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service: service,
			logger:  testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help --tenant beta",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "tenant handled by another instance")
	})

	t.Run("invalid tenant flag is ignored before reactions or comments", func(t *testing.T) {
		service := api.New(nil, &api.ServerConfig{
			Tenant:            "alpha",
			RespondToUnscoped: &trueVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service: service,
			logger:  testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help --tenant alpha@example",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "invalid tenant flag")
	})

	t.Run("targeted help bypasses respond_to_unscoped", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{
			Tenant:            "alpha",
			RespondToUnscoped: &falseVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help -t alpha",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "help posted")
	})

	t.Run("targeted invalid command bypasses respond_to_unscoped", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{
			Tenant:            "alpha",
			RespondToUnscoped: &falseVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot wat -t alpha",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "invalid command")
	})

	t.Run("short tenant flag routes to matching deployment", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{
			Tenant:            "alpha",
			RespondToUnscoped: &trueVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help -t alpha",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "help posted")
	})

	t.Run("help responds when respond_to_unscoped is true", func(t *testing.T) {
		client, mux := setupGitHubServer(t)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
		})

		installClient := ghclient.NewInstallationClient(client, testLogger())
		factory := &fakeClientFactory{client: installClient}

		service := api.New(nil, &api.ServerConfig{
			RespondToUnscoped: &trueVal,
			Repos:             map[string]api.RepoConfig{},
		}, nil, testLogger())

		h := &Handler{
			service:   service,
			ghClients: ghclient.NewSingleClientSet(defaultAppName, factory),
			logger:    testLogger(),
		}

		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: "schemabot help",
			isPR:    true,
		}, nil)

		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "help posted")
	})
}

// httpResponseRecorder creates an httptest.ResponseRecorder.
func httpResponseRecorder() *httptest.ResponseRecorder {
	return httptest.NewRecorder()
}
